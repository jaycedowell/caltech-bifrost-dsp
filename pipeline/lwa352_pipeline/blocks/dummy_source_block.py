from bifrost.proclog import ProcLog
import bifrost.ndarray as BFArray
import bifrost.affinity as cpu_affinity

import os
import time
import ujson as json
import threading
import numpy as np

NTEST_BLOCKS = 2

class DummySource(object):
    """
    **Functionality**

    A dummy source block for throughput testing. Optionally writes
    test data to an output buffer.

    **New Sequence Condition**

    This block starts a single new sequence when main() is called.

    **Input Header Requirements**

    This block is a bifrost source, and thus has no input header
    requirements.

    **Output Headers**
    
    .. table::
        :widths: 15 10 10 15 45

        +------------------+------------+---------------+-----------------+-------------------------------------+
        | Field            | Format     | Units         | Value           | Description                         |
        +==================+============+===============+=================+=====================================+
        | ``sync_time``    | int        | UNIX seconds  | ``int(time.tim  | Synchronization time (corresponding |
        |                  |            |               | e())``          | to spectrum sequence number 0).     |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``seq0``         | int        |               | 0               | Spectra number for the first sample |
        |                  |            |               |                 | in this sequence                    |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``chan0``        | int        |               | 0               | Channel index of the first channel  |
        |                  |            |               |                 | in this sequence                    |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``nchan``        | int        |               | ``nchan``       | Number of channels in the sequence  |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``system_nchan`` | int        |               | ``nchan``       | The total number of channels in the |
        |                  |            |               |                 | system (i.e., the number of         |
        |                  |            |               |                 | channels across all pipelines)      |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``sfreq``        | double     | Hz            | 0.0             | Center frequency of first channel   |
        |                  |            |               |                 | in the sequence                     |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``bw_hz``        | int        | Hz            | ``24000 *       | Bandwidth of the sequence           |
        |                  |            |               | nchan``         |                                     |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``nstand``       | int        |               | ``nstand``      | Number of stands (antennas) in the  |
        |                  |            |               |                 | sequence                            |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``npol``         | int        |               | ``npol``        | Number of polarizations per stand   |
        |                  |            |               |                 | in the sequence                     |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``fs_hz``        | int        | Hz            | ``196608000``   | ADC Sample Rate                     |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``input_to_ant`` | list[int]  |               | entry ``i`` is  | List of input to stand/pol mappings |
        |                  |            |               | ``[i // npol, i | with dimensions ``[nstand x npol,   |
        |                  |            |               | % npol]``       | 2]``. E.g. if entry ``N`` of this   |
        |                  |            |               |                 | list has value ``[S, P]`` then the  |
        |                  |            |               |                 | ``N``-th correlator input is stand  |
        |                  |            |               |                 | ``S``, polarization ``P``.          |
        +------------------+------------+---------------+-----------------+-------------------------------------+
        | ``ant_to_input`` | list[ints] |               | entry ``[s,p]`` | List of stand/pol to correlator     |
        |                  |            |               | is ``npol*s +   | input number mappings with          |
        |                  |            |               | p``             | dimensions ``[nstand, npol]``. E.g. |
        |                  |            |               |                 | if entry ``[S,P]`` of this list has |
        |                  |            |               |                 | value ``N`` then stand ``S``,       |
        |                  |            |               |                 | polarization ``P`` of the array is  |
        |                  |            |               |                 | the ``N``-th correlator input       |
        +------------------+------------+---------------+-----------------+-------------------------------------+

    **Data Buffers**
    
    *Input data buffer*: None

    *Output data buffer*: Complex 4-bit data with dimensions (slowest to fastest)
    ``Time x Freq x Stand x Polarization x Complexity``

    **Instantiation**

    :param log: Logging object to which runtime messages should be
        emitted.
    :type log: logging.Logger

    :param oring: bifrost output data ring
    :type oring: bifrost.ring.Ring

    :param nstand: Number of stands in the array.
    :type nstand: int

    :param npol: Number of polarizations per antenna stand.
    :type npol: int

    :param nchan: Number of frequency channels this block will output
    :type nchan: int

    :param core: CPU core to which this block should be bound. If ``-1``, no binding is used.
    :type core: int

    :param ntime_gulp: The number of time samples to output on each processing loop iteration.
    :type ntime_gulp: int

    :param test_file: Path to a file containing test data, as a raw binary file containing
       4+4 bit complex data in ``time x channel x stand x polarization`` order, with
       polarization changing fastest. This file should contain a multiple of ``ntime_gulp``
       samples. When the end of the file is reached, it is repeated.
    :type test_file: str

    :param target_throughput: The target Gbits/s at which this block should output data.
       Throttling will be used to target this rate if necessary.
    :type target_throughput: float

    :param skip_write: If set to ``True``, no data will be copied to the output buffer,
       blocks of memory will just be marked full as fast as possible. This can be useful
       to test the maximum throughput of the dowstream pipeline blocks.
       If set to ``False`` and no ``testfile`` is provided, the output data is a ramp,
       with each 4+4-bit data sample taking the value ``stand % 8``
    :type skip_write: Bool

    """
    def __init__(self, log, oring, ntime_gulp=2500,
                 core=-1, nchan=192, nstand=352, npol=2, skip_write=False, 
                 target_throughput=22.0, testfile=None, header={}):
        self.log = log
        self.oring = oring
        self.ntime_gulp = ntime_gulp
        self.core = core
        self.nchan = nchan
        self.npol = npol
        self.nstand = nstand
        self.ninputs = nstand * npol
        self.skip_write = skip_write
        self.target_throughput = target_throughput
        self.header_base = header
        
        self.bind_proclog = ProcLog(type(self).__name__+"/bind")
        self.in_proclog   = ProcLog(type(self).__name__+"/in")
        self.out_proclog  = ProcLog(type(self).__name__+"/out")
        self.size_proclog = ProcLog(type(self).__name__+"/size")
        self.sequence_proclog = ProcLog(type(self).__name__+"/sequence0")
        self.perf_proclog = ProcLog(type(self).__name__+"/perf")
        
        self.out_proclog.update( {'nring':1, 'ring0':self.oring.name})
        self.size_proclog.update({'nseq_per_gulp': self.ntime_gulp})
        self.gulp_size = self.ntime_gulp*nchan*nstand*npol*1        # complex8

        # file containing test data
        if testfile is not None:
            self.testfile = open(testfile, 'rb')
            self.testfile_nbytes = os.path.getsize(testfile)
        else:
            self.testfile = None

        # make an array ninputs-elements long with [station, pol] IDs.
        # e.g. if input_to_ant[12] = [27, 1], then the 13th input is stand 27, pol 1
        self.input_to_ant = np.zeros([self.ninputs, 2], dtype=np.int32)
        for s in range(self.nstand):
            for p in range(self.npol):
                self.input_to_ant[self.npol*s + p] = [s, p]

        self.ant_to_input = np.zeros([self.nstand, self.npol], dtype=np.int32)
        for i, inp in enumerate(self.input_to_ant):
            stand = inp[0]
            pol = inp[1]
            self.ant_to_input[stand, pol] = i

        if skip_write:
            self.test_data = BFArray(shape=[NTEST_BLOCKS, ntime_gulp, nchan, nstand, npol], dtype='i8', space='system')
        else:
            #print("initializing random numbers")
            #TODO Can't get 'ci4' type to behave
            #self.test_data = BFArray(np.random.randint(0, high=255, size=[NTEST_BLOCKS, ntime_gulp, nchan, nstand, npol]),
            #                    dtype='u8', space='system')
            self.test_data = BFArray(np.zeros([NTEST_BLOCKS, ntime_gulp, nchan, nstand, npol]),
                                dtype='u8', space='system')
            for i in range(nstand):
                self.test_data[:,:,:,i,:] = i%8

        self.shutdown_event = threading.Event()

    def get_test_data(self):
        r = self.test_data >> 4
        i = self.test_data & 0xf
        return r + 1j*i

    def shutdown(self):
        self.shutdown_event.set()

    def get_testfile_gulp(self, t):
        """
        Get a single gulp from the test file,
        looping back to the beginning of the file when
        the end is reached.
        Inputs: t (int) -- time index of gulp. I.e., increment
            by 1 between gulps.
        """
        nbytes = self.gulp_size
        seekloc = (self.gulp_size * t) % self.testfile_nbytes
        self.testfile.seek(seekloc)
        rawdata = self.testfile.read(nbytes)
        if len(rawdata) != nbytes:
            self.log.error("Failed to get input test vector gulp")
            return np.zeros_like(self.test_data[1:])
        return np.frombuffer(rawdata, dtype=np.uint8).reshape(self.test_data.shape[1:])

    def main(self):
        cpu_affinity.set_core(self.core)
        self.bind_proclog.update({'ncore': 1, 
                                  'core0': cpu_affinity.get_core(),})

        time.sleep(0.1)
        self.oring.resize(self.gulp_size, self.gulp_size*4)
        hdr = {}
        hdr.update(self.header_base)
        hdr['nchan'] = self.nchan
        hdr['system_nchan'] = 32*self.nchan
        hdr['chan0'] = 0
        hdr['bw_hz'] = 24e3 * self.nchan
        hdr['fs_hz'] = 196608000
        hdr['sfreq'] = 0.0
        hdr['nstand'] = self.nstand
        hdr['npol'] = self.npol
        hdr['seq0'] = 0
        hdr['input_to_ant'] = self.input_to_ant.tolist()
        hdr['ant_to_input'] = self.ant_to_input.tolist()
        hdr['sync_time'] = int(time.time())
        time_tag = 0
        REPORT_PERIOD = 100
        bytes_per_report = REPORT_PERIOD * self.gulp_size
        acquire_time = 0 # this block doesn't have an input ring
        gbps = 0
        extra_delay = 0
        with self.oring.begin_writing() as oring:
            tick = time.time()
            ohdr_str = json.dumps(hdr)
            prev_time = time.time()
            with oring.begin_sequence(time_tag=time_tag, header=ohdr_str) as oseq:
                while not self.shutdown_event.is_set():
                    with oseq.reserve(self.gulp_size) as ospan:
                        curr_time = time.time()
                        reserve_time = curr_time - prev_time
                        prev_time = curr_time
                        if not self.skip_write:
                            if self.testfile:
                                self.test_data[time_tag % NTEST_BLOCKS] = self.get_testfile_gulp(time_tag)
                            odata = ospan.data_view(shape=self.test_data.shape[1:], dtype=self.test_data.dtype)
                            odata[...] = self.test_data[time_tag % NTEST_BLOCKS]
                        time_tag += 1
                    curr_time = time.time()
                    process_time = curr_time - prev_time
                    prev_time = curr_time
                    self.perf_proclog.update({'acquire_time': acquire_time, 
                                              'reserve_time': reserve_time, 
                                              'process_time': process_time,
                                              'gbps' : gbps})
                    time.sleep(max(0, extra_delay / REPORT_PERIOD))
                    if time_tag % REPORT_PERIOD == 0:
                        tock = time.time()
                        dt = tock - tick
                        gbps = 8*bytes_per_report / dt / 1e9
                        self.log.info('%d: Sent %d bytes in %.2f seconds (%.2f Gb/s)' % (time_tag // REPORT_PERIOD, bytes_per_report, dt, gbps))
                        target_time = 8*bytes_per_report / self.target_throughput / 1e9
                        extra_delay = target_time - dt + extra_delay
                        tick = tock
        if self.testfile:
            self.testfile.close()
