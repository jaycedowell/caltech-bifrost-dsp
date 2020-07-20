import bifrost.ndarray as BFArray
from bifrost.proclog import ProcLog
from bifrost.libbifrost import _bf
import bifrost.affinity as cpu_affinity
from bifrost.ring import WriteSpan
from bifrost.linalg import LinAlg
from bifrost import map as BFMap
from bifrost.ndarray import copy_array
from bifrost.device import stream_synchronize, set_device as BFSetGPU

import time
import simplejson as json
import numpy as np
from threading import Lock

class CorrSubSel(object):
    """
    Grab arbitrary entries from a GPU buffer and copy them to the CPU
    """
    nvis_out = 4656
    def __init__(self, log, iring, oring,
            guarantee=True, core=-1, nchans=192, gpu=-1):
        self.log = log
        self.iring = iring
        self.oring = oring
        self.guarantee = guarantee
        self.core = core
        self.nchans = nchans
        self.gpu = gpu

        if self.gpu != -1:
            BFSetGPU(self.gpu)

        self.bind_proclog = ProcLog(type(self).__name__+"/bind")
        self.in_proclog   = ProcLog(type(self).__name__+"/in")
        self.out_proclog  = ProcLog(type(self).__name__+"/out")
        self.size_proclog = ProcLog(type(self).__name__+"/size")
        self.sequence_proclog = ProcLog(type(self).__name__+"/sequence0")
        self.perf_proclog = ProcLog(type(self).__name__+"/perf")
        
        self.in_proclog.update(  {'nring':1, 'ring0':self.iring.name})
        self.out_proclog.update( {'nring':1, 'ring0':self.oring.name})
        self.igulp_size = 47849472 * 8 # complex64

        # Create an array of subselection indices on the GPU, and one on the CPU.
        # The user can update the CPU-side array, and the main processing thread
        # will copy this to the GPU when it changes
        # TODO: nvis_out could be dynamic, but we'd have to reallocate the GPU memory
        # if the size changed. Leave static for now, which is all the requirements call for.
        self._subsel = BFArray(shape=[self.nvis_out], dtype='i32', space='cuda')
        self._subsel_next = BFArray(np.array(list(range(self.nvis_out)), dtype=np.int32), dtype='i32', space='cuda_host')
        self._subsel_pending = True
        self._subsel_lock = Lock()
        self.obuf_gpu = BFArray(shape=[self.nchans, self.nvis_out], dtype='i64', space='cuda')
        self.ogulp_size = self.nchans * self.nvis_out * 8

    def add_etcd_controller(self, client):
        etcd_id = client.add_watch_callback('/foo/subsel', self._etcd_update_subsel)

    def _etcd_update_subsel(self, watchresponse):
        v = json.loads(watchresponse.events[0].value)
        if isinstance(v, list):
            self.update_subsel(v)
        else:
            self.log.error("Tried to update subselection with a non-list")

    def update_subsel(self, subsel):
        """
        Update the baseline index list which should be subselected.
        """
        if len(subsel) != self.nvis_out:
            self.log.error("Tried to update baseline subselection with an array of length %d" % len(subsel))
            return
        else:
            self._subsel_lock.acquire()
            self._subsel_next[...] = subsel
            self._subsel_pending = True
            self._subsel_lock.release()

    def main(self):
        cpu_affinity.set_core(self.core)
        if self.gpu != -1:
            BFSetGPU(self.gpu)
        self.bind_proclog.update({'ncore': 1, 
                                  'core0': cpu_affinity.get_core(),})

        self.oring.resize(self.ogulp_size)
        oseq = None
        with self.oring.begin_writing() as oring:
            prev_time = time.time()
            for iseq in self.iring.read(guarantee=self.guarantee):
                ihdr = json.loads(iseq.header.tostring())
                ohdr = ihdr.copy()
                for ispan in iseq.read(self.igulp_size):
                    curr_time = time.time()
                    acquire_time = curr_time - prev_time
                    prev_time = curr_time
                    self.log.debug("Grabbing subselection")
                    idata = ispan.data_view('i64').reshape(47849472)
                    self._subsel_lock.acquire()
                    if self._subsel_pending:
                        self.log.info("Updating baseline subselection indices")
                        self._subsel[...] = self._subsel_next
                        ohdr['subsel'] = self._subsel_next.tolist()
                    self._subsel_pending = False
                    self._subsel_lock.release()
                    ohdr_str = json.dumps(ohdr)
                    with oring.begin_sequence(time_tag=iseq.time_tag, header=ohdr_str, nringlet=iseq.nringlet) as oseq:
                        with oseq.reserve(self.ogulp_size) as ospan:
                            curr_time = time.time()
                            reserve_time = curr_time - prev_time
                            prev_time = curr_time
                            rv = _bf.bfXgpuSubSelect(idata.as_BFarray(), self.obuf_gpu.as_BFarray(), self._subsel.as_BFarray())
                            if (rv != _bf.BF_STATUS_SUCCESS):
                                self.log.error("xgpuIntialize returned %d" % rv)
                                raise RuntimeError
                            odata = ospan.data_view(dtype='i64').reshape([self.nchans, self.nvis_out])
                            copy_array(odata, self.obuf_gpu)
                            # Wait for copy to complete before committing span
                            stream_synchronize()
                            curr_time = time.time()
                            process_time = curr_time - prev_time
                            prev_time = curr_time
                        self.perf_proclog.update({'acquire_time': acquire_time, 
                                                  'reserve_time': reserve_time, 
                                                  'process_time': process_time,})
