import bifrost.ndarray as BFArray
from bifrost.proclog import ProcLog
from bifrost.libbifrost import _bf
import bifrost.affinity as cpu_affinity
from bifrost.ring import WriteSpan
from bifrost.linalg import LinAlg
from bifrost import map as BFMap
from bifrost.ndarray import copy_array
from bifrost.device import set_device as BFSetGPU, get_device as BFGetGPU, stream_synchronize as BFSync
from bifrost.unpack import unpack

import time
import json
import numpy as np
from collections import deque

FS=200.0e6 # sampling rate
CLOCK            = 204.8e6 #???
NCHAN            = 4096
FREQS            = np.around(np.fft.fftfreq(2*NCHAN, 1./CLOCK)[:NCHAN][:-1], 3)
CHAN_BW          = FREQS[1] - FREQS[0]

class Beamform(object):
    # Note: Input data are: [time,chan,ant,pol,cpx,8bit]
    def __init__(self, log, iring, oring, tuning=0, nchan_max=256, nbeam_max=1, nstand=352, npol=2, ntime_gulp=2500, guarantee=True, core=-1, gpu=-1):
        self.log   = log
        self.iring = iring
        self.oring = oring
        self.tuning = tuning
        self.ntime_gulp = ntime_gulp
        self.guarantee = guarantee
        self.core = core
        self.gpu = gpu
        
        self.bind_proclog = ProcLog(type(self).__name__+"/bind")
        self.in_proclog   = ProcLog(type(self).__name__+"/in")
        self.out_proclog  = ProcLog(type(self).__name__+"/out")
        self.size_proclog = ProcLog(type(self).__name__+"/size")
        self.sequence_proclog = ProcLog(type(self).__name__+"/sequence0")
        self.perf_proclog = ProcLog(type(self).__name__+"/perf")
        
        self.in_proclog.update(  {'nring':1, 'ring0':self.iring.name})
        self.out_proclog.update( {'nring':1, 'ring0':self.oring.name})
        self.size_proclog.update({'nseq_per_gulp': self.ntime_gulp})
        
        self.nchan_max = nchan_max
        self.nbeam_max = nbeam_max
        self.nstand = nstand
        self.npol = npol

        # TODO self.configMessage = ISC.BAMConfigurationClient(addr=('adp',5832))
        self._pending = deque()
        
        # Setup the beamformer
        if self.gpu != -1:
            BFSetGPU(self.gpu)
        ## Metadata
        nchan = self.nchan_max
        ## Object
        self.bfbf = LinAlg()
        ## Delays and gains
        self.delays = np.zeros((self.nbeam_max*2,nstand*npol), dtype=np.float64)
        self.gains = np.zeros((self.nbeam_max*2,nstand*npol), dtype=np.float64)
        self.cgains = BFArray(shape=(self.nbeam_max*2,nchan,nstand*npol), dtype=np.complex64, space='cuda')
        ## Intermidiate arrays
        #self.tdata = BFArray(shape=(self.ntime_gulp,nchan,nstand*npol), dtype='ci8', native=False, space='cuda')
        self.tdata = BFArray(shape=(self.ntime_gulp,nchan,nstand*npol), dtype=np.complex64, space='cuda')
        self.bdata = BFArray(shape=(nchan,self.nbeam_max*2,self.ntime_gulp), dtype=np.complex64, space='cuda')
        self.ldata = BFArray(shape=self.bdata.shape, dtype=self.bdata.dtype, space='cuda_host')

    def configMessage(self):
        return None
        
    #@ISC.logException
    def updateConfig(self, config, hdr, time_tag, forceUpdate=False):
        return True
        if self.gpu != -1:
            BFSetGPU(self.gpu)
            
        # Get the current pipeline time to figure out if we need to shelve a command or not
        pipeline_time = time_tag / FS
        
        # Can we act on this configuration change now?
        if config:
            ## Pull out the tuning (something unique to DRX/BAM/COR)
            beam, tuning = config[0], config[3]
            if beam > self.nbeam_max or tuning != self.tuning:
                return False
                
            ## Set the configuration time - BAM commands are for the specified slot in the next second
            slot = config[4] / 100.0
            config_time = int(time.time()) + 1 + slot
            
            ## Is this command from the future?
            if pipeline_time < config_time:
                ### Looks like it, save it for later
                self._pending.append( (config_time, config) )
                config = None
                
                ### Is there something pending?
                try:
                    stored_time, stored_config = self._pending[0]
                    if pipeline_time >= stored_time:
                        config_time, config = self._pending.popleft()
                except IndexError:
                    pass
            else:
                ### Nope, this is something we can use now
                pass
                
        else:
            ## Is there something pending?
            try:
                stored_time, stored_config = self._pending[0]
                if pipeline_time >= stored_time:
                    config_time, config = self._pending.popleft()
            except IndexError:
                #print "No pending configuation at %.1f" % pipeline_time
                pass
                
        if config:
            self.log.info("Beamformer: New configuration received for beam %i (delta = %.1f subslots)", config[0], (pipeline_time-config_time)*100.0)
            beam, delays, gains, tuning, slot = config
            if tuning != self.tuning:
                self.log.info("Beamformer: Not for this tuning, skipping")
                return False
                
            # Byteswap to get into little endian
            delays = delays.byteswap().newbyteorder()
            gains = gains.byteswap().newbyteorder()
            
            # Unpack and re-shape the delays (to seconds) and gains (to floating-point)
            delays = (((delays>>4)&0xFFF) + (delays&0xF)/16.0) / FS
            gains = gains/32767.0
            gains.shape = (gains.size/2, 2)
            
            # Update the internal delay and gain cache so that we can use these later
            self.delays[2*(beam-1)+0,:] = delays
            self.delays[2*(beam-1)+1,:] = delays
            self.gains[2*(beam-1)+0,:] = gains[:,0]
            self.gains[2*(beam-1)+1,:] = gains[:,1]
            
            # Compute the complex gains needed for the beamformer
            freqs = CHAN_BW * (hdr['chan0'] + np.arange(hdr['nchan']))
            freqs.shape = (freqs.size, 1)
            self.cgains[2*(beam-1)+0,:,:] = (np.exp(-2j*np.pi*freqs*self.delays[2*(beam-1)+0,:]) * \
                                             self.gains[2*(beam-1)+0,:]).astype(np.complex64)
            self.cgains[2*(beam-1)+1,:,:] = (np.exp(-2j*np.pi*freqs*self.delays[2*(beam-1)+1,:]) * \
                                             self.gains[2*(beam-1)+1,:]).astype(np.complex64)
            BFSync()
            self.log.info('  Complex gains set - beam %i' % beam)
            
            return True
            
        elif forceUpdate:
            self.log.info("Beamformer: New sequence configuration received")
            
            # Compute the complex gains needed for the beamformer
            freqs = CHAN_BW * (hdr['chan0'] + np.arange(hdr['nchan']))
            freqs.shape = (freqs.size, 1)
            for beam in xrange(1, self.nbeam_max+1):
                self.cgains[2*(beam-1)+0,:,:] = (np.exp(-2j*np.pi*freqs*self.delays[2*(beam-1)+0,:]) \
                                                 * self.gains[2*(beam-1)+0,:]).astype(np.complex64)
                self.cgains[2*(beam-1)+1,:,:] = (np.exp(-2j*np.pi*freqs*self.delays[2*(beam-1)+1,:]) \
                                                 * self.gains[2*(beam-1)+1,:]).astype(np.complex64)
                BFSync()
                self.log.info('  Complex gains set - beam %i' % beam)
                
            return True
            
        else:
            return False
        
    #@ISC.logException
    def main(self):
        cpu_affinity.set_core(self.core)
        if self.gpu != -1:
            BFSetGPU(self.gpu)
        self.bind_proclog.update({'ncore': 1, 
                                  'core0': cpu_affinity.get_core(),
                                  'ngpu': 1,
                                  'gpu0': BFGetGPU(),})
        
        with self.oring.begin_writing() as oring:
            for iseq in self.iring.read(guarantee=self.guarantee):
                ihdr = json.loads(iseq.header.tostring())
                
                self.sequence_proclog.update(ihdr)
                
                nchan  = ihdr['nchan']
                nstand = ihdr['nstand']
                npol   = ihdr['npol']
                
                status = self.updateConfig( self.configMessage(), ihdr, iseq.time_tag, forceUpdate=True )
                
                igulp_size = self.ntime_gulp*nchan*nstand*npol              # 4+4 complex
                ogulp_size = self.ntime_gulp*nchan*self.nbeam_max*npol*8    # complex64
                ishape = (self.ntime_gulp,nchan,nstand*npol)
                oshape = (self.ntime_gulp,nchan,self.nbeam_max*2)
                
                ticksPerTime = int(FS) / int(CHAN_BW)
                base_time_tag = iseq.time_tag
                
                ohdr = ihdr.copy()
                ohdr['nstand'] = self.nbeam_max
                ohdr['nbit'] = 32
                ohdr['complex'] = True
                ohdr_str = json.dumps(ohdr)
                
                self.oring.resize(ogulp_size)
                
                prev_time = time.time()
                with oring.begin_sequence(time_tag=iseq.time_tag, header=ohdr_str) as oseq:
                    for ispan in iseq.read(igulp_size):
                        if ispan.size < igulp_size:
                            continue # Ignore final gulp
                        curr_time = time.time()
                        acquire_time = curr_time - prev_time
                        prev_time = curr_time
                        
                        with oseq.reserve(ogulp_size) as ospan:
                            curr_time = time.time()
                            reserve_time = curr_time - prev_time
                            prev_time = curr_time
                            
                            ## Setup and load
                            idata = ispan.data_view('ci4').reshape(ishape)
                            odata = ospan.data_view(np.complex64)#.reshape(oshape)
                            
                            ## Copy
                            #print(idata.shape)
                            #print(self.tdata.shape)
                            #copy_array(self.tdata, idata)
                            #unpack(idata.reshape(704*480*192), self.tdata.reshape(704*480*192))
                            ##
                            ## Beamform
                            #print(self.cgains.dtype, self.tdata.dtype, self.bdata.dtype)
                            self.bdata = self.bfbf.matmul(1.0, self.cgains.transpose(1,0,2), self.tdata.transpose(1,2,0), 0.0, self.bdata)
                            ##
                            #### Transpose, save and cleanup
                            ##copy_array(self.ldata, self.bdata)
                            ##print(odata.shape)
                            ##print(self.ldata.shape)
                            ##print(self.ldata.transpose(2,0,1).shape)
                            #odata[...] = self.ldata.transpose(2,0,1)
                            #copy_array(odata, self.ldata.transpose(2,0,1).reshape(odata.shape))
                            
                        ## Update the base time tag
                        base_time_tag += self.ntime_gulp*ticksPerTime
                        
                        ## Check for an update to the configuration
                        self.updateConfig( self.configMessage(), ihdr, base_time_tag, forceUpdate=False )
                        
                        curr_time = time.time()
                        process_time = curr_time - prev_time
                        prev_time = curr_time
                        self.perf_proclog.update({'acquire_time': acquire_time, 
                                                  'reserve_time': reserve_time, 
                                                  'process_time': process_time,})
