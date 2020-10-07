from blocks.block_control_base import BlockControl
import numpy as np

class CorrSubsel(BlockControl):
    nvis_out = 4656
    def set_baseline_select(self, subsel):
       subsel = np.array(subsel, dtype=np.int32)
       assert subsel.shape == (self.nvis_out, 2, 2)
       self.send_command(subsel=subsel.tolist())
