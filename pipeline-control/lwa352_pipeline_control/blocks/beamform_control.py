import time
import numpy as np

from .block_control_base import BlockControl

class BeamformControl(BlockControl):
    @staticmethod
    def _get_when_to_execute(slot):
        """
        Static method to convert a slot value into a unix-style timestamp.
        
        """
        return int(time.time()) + 1 + slot/100.0

    def update_calibration_gains(self, input_id, gains, slot=0):
        """
        Update calibration gains for a single beam and input.

        :param input_id: Zero-indexed Input ID for which coefficients are
            begin updated.
        :type input_id: int

        :param gains: Complex-valued gains to load. Should be a numpy
            array with a complex data type and ``nchan`` entries,
            where entry ``i`` corresponds to the ``i``th channel
            being processed by this pipeline.
        :type gains: numpy.array

        """
        # We can't send a numpy array through JSON, so encode
        # as a real-valued list with alternating real/imag entries.
        # This allows the standard JSON messaging scheme to be used,
        # But we could equally use binary strings (which would
        # be much more efficient)
        nchan = gains.shape[0]
        gains_real = np.zeros(2*nchan, dtype=np.float32)
        gains_real[0::2] = gains.real
        gains_real[1::2] = gains.imag
        self._send_command(
            coeffs = {
                'type': 'calibration',
                'input_id': input_id,
                'data': gains_real.tolist(),
                'when': self._get_when_to_execute(slot)
            }
        )

    def update_gains(self, beam_id, gains, slot=0):
        """
        Update the dipole gains for a single beam.
        
        :param beam_id: Zero-indexed Beam ID for which coefficients are
            begin updated.
        :type beam_id: int

        :param delays: Real-valued gains to load.  Should be a numpy array with
            ``ninput`` entries, where entry ``i`` corresponds to the gain to
            apply to the ``i``th beamformer input.
        :type delays: numpy.array
        
        """
        self._send_command(
            coeffs = {
                'type': 'gains',
                'beam_id': beam_id,
                'data': gains.tolist(),
                'when': self._get_when_to_execute(slot)
            }
        )

    def update_delays(self, beam_id, delays, slot=0):
        """
        Update geometric delays for a single beam.

        :param beam_id: Zero-indexed Beam ID for which coefficients are
            begin updated.
        :type beam_id: int

        :param delays: Real-valued delays to load, specified in nanoseconds.
            Should be a numpy array with ``ninput`` entries, where entry ``i``
            corresponds to the delay to apply to the ``i``th beamformer input.
        :type delays: numpy.array

        """
        self._send_command(
            coeffs = {
                'type': 'delays',
                'beam_id': beam_id,
                'data': delays.tolist(),
                'when': self._get_when_to_execute(slot)
            }
        )
