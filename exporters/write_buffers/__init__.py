from .base_buffer import BaseBuffer
from .reservoir_sampling_buffer import ReservoirSamplingWriteBuffer

DEFAULT_WRITE_BUFFER = 'exporters.write_buffers.base_buffer.BaseBuffer'
RESERVOIR_SAMPLING_WRITE_BUFFER = 'exporters.write_buffers.ReservoirSamplingWriteBuffer'

__all__ = ['BaseBuffer', 'ReservoirSamplingWriteBuffer']
