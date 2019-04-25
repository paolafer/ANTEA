import tables as tb

class MCExtentInfo(tb.IsDescription):
    """Store the last row of each table as metadata using
    Pytables.
    """
    evt_number    = tb.Int32Col(pos=0)
    last_sns_data = tb.UInt64Col(pos=1)
    last_hit      = tb.UInt64Col(pos=2)
    last_particle = tb.UInt64Col(pos=3)


class MCWaveformInfo(tb.IsDescription):
    """Describe a waveform."""
    sensor_id = tb.Int32Col(pos=0)
    time_bin  = tb.Float32Col(pos=1)
    charge    = tb.Float32Col(pos=2)
