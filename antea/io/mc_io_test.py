import os
import numpy  as np
import tables as tb

from . mc_io import mc_sns_response_writer

from . mc_io import read_mcsns_response
from . mc_io import read_mcTOFsns_response

from invisible_cities.reco.tbl_functions import get_mc_info


def test_mc_sns_response_writer_reset(output_tmpdir):
    filein  = os.environ['ANTEADIR'] + '/testdata/full_ring_test.pet.h5'
    fileout = os.path.join(output_tmpdir, "test_mc_sns_response_writer_reset.h5")

    with tb.open_file(filein) as h5in:
        with tb.open_file(fileout, 'w') as h5out:

            mc_writer  = mc_sns_response_writer(h5out, h5in)
            events_in  = np.unique(h5in.root.MC.extents[:]['evt_number'])

            sns_dict = {1000: 1, 1001: 2, 1002: 3}
            wvfs     = {e: sns_dict for e in events_in}

            assert mc_writer.last_row              == 0

            mc_writer(get_mc_info(h5in), wvfs, events_in[0])
            assert mc_writer.last_row              == 1

            mc_writer.reset()
            assert mc_writer.last_row              == 0


def test_read_sensor_response():
    test_file = os.environ['ANTEADIR'] + '/testdata/full_ring_test.pet.h5'

    mc_sensor_dict = read_mcsns_response(test_file)
    waveforms = mc_sensor_dict[0]

    n_of_sensors = 796
    sensor_id    = 4162
    
    assert len(waveforms) == n_of_sensors
    assert waveforms[sensor_id].times == np.array([0.])
    assert waveforms[sensor_id].charges == np.array([8.])

def test_read_sensor_tof_response():
    test_file = os.environ['ANTEADIR'] + '/testdata/full_ring_test.pet.h5'

    mc_sensor_dict = read_mcTOFsns_response(test_file)
    waveforms = mc_sensor_dict[0]

    sensor_id = 4371
    bin_width = waveforms[-sensor_id].bin_width
    times = np.array([358, 1562, 5045, 5229, 5960, 6311, 14192]) * bin_width
    charges = np.array([1, 1, 1, 1, 1, 1, 1])

    assert np.allclose(waveforms[-sensor_id].times, times)
    assert np.allclose(waveforms[-sensor_id].charges, charges)

def test_read_last_sensor_response():
    test_file = os.environ['ANTEADIR'] + '/testdata/full_ring_test.pet.h5'

    mc_sensor_dict = read_mcsns_response(test_file)
    waveforms = mc_sensor_dict[0]

    with tb.open_file(test_file, mode='r') as h5in:
        last_written_id = h5in.root.MC.sensor_positions[-1][0]
        last_read_id = list(waveforms.keys())[-1]

        assert last_read_id == last_written_id
