import tables as tb
import numpy  as np

from invisible_cities.core            import system_of_units as units
from invisible_cities.io.mcinfo_io    import units_dict
from invisible_cities.io.mcinfo_io    import read_mcinfo_evt
from invisible_cities.reco            import tbl_functions as tbl

from invisible_cities.evm.event_model import Waveform
from invisible_cities.evm.nh5         import MCHitInfo
from invisible_cities.evm.nh5         import MCParticleInfo

from . nh5 import MCExtentInfo, MCWaveformInfo

from typing import Mapping


class mc_sns_response_writer:
    """Add MC sensor response info to file."""
    def __init__(self, h5file, compression = 'ZLIB4'):

        self.h5file      = h5file
        self.compression = compression
        self._create_tables()
        self.reset()
        self.current_tables = None
        self.bin_width = 1.*units.millisecond

        self.last_written_hit      = 0
        self.last_written_particle = 0
        self.last_written_wvf      = 0
        self.first_extent_row      = True
        self.first_file            = True

    def reset(self):
        # last visited row
        self.last_row              = 0

    def _create_tables(self):
        """Create tables in MC group in file h5file."""
        if '/MC' in self.h5file:
            MC = self.h5file.root.MC
        else:
            MC = self.h5file.create_group(self.h5file.root, "MC")

        self.extent_table = self.h5file.create_table(MC, "extents",
                                                     description = MCExtentInfo,
                                                     title       = "extents",
                                                     filters     = tbl.filters(self.compression))

        # Mark column to index after populating table
        self.extent_table.set_attr('columns_to_index', ['evt_number'])

        self.hit_table = self.h5file.create_table(MC, "hits",
                                                  description = MCHitInfo,
                                                  title       = "hits",
                                                  filters     = tbl.filters(self.compression))

        self.particle_table = self.h5file.create_table(MC, "particles",
                                                       description = MCParticleInfo,
                                                       title       = "particles",
                                                       filters     = tbl.filters(self.compression))

        self.wvf_table = self.h5file.create_table(MC, "waveforms",
                                                  description = MCWaveformInfo,
                                                  title       = "waveforms",
                                                  filters     = tbl.filters(self.compression))


    def __call__(self, mctables: (tb.Table, tb.Table, tb.Table, tb.Table),
                 sns_response: Mapping[int, Mapping[int, Waveform]], evt_number: int):
        if mctables is not self.current_tables:
            self.current_tables = mctables
            self.reset()

        extents = mctables[0]
        for iext in range(self.last_row, len(extents)):
            this_row = extents[iext]
            if this_row['evt_number'] == evt_number:
                if iext == 0:
                    if self.first_file:
                        modified_hit          = this_row['last_hit']
                        modified_particle     = this_row['last_particle']
                        modified_wvf          = len(sns_response[evt_number]) - 1
                        self.first_extent_row = False
                        self.first_file       = False
                    else:
                        modified_hit          = this_row['last_hit'] + self.last_written_hit + 1
                        modified_particle     = this_row['last_particle'] + self.last_written_particle + 1
                        modified_wvf          = self.last_written_wvf + len(sns_response[evt_number])
                        self.first_extent_row = False

                elif self.first_extent_row:
                    previous_row          = extents[iext-1]
                    modified_hit          = this_row['last_hit'] - previous_row['last_hit'] + self.last_written_hit - 1
                    modified_particle     = this_row['last_particle'] - previous_row['last_particle'] + self.last_written_particle - 1
                    modified_wvf          = self.last_written_wvf + len(sns_response[evt_number]) - 1
                    self.first_extent_row = False
                    self.first_file       = False
                else:
                    previous_row      = extents[iext-1]
                    modified_hit      = this_row['last_hit'] - previous_row['last_hit'] + self.last_written_hit
                    modified_particle = this_row['last_particle'] - previous_row['last_particle'] + self.last_written_particle
                    modified_wvf       = self.last_written_wvf + len(sns_response[evt_number])

                modified_row                  = self.extent_table.row
                modified_row['evt_number']    = evt_number
                modified_row['last_sns_data'] = modified_wvf
                modified_row['last_hit']      = modified_hit
                modified_row['last_particle'] = modified_particle
                modified_row.append()

                self.last_written_hit      = modified_hit
                self.last_written_particle = modified_particle
                self.last_written_wvf      = modified_wvf

                break

        self.extent_table.flush()

        hits, particles, _ = read_mcinfo_evt(mctables, evt_number, self.last_row)
        waveforms          = sns_response[evt_number]
        self.last_row = iext + 1

        for h in hits:
            new_row = self.hit_table.row
            new_row['hit_position']  = h[0]
            new_row['hit_time']      = h[1]
            new_row['hit_energy']    = h[2]
            new_row['label']         = h[3]
            new_row['particle_indx'] = h[4]
            new_row['hit_indx']      = h[5]
            new_row.append()
        self.hit_table.flush()

        for p in particles:
            new_row = self.particle_table.row
            new_row['particle_indx']  = p[0]
            new_row['particle_name']  = p[1]
            new_row['primary']        = p[2]
            new_row['mother_indx']    = p[3]
            new_row['initial_vertex'] = p[4]
            new_row['final_vertex']   = p[5]
            new_row['initial_volume'] = p[6]
            new_row['final_volume']   = p[7]
            new_row['momentum']       = p[8]
            new_row['kin_energy']     = p[9]
            new_row['creator_proc']   = p[10]
            new_row.append()
        self.particle_table.flush()

        for sns, charge in waveforms.items():
            new_row = self.wvf_table.row
            new_row['sensor_id'] = sns
            new_row['time_bin']  = 0
            new_row['charge']    = charge
            new_row.append()
        self.wvf_table.flush()


def read_SiPM_bin_width_from_conf(h5f):

    h5config = h5f.root.MC.configuration
    bin_width = None
    for row in h5config:
        param_name = row['param_key'].decode('utf-8','ignore')
        if param_name.find('time_binning') >= 0:
            param_value = row['param_value'].decode('utf-8','ignore')
            numb, unit  = param_value.split()
            if param_name.find('SiPM') >= 0:
                bin_width = float(numb) * units_dict[unit]

    if bin_width is None:
        bin_width = 1 * units.microsecond

    return bin_width
    

def read_mcsns_response_evt (mctables: (tb.Table, tb.Table),
                             event_number: int, last_line_of_event,
                             bin_width, last_row=0) -> [tb.Table]:

    h5extents   = mctables[0]
    h5waveforms = mctables[1]

    current_event = {}
    event_range   = (last_row, int(1e9))

    iwvf = int(0)
    if event_range[0] > 0:
        iwvf = int(h5extents[event_range[0]-1][last_line_of_event]) + 1

    for iext in range(*event_range):
        this_row = h5extents[iext]
        if this_row['evt_number'] == event_number:
            # the index of the first waveform is 0 unless the first event
            #  written is to be skipped: in this case they must be read from the extents
            iwvf_end          = int(h5extents[iext][last_line_of_event])
            if iwvf_end < iwvf: break
            current_sensor_id = h5waveforms[iwvf]['sensor_id']
            time_bins         = []
            charges           = []
            while iwvf <= iwvf_end:
                wvf_row   = h5waveforms[iwvf]
                sensor_id = wvf_row['sensor_id']

                if sensor_id == current_sensor_id:
                    time_bins.append(wvf_row['time_bin'])
                    charges.  append(wvf_row['charge'])
                else:
                    times = np.array(time_bins) * bin_width
                    current_event[current_sensor_id] = Waveform(times, charges, bin_width)

                    time_bins = []
                    charges   = []
                    time_bins.append(wvf_row['time_bin'])
                    charges.append(wvf_row['charge'])

                    current_sensor_id = sensor_id

                iwvf += 1

            times     = np.array(time_bins) * bin_width
            current_event[current_sensor_id] = Waveform(times, charges, bin_width)
            break

    return current_event

def go_through_file(h5f, h5waveforms, event_range=(0, int(1e9)), bin_width=1.*units.microsecond, kind_of_waveform='data'):

    h5extents   = h5f.root.MC.extents
    sns_info    = (h5extents, h5waveforms)

    last_line_name = 'last_sns_' + kind_of_waveform     
    events_in_file = len(h5extents)
    
    all_events     = {}
    for iext in range(*event_range):
        if iext >= events_in_file:
            break

        evt_number = h5extents[iext]['evt_number']
        wvf_rows = read_mcsns_response_evt(sns_info, evt_number, last_line_name, bin_width, iext)
        all_events[evt_number] = wvf_rows

    return all_events

def read_mcsns_response(file_name, event_range=(0, int(1e9))) ->Mapping[int, Mapping[int, Waveform]]:

    kind_of_waveform = 'data'

    with tb.open_file(file_name, mode='r') as h5f:
        bin_width   = read_SiPM_bin_width_from_conf(h5f)
        h5waveforms = h5f.root.MC.waveforms
        all_events  = go_through_file(h5f, h5waveforms, event_range, bin_width, kind_of_waveform)

        return all_events

def read_mcTOFsns_response(file_name, event_range=(0, int(1e9))) ->Mapping[int, Mapping[int, Waveform]]:

    kind_of_waveform = 'tof'
    bin_width        = 5 * units.picosecond

    with tb.open_file(file_name, mode='r') as h5f:
        h5waveforms = h5f.root.MC.tof_waveforms
        all_events = go_through_file(h5f, h5waveforms, event_range, bin_width, kind_of_waveform)

        return all_events

