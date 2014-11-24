#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_wvs
@file marine-integrations/mi/dataset/parser/adcpt_m_wvs.py
@author Ronald Ronquillo
@brief Parser for the adcpt_m_wvs dataset driver

This file contains code for the adcpt_m_wvs parser and code to produce data particles.

The wave record structure is an extensible, packed, binary, data format that contains the
processed results of a single burst of ADCP wave data. A burst is typically 20 minutes
of data sampled at 2 Hz. Wave Records are appended together into a file that represents a
deployment time. The wave record usually contains the wave height spectra, directional
spectra, wave parameters, and information about how the data was collected/processed. A
wave record can also contain raw time-series from pressure sensor, surface track and
orbital velocity measurements.

The waves record file (*.WVS) is in binary (HID:7F7A) format.
This format is similar to that of the binary PD0 recovered format.
The data is divided into files of ~20MB size.

Mal-formed sensor data records produce no particles.

Release notes:

Initial Release
"""

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


import calendar
import numpy
import re
import struct
from mi.core.exceptions import UnexpectedDataException, RecoverableSampleException
from mi.dataset.dataset_parser import BufferLoadingParser

from mi.core.common import BaseEnum

from mi.core.instrument.data_particle import DataParticle, DataParticleKey

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.common_regexes import UNSIGNED_INT_REGEX


#Data Type IDs
Header =                    31359   # Hex '\x7f\x7a'
Fixed_Leader =              1
Variable_Leader =           2
Velocity_Time_Series =      3
Amplitude_Time_Series =     4
Surface_Time_Series =       5
Pressure_Time_Series =      6
Velocity_Spectrum =         7
Surface_Track_Spectrum =    8
Pressure_Spectrum =         9
Directional_Spectrum =      10
Wave_Parameters =           11
Wave_Parameters2 =          12
Surface_Dir_Spectrum =              13
Heading_Pitch_Roll_Time_Series =    14
Bottom_Velocity_Time_Series =       15
Altitude_Time_Series =              16
Unknown =                           17

# The particle ID's that must be filled in a particle
EXPECTED_PARTICLE_IDS_SET = frozenset(
    [Fixed_Leader,
     Variable_Leader,
     Velocity_Spectrum,
     Surface_Track_Spectrum,
     Pressure_Spectrum,
     Directional_Spectrum,
     Wave_Parameters,
     Heading_Pitch_Roll_Time_Series])


class AdcptMWVSParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    FILE_TIME = "file_time"
    SEQUENCE_NUMBER = "sequence_number"
    FILE_MODE = "file_mode"
    REC_TIME_SERIES = "rec_time_series"
    REC_SPECTRA = "rec_spectra"
    REC_DIR_SPEC = "rec_dir_spec"
    SAMPLES_PER_BURST = "samples_per_burst"
    TIME_BETWEEN_SAMPLES = "time_between_samples"
    TIME_BETWEEN_BURSTS_SEC = "time_between_bursts_sec"
    BIN_SIZE = "bin_size"
    BIN_1_MIDDLE = "bin_1_middle"
    NUM_RANGE_BINS = "num_range_bins"
    NUM_VEL_BINS = "num_vel_bins"
    NUM_INT_BINS = "num_int_bins"
    NUM_BEAMS = "num_beams"
    BEAM_CONF = "beam_conf"
    WAVE_PARAM_SOURCE = "wave_param_source"
    NFFT_SAMPLES = "nfft_samples"
    NUM_DIRECTIONAL_SLICES = "num_directional_slices"
    NUM_FREQ_BINS = "num_freq_bins"
    WINDOW_TYPE = "window_type"
    USE_PRESS_4_DEPTH = "use_press_4_depth"
    USE_STRACK_4_DEPTH = "use_strack_4_depth"
    STRACK_SPEC = "strack_spec"
    PRESS_SPEC = "press_spec"
    VEL_MIN = "vel_min"
    VEL_MAX = "vel_max"
    VEL_STD = "vel_std"
    VEL_MAX_CHANGE = "vel_max_change"
    VEL_PCT_GD = "vel_pct_gd"
    SURF_MIN = "surf_min"
    SURF_MAX = "surf_max"
    SURF_STD = "surf_std"
    SURF_MAX_CHNG = "surf_max_chng"
    SURF_PCT_GD = "surf_pct_gd"
    TBE_MAX_DEV = "tbe_max_dev"
    H_MAX_DEV = "h_max_dev"
    PR_MAX_DEV = "pr_max_dev"
    NOM_DEPTH = "nom_depth"
    CAL_PRESS = "cal_press"
    DEPTH_OFFSET = "depth_offset"
    CURRENTS = "currents"
    SMALL_WAVE_FREQ = "small_wave_freq"
    SMALL_WAVE_THRESH = "small_wave_thresh"
    TILTS = "tilts"
    FIXED_PITCH = "fixed_pitch"
    FIXED_ROLL = "fixed_roll"
    BOTTOM_SLOPE_X = "bottom_slope_x"
    BOTTOM_SLOPE_Y = "bottom_slope_y"
    DOWN = "down"
    TRANS_V2_SURF = "trans_v2_surf"
    SCALE_SPEC = "scale_spec"
    SAMPLE_RATE = "sample_rate"
    FREQ_THRESH = "freq_thresh"
    DUMMY_SURF = "dummy_surf"
    REMOVE_BIAS = "remove_bias"
    DIR_CUTOFF = "dir_cutoff"
    HEADING_VARIATION = "heading_variation"
    SOFT_REV = "soft_rev"
    CLIP_PWR_SPEC = "clip_pwr_spec"
    DIR_P2 = "dir_p2"
    HORIZONTAL = "horizontal"
    START_TIME = "start_time"
    STOP_TIME = "stop_time"
    FREQ_LO = "freq_lo"
    AVERAGE_DEPTH = "average_depth"
    ALTITUDE = "altitude"
    BIN_MAP = "bin_map"
    DISC_FLAG = "disc_flag"
    PCT_GD_PRESS = "pct_gd_press"
    AVG_SS = "avg_ss"
    AVG_TEMP = "avg_temp"
    PCT_GD_SURF = "pct_gd_surf"
    PCT_GD_VEL = "pct_gd_vel"
    HEADING_OFFSET = "heading_offset"
    HS_STD = "hs_std"
    VS_STD = "vs_std"
    PS_STD = "ps_std"
    DS_FREQ_HI = "ds_freq_hi"
    VS_FREQ_HI = "vs_freq_hi"
    PS_FREQ_HI = "ps_freq_hi"
    SS_FREQ_HI = "ss_freq_hi"
    X_VEL = "x_vel"
    Y_VEL = "y_vel"
    AVG_PITCH = "avg_pitch"
    AVG_ROLL = "avg_roll"
    AVG_HEADING = "avg_heading"
    SAMPLES_COLLECTED = "samples_collected"
    VSPEC_PCT_MEASURED = "vspec_pct_measured"
    VSPEC_NUM_FREQ = "vspec_num_freq"
    VSPEC_DAT = "vspec_dat"
    SSPEC_NUM_FREQ = "sspec_num_freq"
    SSPEC_DAT = "sspec_dat"
    PSPEC_NUM_FREQ = "pspec_num_freq"
    PSPEC_DAT = "pspec_dat"
    DSPEC_NUM_FREQ = "dspec_num_freq"
    DSPEC_NUM_DIR = "dspec_num_dir"
    DSPEC_GOOD = "dspec_good"
    DSPEC_DAT = "dspec_dat"
    WAVE_HS1 = "wave_hs1"
    WAVE_TP1 = "wave_tp1"
    WAVE_DP1 = "wave_dp1"
    WAVE_HS2 = "wave_hs2"
    WAVE_TP2 = "wave_tp2"
    WAVE_DP2 = "wave_dp2"
    WAVE_DM = "wave_dm"
    HPR_NUM_SAMPLES = "hpr_num_samples"
    BEAM_ANGLE = "beam_angle"
    HEADING_TIME_SERIES = "heading_time_series"
    PITCH_TIME_SERIES = "pitch_time_series"
    ROLL_TIME_SERIES = "roll_time_series"
    SPARE = "spare"

# Basic patterns
common_matches = {
    'UINT': UNSIGNED_INT_REGEX
}

common_matches.update(AdcptMWVSParticleKey.__dict__)


# Regex to extract the timestamp from the WVS log file path
# (path/to/CE01ISSM-ADCPT_YYYYMMDD_###_TS.WVS)
# 'CE01ISSM-ADCPT_20140418_000_TS1404180021.WVS'
# 'CE01ISSM-ADCPT_20140418_000_TS1404180021 - excerpt.WVS'
FILE_NAME_MATCHER = re.compile(r"""(?x)
    .+CE01ISSM-ADCPT_
    %(UINT)s_(?P<%(SEQUENCE_NUMBER)s> %(UINT)s)_TS(?P<%(FILE_TIME)s> %(UINT)s).*\.WVS
    """ % common_matches, re.VERBOSE | re.DOTALL)

# Header data:
HEADER_MATCHER = re.compile(r"""(?x)
    \x7f\x7a(?P<Spare1> (.){2}) (?P<Record_Size> (.{4})) (?P<Spare2_4> (.){3}) (?P<NumDataTypes> (.))
    """, re.VERBOSE | re.DOTALL)

"""
Format	C Type	            Python type	        Standard size
x	    pad byte	        no value
c	    char	            string of length 1	1
b	    signed char	        integer	            1
B	    unsigned char       integer	            1
?	    _Bool	            bool	            1
h	    short	            integer	            2
H	    unsigned short	    integer	            2
i	    int	                integer	            4
I	    unsigned int	    integer	            4
l	    long	            integer	            4
L	    unsigned long	    integer	            4
q	    long long	        integer	            8
Q	    unsigned long long  integer	            8
f	    float	            float	            4
d	    double	            float	            8
s	    char[]	            string
p	    char[]	            string
P	    void *	            integer
"""

def make_null_parameters(rules):
    """
    Get the parameter names from an encoding rules list and create a list with NULL parameters
    """
    return [{DataParticleKey.VALUE_ID: key[:1][0], DataParticleKey.VALUE: None}
            for key in rules if 'spare' not in key]

# ENCODING RULES = [parameter name, unpack format]
FIXED_LEADER_ENCODING_RULES = [
    (AdcptMWVSParticleKey.FILE_MODE, 'B'),
    (AdcptMWVSParticleKey.REC_TIME_SERIES, 'B'),
    (AdcptMWVSParticleKey.REC_SPECTRA, 'B'),
    (AdcptMWVSParticleKey.REC_DIR_SPEC, 'B'),
    (AdcptMWVSParticleKey.SAMPLES_PER_BURST, 'H'),
    (AdcptMWVSParticleKey.TIME_BETWEEN_SAMPLES, 'H'),
    (AdcptMWVSParticleKey.TIME_BETWEEN_BURSTS_SEC, 'H'),
    (AdcptMWVSParticleKey.BIN_SIZE, 'H'),
    (AdcptMWVSParticleKey.BIN_1_MIDDLE, 'H'),
    (AdcptMWVSParticleKey.NUM_RANGE_BINS, 'B'),
    (AdcptMWVSParticleKey.NUM_VEL_BINS, 'B'),
    (AdcptMWVSParticleKey.NUM_INT_BINS, 'B'),
    (AdcptMWVSParticleKey.NUM_BEAMS, 'B'),
    (AdcptMWVSParticleKey.BEAM_CONF, 'B'),
    (AdcptMWVSParticleKey.WAVE_PARAM_SOURCE, 'B'),
    (AdcptMWVSParticleKey.NFFT_SAMPLES, 'H'),
    (AdcptMWVSParticleKey.NUM_DIRECTIONAL_SLICES, 'H'),
    (AdcptMWVSParticleKey.NUM_FREQ_BINS, 'H'),
    (AdcptMWVSParticleKey.WINDOW_TYPE, 'H'),
    (AdcptMWVSParticleKey.USE_PRESS_4_DEPTH, 'B'),
    (AdcptMWVSParticleKey.USE_STRACK_4_DEPTH, 'B'),
    (AdcptMWVSParticleKey.STRACK_SPEC, 'B'),
    (AdcptMWVSParticleKey.PRESS_SPEC, 'B'),
#SCREENING_TYPE_ENCODING_RULES
    (AdcptMWVSParticleKey.VEL_MIN, 'h'),
    (AdcptMWVSParticleKey.VEL_MAX, 'h'),
    (AdcptMWVSParticleKey.VEL_STD, 'B'),
    (AdcptMWVSParticleKey.VEL_MAX_CHANGE, 'H'),
    (AdcptMWVSParticleKey.VEL_PCT_GD, 'B'),
    (AdcptMWVSParticleKey.SURF_MIN, 'i'),
    (AdcptMWVSParticleKey.SURF_MAX, 'i'),
    (AdcptMWVSParticleKey.SURF_STD, 'B'),
    (AdcptMWVSParticleKey.SURF_MAX_CHNG, 'i'),
    (AdcptMWVSParticleKey.SURF_PCT_GD, 'B'),
    (AdcptMWVSParticleKey.TBE_MAX_DEV, 'H'),
    (AdcptMWVSParticleKey.H_MAX_DEV, 'H'),
    (AdcptMWVSParticleKey.PR_MAX_DEV, 'B'),
    (AdcptMWVSParticleKey.NOM_DEPTH, 'I'),
    (AdcptMWVSParticleKey.CAL_PRESS, 'B'),
    (AdcptMWVSParticleKey.DEPTH_OFFSET, 'i'),
    (AdcptMWVSParticleKey.CURRENTS, 'B'),
    (AdcptMWVSParticleKey.SMALL_WAVE_FREQ, 'H'),
    (AdcptMWVSParticleKey.SMALL_WAVE_THRESH, 'h'),
    (AdcptMWVSParticleKey.TILTS, 'B'),
    (AdcptMWVSParticleKey.FIXED_PITCH, 'h'),
    (AdcptMWVSParticleKey.FIXED_ROLL, 'h'),
    (AdcptMWVSParticleKey.BOTTOM_SLOPE_X, 'h'),
    (AdcptMWVSParticleKey.BOTTOM_SLOPE_Y, 'h'),
    (AdcptMWVSParticleKey.DOWN, 'B'),
    (AdcptMWVSParticleKey.SPARE, '17x'),
#END_SCREENING_TYPE_ENCODING_RULES
    (AdcptMWVSParticleKey.TRANS_V2_SURF, 'B'),
    (AdcptMWVSParticleKey.SCALE_SPEC, 'B'),
    (AdcptMWVSParticleKey.SAMPLE_RATE, 'f'),
    (AdcptMWVSParticleKey.FREQ_THRESH, 'f'),
    (AdcptMWVSParticleKey.DUMMY_SURF, 'B'),
    (AdcptMWVSParticleKey.REMOVE_BIAS, 'B'),
    (AdcptMWVSParticleKey.DIR_CUTOFF, 'H'),
    (AdcptMWVSParticleKey.HEADING_VARIATION, 'h'),
    (AdcptMWVSParticleKey.SOFT_REV, 'B'),
    (AdcptMWVSParticleKey.CLIP_PWR_SPEC, 'B'),
    (AdcptMWVSParticleKey.DIR_P2, 'B'),
    (AdcptMWVSParticleKey.HORIZONTAL, 'B')
]

NULL_FIXED_LEADER = make_null_parameters(FIXED_LEADER_ENCODING_RULES)

VARIABLE_LEADER_ENCODING_RULES = [
    (AdcptMWVSParticleKey.START_TIME, '8B'),
    (AdcptMWVSParticleKey.STOP_TIME, '8B'),
    (AdcptMWVSParticleKey.FREQ_LO, 'H'),
    (AdcptMWVSParticleKey.AVERAGE_DEPTH, 'I'),
    (AdcptMWVSParticleKey.ALTITUDE, 'I'),
    (AdcptMWVSParticleKey.BIN_MAP, '128b'),
    (AdcptMWVSParticleKey.DISC_FLAG, 'B'),
    (AdcptMWVSParticleKey.PCT_GD_PRESS, 'B'),
    (AdcptMWVSParticleKey.AVG_SS, 'H'),
    (AdcptMWVSParticleKey.AVG_TEMP, 'H'),
    (AdcptMWVSParticleKey.PCT_GD_SURF, 'B'),
    (AdcptMWVSParticleKey.PCT_GD_VEL, 'B'),
    (AdcptMWVSParticleKey.HEADING_OFFSET, 'h'),
    (AdcptMWVSParticleKey.HS_STD, 'I'),
    (AdcptMWVSParticleKey.VS_STD, 'I'),
    (AdcptMWVSParticleKey.PS_STD, 'I'),
    (AdcptMWVSParticleKey.DS_FREQ_HI, 'I'),
    (AdcptMWVSParticleKey.VS_FREQ_HI, 'I'),
    (AdcptMWVSParticleKey.PS_FREQ_HI, 'I'),
    (AdcptMWVSParticleKey.SS_FREQ_HI, 'I'),
    (AdcptMWVSParticleKey.X_VEL, 'h'),
    (AdcptMWVSParticleKey.Y_VEL, 'h'),
    (AdcptMWVSParticleKey.AVG_PITCH, 'h'),
    (AdcptMWVSParticleKey.AVG_ROLL, 'h'),
    (AdcptMWVSParticleKey.AVG_HEADING, 'h'),
    (AdcptMWVSParticleKey.SAMPLES_COLLECTED, 'h'),
    (AdcptMWVSParticleKey.VSPEC_PCT_MEASURED, 'h')
]

NULL_VARIABLE_LEADER = make_null_parameters(VARIABLE_LEADER_ENCODING_RULES)

VELOCITY_SPECTRUM_ENCODING_RULES = [
    (AdcptMWVSParticleKey.VSPEC_NUM_FREQ, 'H'),
    (AdcptMWVSParticleKey.VSPEC_DAT, 'i')
]

NULL_VELOCITY_SPECTRUM = make_null_parameters(VELOCITY_SPECTRUM_ENCODING_RULES)

SURFACE_TRACK_SPECTRUM_ENCODING_RULES = [
    (AdcptMWVSParticleKey.SSPEC_NUM_FREQ, 'H'),
    (AdcptMWVSParticleKey.SSPEC_DAT, 'i')
]

NULL_SURFACE_TRACK_SPECTRUM = make_null_parameters(SURFACE_TRACK_SPECTRUM_ENCODING_RULES)

PRESSURE_SPECTRUM_ENCODING_RULES = [
    (AdcptMWVSParticleKey.PSPEC_NUM_FREQ, 'H'),
    (AdcptMWVSParticleKey.PSPEC_DAT, 'i')
]

NULL_PRESSURE_SPECTRUM = make_null_parameters(PRESSURE_SPECTRUM_ENCODING_RULES)

DIRECTIONAL_SPECTRUM_ENCODING_RULES = [
    (AdcptMWVSParticleKey.DSPEC_NUM_FREQ, 'H'),  # COUNT uint32[dspec_num_freq][dspec_num_dir]
    (AdcptMWVSParticleKey.DSPEC_NUM_DIR, 'H'),   # COUNT
    (AdcptMWVSParticleKey.DSPEC_GOOD, 'H'),
    (AdcptMWVSParticleKey.DSPEC_DAT, 'I')
]

NULL_DIRECTIONAL_SPECTRUM = make_null_parameters(DIRECTIONAL_SPECTRUM_ENCODING_RULES)

WAVE_PARAMETER_ENCODING_RULES = [
    (AdcptMWVSParticleKey.WAVE_HS1, 'h'),
    (AdcptMWVSParticleKey.WAVE_TP1, 'h'),
    (AdcptMWVSParticleKey.WAVE_DP1, 'h'),
    (AdcptMWVSParticleKey.SPARE, 'x'),  # SPARE!
    (AdcptMWVSParticleKey.WAVE_HS2, 'h'),
    (AdcptMWVSParticleKey.WAVE_TP2, 'h'),
    (AdcptMWVSParticleKey.WAVE_DP2, 'h'),
    (AdcptMWVSParticleKey.WAVE_DM, 'h')
]

NULL_WAVE_PARAMETER = make_null_parameters(WAVE_PARAMETER_ENCODING_RULES)

HPR_TIME_SERIES_ENCODING_RULES = [
    (AdcptMWVSParticleKey.HPR_NUM_SAMPLES, 'H'),   # COUNT
    (AdcptMWVSParticleKey.BEAM_ANGLE, 'H'),
    (AdcptMWVSParticleKey.SPARE, 'H'),  # SPARE!
    ([AdcptMWVSParticleKey.HEADING_TIME_SERIES,
      AdcptMWVSParticleKey.PITCH_TIME_SERIES,
      AdcptMWVSParticleKey.ROLL_TIME_SERIES]
     , 'h')
]

NULL_HPR_TIME_SERIES = make_null_parameters(HPR_TIME_SERIES_ENCODING_RULES)

# Conversion for unpack format to offset
FMT_SIZES = {
    '8B': 8,
    '17b': 17,
    '17x': 17,
    'x': 1,
    '128b': 128,
    'B': 1,
    'b': 1,
    'H': 2,
    'h': 2,
    'I': 4,
    'i': 4,
    'f': 4
}

HEADER_NUM_DATA_TYPES_OFFSET = 11
HEADER_OFFSETS_OFFSET = 12
ID_TYPE_SIZE = 2

# Size and Indices used for unpacking Heading, Pitch, Role data type
HPR_TIME_SERIES_ARRAY_SIZE = 3
HEADING_TIME_SERIES_IDX = 0
PITCH_TIME_SERIES_IDX = 1
ROLL_TIME_SERIES_IDX = 2

# Indices into an encoding rules list
ENCODE_RULES = 0
ENCODE_FUNC = 1
ENCODE_NULL = 2


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m WVS recovered data
    """
    SAMPLE = 'adcpt_m_wvs_recovered'  # instrument data particle


class AdcptMWVSInstrumentDataParticle(DataParticle):
    """
    Class for generating the adcpt_m_instrument_wvs_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE
    _file_time = None
    _sequence_number = None

    def __init__(self, *args, **kwargs):

        # Data Type ID: [Encoding Rules, Encoding Function, NULL Filler]
        self.encoding_func_dict = {
            Fixed_Leader: [FIXED_LEADER_ENCODING_RULES,
                           self._parse_values, NULL_FIXED_LEADER],
            Variable_Leader: [VARIABLE_LEADER_ENCODING_RULES,
                              self._parse_variable_leader, NULL_VARIABLE_LEADER],
            Velocity_Spectrum: [VELOCITY_SPECTRUM_ENCODING_RULES,
                                self._parse_values_with_array, NULL_VELOCITY_SPECTRUM],
            Surface_Track_Spectrum: [SURFACE_TRACK_SPECTRUM_ENCODING_RULES,
                                     self._parse_values_with_array, NULL_SURFACE_TRACK_SPECTRUM],
            Pressure_Spectrum: [PRESSURE_SPECTRUM_ENCODING_RULES,
                                self._parse_values_with_array, NULL_PRESSURE_SPECTRUM],
            Directional_Spectrum: [DIRECTIONAL_SPECTRUM_ENCODING_RULES,
                                   self._parse_directional_spectrum, NULL_DIRECTIONAL_SPECTRUM],
            Wave_Parameters: [WAVE_PARAMETER_ENCODING_RULES,
                              self._parse_values, NULL_WAVE_PARAMETER],
            Heading_Pitch_Roll_Time_Series: [HPR_TIME_SERIES_ENCODING_RULES,
                                             self._parse_hpr_time_series, NULL_HPR_TIME_SERIES]
        }

        super(AdcptMWVSInstrumentDataParticle, self).__init__(*args, **kwargs)

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered Instrument Data Particle.
        """

        self.final_result = []
        retrieved_data_types = set()    # keeps track of data type ID's unpacked from record

        # get the file time from the file name
        if self._file_time:
            self.final_result.append(self._encode_value(
                AdcptMWVSParticleKey.FILE_TIME, self._file_time, str))
        else:
            self.final_result.append({DataParticleKey.VALUE_ID: AdcptMWVSParticleKey.FILE_TIME,
                                      DataParticleKey.VALUE: None})

        # get the sequence number from the file name
        if self._sequence_number:
            self.final_result.append(self._encode_value(
                AdcptMWVSParticleKey.SEQUENCE_NUMBER, self._sequence_number, int))
        else:
            self.final_result.append({DataParticleKey.VALUE_ID: AdcptMWVSParticleKey.SEQUENCE_NUMBER,
                                      DataParticleKey.VALUE: None})

        # get the number of data types from the Header
        num_data_types = struct.unpack_from('<B', self.raw_data, HEADER_NUM_DATA_TYPES_OFFSET)
        # get the list of offsets from the Header
        offsets = struct.unpack_from('<%sI' % num_data_types, self.raw_data, HEADER_OFFSETS_OFFSET)

        # unpack Type IDs from the offsets
        for offset in offsets:
            data_type_id, = struct.unpack_from('h', self.raw_data, offset)
            # keep track of retrieved data types
            retrieved_data_types.add(data_type_id)

            # feed the data through the corresponding encoding function and encoding rules
            try:
                self.encoding_func_dict[data_type_id][ENCODE_FUNC](
                    offset+ID_TYPE_SIZE, self.encoding_func_dict[data_type_id][ENCODE_RULES])
            except KeyError:
                log.debug("Skipping unsupported data type ID: %s at offset: %s",
                          data_type_id, offset)

        # go through the list of expected data type ID's, fill in None for missing data type ID's
        missing_data = EXPECTED_PARTICLE_IDS_SET.difference(retrieved_data_types)
        for data_type_id in missing_data:
            self.final_result.extend(self.encoding_func_dict[data_type_id][ENCODE_NULL])

        log.trace("FINAL RESULT: %s\n", self.final_result)

        return self.final_result

    def _parse_directional_spectrum(self, offset, rules):
        """
        Assumes first value to unpack contains the size of the array in the second value to unpack
        """
        # Unpack the unpacking rules
        (num_freq_name, num_dir_name, good_name, dat_name),\
        (num_freq_fmt, num_dir_fmt, good_fmt, dat_fmt) = zip(*rules)

        log.trace("ZIP:, %s %s %s %s",
                  num_freq_name, num_freq_fmt, num_dir_name, num_dir_fmt)

        # First unpack the array lengths and single length values
        (num_freq_data, num_dir_data, dspec_good_data) = struct.unpack_from(
            '<%s%s%s' % (num_freq_fmt, num_dir_fmt, good_fmt), self.raw_data, offset)

        # Then unpack the array using the retrieved lengths values
        next_offset = offset + FMT_SIZES[num_freq_fmt] + FMT_SIZES[num_dir_fmt] + FMT_SIZES[good_fmt]
        dspec_dat_list_data = struct.unpack_from(
            '<%s%s' % (num_freq_data * num_dir_data, dat_fmt), self.raw_data, next_offset)

        # convert to numpy array and reshape the data per IDD spec
        transformed_dat_data = numpy.array(dspec_dat_list_data).reshape(
            (num_freq_data, num_dir_data)).tolist()

        # Add to the collected parameter data
        self.final_result.extend(
            ({DataParticleKey.VALUE_ID: num_freq_name, DataParticleKey.VALUE: num_freq_data},
             {DataParticleKey.VALUE_ID: num_dir_name, DataParticleKey.VALUE: num_dir_data},
             {DataParticleKey.VALUE_ID: good_name, DataParticleKey.VALUE: dspec_good_data},
             {DataParticleKey.VALUE_ID: dat_name, DataParticleKey.VALUE: transformed_dat_data}))

    def _parse_hpr_time_series(self, offset, rules):

        position = offset
        temp_dict = {}
        for key, formatter, num_data_key in rules:
            if 'spare' in key:
                position += FMT_SIZES[formatter]
            elif num_data_key and num_data_key in temp_dict:
                length = HPR_TIME_SERIES_ARRAY_SIZE * temp_dict[num_data_key]
                value = struct.unpack_from('<%s%s' % (length, formatter), self.raw_data, position)

                # convert to numpy array and reshape the data to a 2d array
                np_array = numpy.array(value).reshape(
                    (temp_dict[num_data_key], HPR_TIME_SERIES_ARRAY_SIZE)).transpose().tolist()

                log.trace("TEST DATA: %s @ %s", np_array, position)

                self.final_result.extend(({DataParticleKey.VALUE_ID: key[HEADING_TIME_SERIES_IDX],
                                           DataParticleKey.VALUE: np_array[HEADING_TIME_SERIES_IDX]},
                                          {DataParticleKey.VALUE_ID: key[PITCH_TIME_SERIES_IDX],
                                           DataParticleKey.VALUE: np_array[PITCH_TIME_SERIES_IDX]},
                                          {DataParticleKey.VALUE_ID: key[ROLL_TIME_SERIES_IDX],
                                           DataParticleKey.VALUE: np_array[ROLL_TIME_SERIES_IDX]}))

            else:
                value, = struct.unpack_from('<%s' % formatter, self.raw_data, position)
                temp_dict.update({key: value})
                self.final_result.append({DataParticleKey.VALUE_ID: key,
                                          DataParticleKey.VALUE: value})
                log.trace("DATA: %s:%s @ %s", key, value, position)
                position += FMT_SIZES[formatter]

    def _parse_hpr_time_series_special(self, key, formatter, num_data, position):

        length = HPR_TIME_SERIES_ARRAY_SIZE * num_data
        value = struct.unpack_from('<%s%s' % (length, formatter), self.raw_data, position)

        # convert to numpy array and reshape the data to a 2d array
        np_array = numpy.array(value).reshape(
            (num_data, HPR_TIME_SERIES_ARRAY_SIZE)).transpose().tolist()

        log.trace("DATA: %s @ %s", np_array, position)

        self.final_result.extend(({DataParticleKey.VALUE_ID: key[HEADING_TIME_SERIES_IDX],
                                   DataParticleKey.VALUE: np_array[HEADING_TIME_SERIES_IDX]},
                                  {DataParticleKey.VALUE_ID: key[PITCH_TIME_SERIES_IDX],
                                   DataParticleKey.VALUE: np_array[PITCH_TIME_SERIES_IDX]},
                                  {DataParticleKey.VALUE_ID: key[ROLL_TIME_SERIES_IDX],
                                   DataParticleKey.VALUE: np_array[ROLL_TIME_SERIES_IDX]}))

    def _parse_hpr_time_series(self, offset, rules):
        """
        Assumes first value to unpack contains the size of the array in the second value to unpack
        """
        # Unpack the unpacking rules
        (hpr_num_name, beam_angle_name, spare_name, hpr_time_names),\
        (hpr_num_fmt, beam_angle_fmt, spare_fmt, hpr_time_fmt) = zip(*rules)

        log.trace("ZIP:, %s %s %s %s",
                  hpr_num_name, hpr_num_fmt, beam_angle_name, beam_angle_fmt)

        # First unpack the array length and single length value, no need to unpack spare
        (hpr_num_data, beam_angle_data) = struct.unpack_from(
            '<%s%s' % (hpr_num_fmt, beam_angle_fmt), self.raw_data, offset)

        # Then unpack the array using the retrieved lengths value
        next_offset = offset + FMT_SIZES[hpr_num_fmt] + FMT_SIZES[beam_angle_fmt] + FMT_SIZES[spare_fmt]
        hpr_time_list_data = struct.unpack_from(
            '<%s%s' % (hpr_num_data * HPR_TIME_SERIES_ARRAY_SIZE, hpr_time_fmt), self.raw_data, next_offset)

        # convert to numpy array and reshape the data to a 2d array per IDD spec
        transformed_hpr_time_data = numpy.array(hpr_time_list_data).reshape(
            (hpr_num_data, HPR_TIME_SERIES_ARRAY_SIZE)).transpose().tolist()

        # Add to the collected parameter data
        self.final_result.extend(
            ({DataParticleKey.VALUE_ID: hpr_num_name, DataParticleKey.VALUE: hpr_num_data},
             {DataParticleKey.VALUE_ID: beam_angle_name, DataParticleKey.VALUE: beam_angle_data},
             {DataParticleKey.VALUE_ID: hpr_time_names[HEADING_TIME_SERIES_IDX],
              DataParticleKey.VALUE: transformed_hpr_time_data[HEADING_TIME_SERIES_IDX]},
             {DataParticleKey.VALUE_ID: hpr_time_names[PITCH_TIME_SERIES_IDX],
              DataParticleKey.VALUE: transformed_hpr_time_data[PITCH_TIME_SERIES_IDX]},
             {DataParticleKey.VALUE_ID: hpr_time_names[ROLL_TIME_SERIES_IDX],
              DataParticleKey.VALUE: transformed_hpr_time_data[ROLL_TIME_SERIES_IDX]}))

    def _parse_values(self, offset, rules):
        position = offset

        for key, formatter in rules:
            if 'spare' in key:
                position += FMT_SIZES[formatter]
            else:
                value = list(struct.unpack_from('<%s' % formatter, self.raw_data, position))
                if len(value) == 1:
                    value = value[0]
                log.trace("DATA: %s:%s @ %s", key, value, position)
                position += FMT_SIZES[formatter]
                self.final_result.append({DataParticleKey.VALUE_ID: key,
                                          DataParticleKey.VALUE: value})

    def _parse_variable_leader(self, offset, rules):
        position = offset

        for key, formatter in rules:
            value = list(struct.unpack_from('<%s' % formatter, self.raw_data, position))
            if len(value) == 1:
                value = value[0]
            if 'start_time' in key:
                timestamp = ((value[0]*100 + value[1]), value[2], value[3], value[4],
                             value[5], value[6], value[7], 0, 0)
                log.trace("TIMESTAMP: %s", timestamp)
                elapsed_seconds = calendar.timegm(timestamp)
                self.set_internal_timestamp(unix_time=elapsed_seconds)
            log.trace("DATA: %s:%s @ %s", key, value, position)
            position += FMT_SIZES[formatter]
            self.final_result.append({DataParticleKey.VALUE_ID: key,
                                      DataParticleKey.VALUE: value})

    def _parse_values_with_array(self, offset, rules):
        """
        Assumes first value to unpack contains the size of the array for the second value to unpack
        """
        # Unpack the unpacking rules
        (param_size_name, param_list_name), (param_size_fmt, param_list_fmt) = zip(*rules)

        log.trace("ZIP:, %s %s %s %s",
                  param_size_name, param_size_fmt, param_list_name, param_list_fmt)

        # First unpack the array length value
        num_data, = struct.unpack_from('<%s' % param_size_fmt, self.raw_data, offset)

        # Then unpack the array using the retrieved length value, casting from a tuple to a list
        param_list_data = list(struct.unpack_from('<%s%s' % (num_data, param_list_fmt),
                                                  self.raw_data, offset + FMT_SIZES[param_size_fmt]))

        # Add to the collected parameter data
        self.final_result.extend(
            ({DataParticleKey.VALUE_ID: param_size_name, DataParticleKey.VALUE: num_data},
             {DataParticleKey.VALUE_ID: param_list_name, DataParticleKey.VALUE: param_list_data}))


class AdcptMWVSParser(BufferLoadingParser):
    """
    Parser for WVS data.
    """

    def __init__(self, config, stream_handle, *args, **kwargs):

        # self.input_file = stream_handle
        # Extract the sequence number & file time from the file name
        self.input_file_name = stream_handle.name
        self.particle_count = 0

        # Extract the sequence number & file time from the file name
        # match = FILE_NAME_MATCHER.match(stream_handle.name)
        #
        # if match:
        #     # store the sequence number & file time to put into the particle
        #     self._particle_class._sequence_number = match.group(AdcptMWVSParticleKey.SEQUENCE_NUMBER)
        #     self._particle_class._file_time = match.group(AdcptMWVSParticleKey.FILE_TIME)
        # else:
        #     self.recov_exception_callback(
        #         'Unable to extract file time or sequence number from WVS input file name: %s '
        #         % stream_handle.name)

        super(AdcptMWVSParser, self).__init__(
            config,
            stream_handle,
            None,
            self.sieve_function,
            *args, **kwargs)

    def sieve_function(self, input_buffer):
        """
        Sort through the input buffer looking for a data record.
        A data record is considered to be properly framed if there is a
        sync word and the appropriate size.
        Arguments:
          input_buffer - the contents of the input stream
        Returns:
          A list of start,end tuples
        """

        indices_list = []  # initialize the return list to empty

        # File is initially being read 1024 bytes at a time
        # Match a Header up to the "number of data types" value
        match = HEADER_MATCHER.search(input_buffer)
        log.trace("SEARCHING %x", self._stream_handle.tell())

        if match:
            record_start = match.start()
            record_size, = struct.unpack('I', match.group('Record_Size'))
            record_end = record_start + record_size
            num_data, = struct.unpack('B', match.group('NumDataTypes'))

            # if not EOF, check that the next position contains the "starting id" thing

            # Get a whole record based on meeting the required size
            if len(input_buffer) >= record_end:

                # check if next byte has the sync word or at end of file
                if len(input_buffer) == record_size or \
                   len(input_buffer) == (record_size + 2) and \
                   Header == struct.unpack_from('<H', input_buffer, record_size)[0]:

                    self.particle_count += 1
                    indices_list.append((record_start, record_end))
                    log.trace("FOUND RECORD #%s %s:%s ending at %x with %s data types",
                              self.particle_count,
                              record_start, record_end,
                              self._stream_handle.tell(), num_data)
            else:
                # go ahead and read in the rest of the record
                self.get_block(record_size-len(input_buffer)+2)

        return indices_list

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            self._exception_callback(UnexpectedDataException(
                "Found %d bytes of un-expected non-data %s" %
                (len(non_data), non_data)))

    def recov_exception_callback(self, message):
        log.warn(message)
        self._exception_callback(RecoverableSampleException(message))

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this parsing, plus the state.
        """

        # # Extract the sequence number & file time from the file name
        match = FILE_NAME_MATCHER.match(self._stream_handle.name)

        if match:
            # store the sequence number & file time to put into the particle
            self._particle_class._sequence_number = match.group(AdcptMWVSParticleKey.SEQUENCE_NUMBER)
            self._particle_class._file_time = match.group(AdcptMWVSParticleKey.FILE_TIME)
        else:
            self.recov_exception_callback(
                'Unable to extract file time or sequence number from WVS input file name: %s '
                % self.input_file_name)

        result_particles = []
        nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
        timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while chunk:

            particle = self._extract_sample(self._particle_class,
                                            None, chunk, None)

            log.trace('Parsed particle: %s\n\n' % particle.generate_dict())

            if particle is not None:
                result_particles.append((particle, None))

            nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
            timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles