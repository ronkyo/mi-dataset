#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_wvs
@file marine-integrations/mi/dataset/parser/adcpt_m_wvs.py
@author Ronald Ronquillo
@brief Parser for the adcpt_m_wvs dataset driver

This file contains code for the adcpt_m_wvs parser and code to produce data particles

Fourier Coefficients data files (FCoeff*.txt) are space-delimited ASCII (with leading spaces).
FCoeff files contain leader rows containing English readable text.
Subsequent rows in WVS contain float data.
The file contains data for a single burst/record of pings.
Mal-formed sensor data records produce no particles.

The sensor data record has the following format:

% Fourier Coefficients
% <NumFields> Fields and <NumFreq> Frequencies
% Frequency(Hz), Band width(Hz), Energy density(m^2/Hz), Direction (deg), A1, B1, A2, B2, Check Factor
% Frequency Bands are <Fw_band> Hz wide(first frequency band is centered at <F0>)
<frequency_band[0]> <bandwidth_band[0]> <energy_density_band[0]> <direction_band[0]> <a1_band[0]> <b1_band[0]> <a2_band[0]> <b2_band[0]> <check_factor_band[0]>
<frequency_band[1]> <bandwidth_band[1]> <energy_density_band[1]> <direction_band[1]> <a1_band[1]> <b1_band[1]> <a2_band[1]> <b2_band[1]> <check_factor_band[1]>
...
<frequency_band[NumFreq]> <bandwidth_band[NumFreq]> <energy_density_band[NumFreq]> <direction_band[NumFreq]> <a1_band[NumFreq]> <b1_band[NumFreq]> <a2_band[NumFreq]> <b2_band[NumFreq]> <check_factor_band[NumFreq]>


Release notes:

Initial Release
"""

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


import ntplib
import re
import struct
import sys
from itertools import chain
from collections import namedtuple

from mi.dataset.parser import utilities
from functools import partial
from mi.core.instrument.chunker import StringChunker
from mi.dataset.dataset_parser import BufferLoadingParser, DataSetDriverConfigKeys

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.core.common import BaseEnum

from mi.core.instrument.data_particle import DataParticle

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.parser.common_regexes import \
    UNSIGNED_INT_REGEX, \
    FLOAT_REGEX, \
    END_OF_LINE_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    ANY_CHARS_REGEX


#Data Type IDs
# Header =                    '\x7f\x7a'
# Fixed_Leader =              '\x00\x01'
# Variable_Leader =           '\x00\x02'
# Velocity_Time_Series =      '\x00\x03'
# Amplitude_Time_Series =     '\x00\x04'
# Surface_Time_Series =       '\x00\x05'
# Pressure_Time_Series =      '\x00\x06'
# Velocity_Spectrum =         '\x00\x07'
# Surface_Track_Spectrum =    '\x00\x08'
# Pressure_Spectrum =         '\x00\x09'
# Directional_Spectrum =      '\x00\x0A'
# Wave_Parameters =           '\x00\x0B'
# Wave_Parameters2 =          '\x00\x0C'
# Surface_Dir_Spectrum =              '\x00\x0D'
# Heading_Pitch_Roll_Time_Series =    '\x00\x0E'
# Bottom_Velocity_Time_Series =       '\x00\x0F'
# Altitude_Time_Series =              '\x00\x10'

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

# Basic patterns
common_matches = {
    'FLOAT': FLOAT_REGEX,
    'UINT': UNSIGNED_INT_REGEX,
    'ANY_CHARS_REGEX': ANY_CHARS_REGEX,
    'ONE_OR_MORE_WHITESPACE': ONE_OR_MORE_WHITESPACE_REGEX,
    'START_METADATA': '\s*\%\s',            # regex for identifying start of a header line
    'END_OF_LINE_REGEX': END_OF_LINE_REGEX
}

common_matches.update(AdcptMWVSParticleKey.__dict__)

# regex for identifying an empty line
EMPTY_LINE_MATCHER = re.compile(END_OF_LINE_REGEX, re.DOTALL)

# WVS Filename timestamp format
TIMESTAMP_FORMAT = "%y%m%d%H%M"

# Regex to extract the timestamp from the WVS log file path (path/to/WVSYYMMDDHHmm.txt)
FILE_NAME_MATCHER = re.compile(r"""(?x)
    .+WVS(?P<%(FILE_TIME)s> %(UINT)s)\.txt
    """ % common_matches, re.VERBOSE | re.DOTALL)

# Header data:
# Metadata starts with '%' or ' %' followed by text &  newline, ie:
# % Fourier Coefficients
# % Frequency(Hz), Band width(Hz), Energy density(m^2/Hz), Direction (deg), A1, B1, A2, B2, Check Factor

# \x7f\x7a <Spare1>{2} <record_size>{4} <Spares2-4>{3} <NumDataTypes> {1} <Offsets> {4x9}
HEADER_MATCHER = re.compile(r"""(?x)
    \x7f\x7a(?P<Spare1> (.){2}) (?P<Record_Size> (.{4})) (?P<Spare2_4> (.){3}) (?P<NumDataTypes> (.))
    """ % common_matches, re.VERBOSE | re.DOTALL)





FCOEFF_ENCODING_RULES = [
    (AdcptMWVSParticleKey.FILE_MODE, 'B', int),  #, num_to_unpack, encoding_func)    # num_to_unpack points to another "name" or is None?
    (AdcptMWVSParticleKey.REC_TIME_SERIES, 'B', int),
    (AdcptMWVSParticleKey.REC_SPECTRA, 'B', int),
    (AdcptMWVSParticleKey.REC_DIR_SPEC, 'B', int),
    (AdcptMWVSParticleKey.SAMPLES_PER_BURST, 'H', int),
    (AdcptMWVSParticleKey.TIME_BETWEEN_SAMPLES, 'H', int),
    (AdcptMWVSParticleKey.TIME_BETWEEN_BURSTS_SEC, 'H', int),
    (AdcptMWVSParticleKey.BIN_SIZE, 'H', int),
    (AdcptMWVSParticleKey.BIN_1_MIDDLE, 'H', int),
    (AdcptMWVSParticleKey.NUM_RANGE_BINS, 'B', int),
    (AdcptMWVSParticleKey.NUM_VEL_BINS, 'B', int),
    (AdcptMWVSParticleKey.NUM_INT_BINS, 'B', int),
    (AdcptMWVSParticleKey.NUM_BEAMS, 'B', int),
    (AdcptMWVSParticleKey.BEAM_CONF, 'B', int),
    (AdcptMWVSParticleKey.WAVE_PARAM_SOURCE, 'B', int),
    (AdcptMWVSParticleKey.NFFT_SAMPLES, 'H', int),
    (AdcptMWVSParticleKey.NUM_DIRECTIONAL_SLICES, 'H', int),
    (AdcptMWVSParticleKey.NUM_FREQ_BINS, 'H', int),
    (AdcptMWVSParticleKey.WINDOW_TYPE, 'H', int),
    (AdcptMWVSParticleKey.USE_PRESS_4_DEPTH, 'B', int),
    (AdcptMWVSParticleKey.USE_STRACK_4_DEPTH, 'B', int),
    (AdcptMWVSParticleKey.STRACK_SPEC, 'B', int),
    (AdcptMWVSParticleKey.PRESS_SPEC, 'B', int)
]



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

    def _build_parsed_values(self):
        """
        Build parsed values for Recovered Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name, which is also
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        return [self._encode_value(name, self.raw_data[name], function)
                for name, function in FCOEFF_ENCODING_RULES]


class AdcptMWVSRecoveredInstrumentDataParticle(AdcptMWVSInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.SAMPLE


class AdcptMWVSParser(BufferLoadingParser):
    """
    Parser for dcl data.
    In addition to the standard constructor parameters,
    this constructor takes additional parameters sensor_data_matcher
    and metadata_matcher
    """

    particle_count = 0

    def __init__(self,
                 config,
                 stream_handle,
                 *args, **kwargs):

        # Default the position within the file to the beginning.
        self.input_file = stream_handle
        # record_matcher = kwargs.get('record_matcher', RECORD_MATCHER)

        # No fancy sieve function needed for this parser.
        # File is ASCII with records separated by newlines.
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
        sync word and the checksum matches.
        Arguments:
          input_buffer - the contents of the input stream
        Returns:
          A list of start,end tuples
        """

        #log.debug("sieve called with buffer of length %d", len(input_buffer))

        indices_list = []  # initialize the return list to empty

        # log.warn("TEST BUFF: %s", input_buffer)

        # File is being read 1024 bytes at a time
        # Match up to the "number of data types"
        first_match = HEADER_MATCHER.search(input_buffer)

        # NOTE: reassess this, take into account erroneous data causing shifts

        # wait till an entire header structure is found, including offsets
        if first_match:
            record_start = first_match.start()
            record_end = record_start + struct.unpack('I', first_match.group('Record_Size'))[0]
            num_data = struct.unpack('B', first_match.group('NumDataTypes'))[0]

            # Get a whole record
            if len(input_buffer) >= record_end:
                indices_list.append((record_start, record_end))
                log.warn("FOUND RECORD %s:%s at %s with %s data types", record_start, record_end,
                         self._stream_handle.tell(), num_data)

            # Get the header, then the parts? Needs control of the reading...



        # #find all occurrences of the record header sentinel
        # for match in HEADER_MATCHER.finditer(input_buffer):
        #
        #     record_start = match.start()
        #
        #     Record_Size = struct.unpack('I', match.group('Record_Size'))
        #
        #
        #     log.warn("sieve function found sentinel at byte  %d", record_start)
        #
        #     # num_bytes = struct.unpack("<H", input_buffer[record_start + 2: record_start + 4])[0]
        #     # get the number of bytes in the record, does not include the 2 checksum bytes
        #
        #     record_end = record_start + Record_Size[0]
        #
        #     #log.debug("sieve function number of bytes= %d , record end is %d", num_bytes, record_end)
        #
        #     #if there is enough in the buffer check the record
        #     # if record_end <= len(input_buffer):
        #     #     #make sure the checksum bytes are in the buffer too
        #     #
        #     #
        #     #     indices_list.append((record_start, record_end))
        #     #
        #     #     #log.debug("sieve function found record.  Start = %d End = %d", record_start, record_end)

        return indices_list

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            log.warn("Found %d bytes of un-expected non-data %s" %
                     (len(non_data), non_data))

            # self._exception_callback(UnexpectedDataException(
            #     "Found %d bytes of un-expected non-data %s" %
            #     (len(non_data), non_data)))

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state.
        """
        result_particles = []
        nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
        timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        # # If not set from config & no InstrumentParameterException error from constructor
        # if self.particle_classes is None:
        #     self.particle_classes = (self._particle_class,)

        while chunk:

            # log.warn("TEST PRINT: %r", ''.join([hex(ord(ch)) for ch in chunk]))
            self.particle_count += 1
            log.warn("TEST INDICES: %s:%s #%s", start, end, self.particle_count)

            Header = namedtuple('Header', 'id1 id2 spare0 spare1 record_size spare2 spare3 spare4 num_data_types')
            header_dict = {}
            header_dict = Header._asdict(Header._make(struct.unpack_from('<4bI3bB', chunk)))






            # get offsets % header_dict['num_data_types']

            offsets = struct.unpack_from('<%sI' % header_dict['num_data_types'], chunk, 12)

            log.warn("UNPACKED: %s\n%s", header_dict, offsets)

            # unpack IDs from those offsets

            for offset in offsets:
                data_type_id = struct.unpack_from('h', chunk, offset)
                log.warn("TEST PRINT: %r", ''.join([hex(ch) for ch in data_type_id]))


            # fmt = "".join([x[1] for x in FCOEFF_ENCODING_RULES]) # add the '<'?

            # Fixed_Leader = namedtuple('Fixed_Leader', " ".join([x[0] for x in FCOEFF_ENCODING_RULES]))
            # fixed_leader_dict = Fixed_Leader._asdict(Fixed_Leader._make(struct.unpack_from(
            #     fmt, chunk, offsets[0]+2)))
            #
            # log.warn("TEST1: %s: %s", fmt, fixed_leader_dict)

            fmt_sizes = {
                'H': 2,
                'B': 1
            }


            position = offsets[0] + 2   # +2 offset for the type
            fixed_leader_dict2 = {}
            for key, formatter, enc in FCOEFF_ENCODING_RULES:
                value = struct.unpack_from('<%s' % formatter, chunk, position)[0]
                fixed_leader_dict2.update({key: value})
                position += fmt_sizes[formatter]
                # put this into _build_parsed_values & call encode_value() directly to add to list!

            log.warn("TEST2: %s", fixed_leader_dict2)


            # if sensor_match is not None:
            #     particle = self._extract_sample(particle_class,
            #                                     None,
            #                                     sensor_match.groups(),
            #                                     None)
            #     if particle is not None:
            #         result_particles.append((particle, None))
            #
            # # It's not a sensor data record, see if it's a metadata record.
            # else:
            #
            #     # If it's a valid metadata record, ignore it.
            #     # Otherwise generate warning for unknown data.
            #     meta_match = self.metadata_matcher.match(chunk)
            #     if meta_match is None:
            #         error_message = 'Unknown data found in chunk %s' % chunk
            #         log.warn(error_message)
            #         self._exception_callback(UnexpectedDataException(error_message))

            nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
            timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles


class AdcptMWVSParser2(SimpleParser):
    """
    Parser for adcpt_m WVS*.txt files.
    """

    def recov_exception_callback(self, message):
        log.warn(message)
        self._exception_callback(RecoverableSampleException(message))

    def parse_file(self):
        """
        Parse the WVS*.txt file. Create a chunk from valid data in the file.
        Build a data particle from the chunk.
        """

        file_time_dict = {}
        dir_freq_dict = {}
        freq_band_dict = {}
        sensor_data_dict = {
            AdcptMWVSParticleKey.FREQ_BAND: [],
            AdcptMWVSParticleKey.BANDWIDTH_BAND: [],
            AdcptMWVSParticleKey.ENERGY_BAND: [],
            AdcptMWVSParticleKey.DIR_BAND: [],
            AdcptMWVSParticleKey.A1_BAND: [],
            AdcptMWVSParticleKey.B1_BAND: [],
            AdcptMWVSParticleKey.A2_BAND: [],
            AdcptMWVSParticleKey.B2_BAND: [],
            AdcptMWVSParticleKey.CHECK_BAND: []
        }

        # Extract the file time from the file name
        # input_file_name = self._stream_handle.name
        #
        # match = FILE_NAME_MATCHER.match(input_file_name)
        #
        # if match:
        #     file_time_dict = match.groupdict()
        # else:
        #     self.recov_exception_callback(
        #         'Unable to extract file time from WVS input file name: %s ' % input_file_name)

        # read the first line in the file
        # line = self._stream_handle.readline()
        line = self._stream_handle.read(38850)

        while line:

            log.warn("TEST PRINT: %r", ''.join([hex(ord(ch)) for ch in line]))

            if HEADER_MATCHER.match(line):
                match = HEADER_MATCHER.match(line)
                log.warn("TEST REGEX: %s", match.groupdict())

                # Record_Size = struct.unpack('I', match.group('Record_Size'))
                # log.warn("TEST UNPACK: %s", Record_Size)

                # Offsets = struct.unpack('9I', match.group('Offsets'))
                # log.warn("TEST UNPACK: %s", Offsets)
                #
                # for offset in Offsets:
                #     log.warn("Offset: %s %s", hex(ord(line[offset])), hex(ord(line[offset+1])))


            log.warn("FILE SIZE?: %s", sys.getsizeof(line))


            # if EMPTY_LINE_MATCHER.match(line):
            #     # ignore blank lines, do nothing
            #     pass
            #
            # elif HEADER_MATCHER.match(line):
            #     # we need header records to extract useful information
            #     for matcher in HEADER_MATCHER_LIST:
            #         header_match = matcher.match(line)
            #
            #         if header_match is not None:
            #
            #             if matcher is DIR_FREQ_MATCHER:
            #                 dir_freq_dict = header_match.groupdict()
            #
            #             elif matcher is FREQ_BAND_MATCHER:
            #                 freq_band_dict = header_match.groupdict()
            #
            #             else:
            #                 #ignore
            #                 pass
            #
            # elif FCOEFF_DATA_MATCHER.match(line):
            #     # Extract a row of data
            #     sensor_match = FCOEFF_DATA_MATCHER.match(line)
            #
            #     sensor_data_dict[AdcptMWVSParticleKey.FREQ_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.FREQ_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.BANDWIDTH_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.BANDWIDTH_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.ENERGY_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.ENERGY_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.DIR_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.DIR_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.A1_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.A1_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.B1_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.B1_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.A2_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.A2_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.B2_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.B2_BAND))
            #     sensor_data_dict[AdcptMWVSParticleKey.CHECK_BAND].append(
            #         sensor_match.group(AdcptMWVSParticleKey.CHECK_BAND))
            #
            # else:
            #     # Generate a warning for unknown data
            #     self.recov_exception_callback('Unexpected data found in line %s' % line)

            # read the next line in the file
            line = self._stream_handle.readline()

        # # Construct parsed data list to hand over to the Data Particle class for particle creation
        # # Make all the collected data effectively into one long dictionary
        # parsed_dict = dict(chain(file_time_dict.iteritems(),
        #                          dir_freq_dict.iteritems(),
        #                          freq_band_dict.iteritems(),
        #                          sensor_data_dict.iteritems()))
        #
        # error_flag = False
        # # Check if all parameter data is accounted for
        # for name in FCOEFF_ENCODING_RULES:
        #     try:
        #         log.trace("parsed_dict[%s]: %s", name[0], parsed_dict[name[0]])
        #     except KeyError:
        #         self.recov_exception_callback('Missing particle data: %s' % name[0])
        #         error_flag = True
        #
        # # Don't create a particle if data is missing
        # if error_flag:
        #     return
        #
        # # Check if the specified number of frequencies were retrieved from the data
        # wvs_data_length = len(sensor_data_dict[AdcptMWVSParticleKey.FREQ_BAND])
        # if wvs_data_length != int(dir_freq_dict[AdcptMWVSParticleKey.NUM_FREQ]):
        #     self.recov_exception_callback(
        #         'Unexpected number of frequencies in WVS Matrix: expected %s, got %s'
        #         % (dir_freq_dict[AdcptMWVSParticleKey.NUM_FREQ], wvs_data_length))
        #
        #     # Don't create a particle if data is missing
        #     return
        #
        # # Convert the filename timestamp into the particle timestamp
        # time_stamp = ntplib.system_to_ntp_time(
        #     utilities.formatted_timestamp_utc_time(file_time_dict[AdcptMWVSParticleKey.FILE_TIME]
        #                                            , TIMESTAMP_FORMAT))
        #
        # # Extract a particle and append it to the record buffer
        # particle = self._extract_sample(AdcptMWVSInstrumentDataParticle,
        #                                 None, parsed_dict, time_stamp)
        # log.trace('Parsed particle: %s' % particle.generate_dict())
        # self._record_buffer.append(particle)