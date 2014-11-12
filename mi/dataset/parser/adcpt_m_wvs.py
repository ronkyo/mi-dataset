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
from itertools import chain

from mi.dataset.parser import utilities

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
Header =                    '\x7f\x7a'
Fixed_Leader =              '\x00\x01'
Variable_Leader =           '\x00\x02'
Velocity_Time_Series =      '\x00\x03'
Amplitude_Time_Series =     '\x00\x04'
Surface_Time_Series =       '\x00\x05'
Pressure_Time_Series =      '\x00\x06'
Velocity_Spectrum =         '\x00\x07'
Surface_Track_Spectrum =    '\x00\x08'
Pressure_Spectrum =         '\x00\x09'
Directional_Spectrum =      '\x00\x0A'
Wave_Parameters =           '\x00\x0B'
Wave_Parameters2 =          '\x00\x0C'
Surface_Dir_Spectrum =              '\x00\x0D'
Heading_Pitch_Roll_Time_Series =    '\x00\x0E'
Bottom_Velocity_Time_Series =       '\x00\x0F'
Altitude_Time_Series =              '\x00\x10'

class AdcptMWVSParticleKey(BaseEnum):
    """
    Class that defines fields that need to be extracted from the data
    """
    FILE_TIME = "file_time"
    NUM_FIELDS = "num_fields"
    NUM_FREQ = "num_freq"
    FREQ_W_BAND = "freq_w_band"
    FREQ_0 = "freq_0"
    FREQ_BAND = "frequency_band"
    BANDWIDTH_BAND = "bandwidth_band"
    ENERGY_BAND = "energy_density_band"
    DIR_BAND = "direction_band"
    A1_BAND = "a1_band"
    B1_BAND = "b1_band"
    A2_BAND = "a2_band"
    B2_BAND = "b2_band"
    CHECK_BAND = "check_factor_band"

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
# HEADER_MATCHER = re.compile(r"""(?x)
#     %(START_METADATA)s %(ANY_CHARS_REGEX)s %(END_OF_LINE_REGEX)s
#     """ % common_matches, re.VERBOSE | re.DOTALL)


# \x7f\x7a <Spare1>{2} <record_size>{4} <Spares2-4>{3} <NumDataTypes> {1} <Offsets> {4x9}
HEADER_MATCHER = re.compile(r"""(?x)
    \x7f\x7a(?P<Spare1> (.{2})) (?P<Record_Size> (.{4})) (?P<Spare2_4> (.{3}))
    (?P<NumDataTypes> (.)) (?P<Offsets> (.{36}))
    %(ANY_CHARS_REGEX)s %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)


# Extract num_fields and num_freq from the following metadata line
# % 9 Fields and 64 Frequencies
DIR_FREQ_MATCHER = re.compile(r"""(?x)
    %(START_METADATA)s
    (?P<%(NUM_FIELDS)s> %(UINT)s) %(ONE_OR_MORE_WHITESPACE)s Fields\sand %(ONE_OR_MORE_WHITESPACE)s
    (?P<%(NUM_FREQ)s>   %(UINT)s) %(ONE_OR_MORE_WHITESPACE)s Frequencies %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# Extract freq_w_band and freq_0 from the following metadata line
# % Frequency Bands are 0.01562500 Hz wide(first frequency band is centered at 0.00830078)
FREQ_BAND_MATCHER = re.compile(r"""(?x)
    %(START_METADATA)s
    Frequency\sBands\sare\s (?P<%(FREQ_W_BAND)s> %(FLOAT)s)
    %(ONE_OR_MORE_WHITESPACE)s Hz\swide
    \(first\sfrequency\sband\sis\scentered\sat\s (?P<%(FREQ_0)s> %(FLOAT)s) \) %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)

# List of possible matchers for header data
HEADER_MATCHER_LIST = [DIR_FREQ_MATCHER, FREQ_BAND_MATCHER]

# Regex for identifying a single record of WVS data, ie:
#  0.008789 0.015625 0.003481 211.254501 -0.328733 -0.199515 -0.375233 0.062457 0.352941
FCOEFF_DATA_MATCHER = re.compile(r"""(?x)
    \s(?P<%(FREQ_BAND)s>      %(FLOAT)s)
    \s(?P<%(BANDWIDTH_BAND)s> %(FLOAT)s)
    \s(?P<%(ENERGY_BAND)s>    %(FLOAT)s)
    \s(?P<%(DIR_BAND)s>       %(FLOAT)s)
    \s(?P<%(A1_BAND)s>        %(FLOAT)s)
    \s(?P<%(B1_BAND)s>        %(FLOAT)s)
    \s(?P<%(A2_BAND)s>        %(FLOAT)s)
    \s(?P<%(B2_BAND)s>        %(FLOAT)s)
    \s(?P<%(CHECK_BAND)s>     %(FLOAT)s)
    %(END_OF_LINE_REGEX)s
    """ % common_matches, re.VERBOSE | re.DOTALL)


FCOEFF_ENCODING_RULES = [
    (AdcptMWVSParticleKey.FILE_TIME,         str),
    (AdcptMWVSParticleKey.NUM_FIELDS,        int),
    (AdcptMWVSParticleKey.NUM_FREQ,          int),
    (AdcptMWVSParticleKey.FREQ_W_BAND,       float),
    (AdcptMWVSParticleKey.FREQ_0,            float),
    (AdcptMWVSParticleKey.FREQ_BAND,         lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.BANDWIDTH_BAND,    lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.ENERGY_BAND,       lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.DIR_BAND,          lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.A1_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.B1_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.A2_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.B2_BAND,           lambda x: [float(y) for y in x]),
    (AdcptMWVSParticleKey.CHECK_BAND,        lambda x: [float(y) for y in x])
]


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m WVS recovered data
    """
    SAMPLE = 'adcpt_m_instrument_wvs_recovered'  # instrument data particle


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


class AdcptMWVSParser(SimpleParser):
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
        line = self._stream_handle.readline()

        while line:

            log.warn("TEST PRINT: %r", line)
            if HEADER_MATCHER.match(line):
                match = HEADER_MATCHER.match(line)
                log.warn("TEST REGEX: %s", match.groupdict())

                Record_Size = struct.unpack('I', match.group('Record_Size'))
                log.warn("TEST UNPACK: %s", Record_Size)

                Offsets = struct.unpack('9I', match.group('Offsets'))
                log.warn("TEST UNPACK: %s", Offsets)

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