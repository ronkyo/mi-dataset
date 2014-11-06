#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_fcoeff
@file marine-integrations/mi/dataset/parser/adcpt_m_fcoeff.py
@author Ronald Ronquillo
@brief Parser for the adcpt_m_fcoeff dataset driver

This file contains ...

__author__ = 'Ronald Ronquillo'

Release notes:

Initial Release
"""

__author__ = 'Ronald Ronquillo'
__license__ = 'Apache 2.0'


import calendar
import numpy
import re

from mi.core.exceptions import \
    SampleException, \
    RecoverableSampleException

from mi.dataset.dataset_parser import SimpleParser

from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum

from mi.dataset.parser.common_regexes import \
    UNSIGNED_INT_REGEX, \
    FLOAT_REGEX, \
    END_OF_LINE_REGEX, \
    SPACE_REGEX, \
    ONE_OR_MORE_WHITESPACE_REGEX, \
    ANY_CHARS_REGEX


# Basic patterns

# Regex for zero or more whitespaces
ZERO_OR_MORE_WHITESPACE_REGEX = r'\s*'

# regex for identifying an empty line
EMPTY_LINE_REGEX = END_OF_LINE_REGEX
EMPTY_LINE_MATCHER = re.compile(EMPTY_LINE_REGEX, re.DOTALL)

INT_GROUP_REGEX = r'(' + UNSIGNED_INT_REGEX + ')'
FLOAT_GROUP_REGEX = r'(' + FLOAT_REGEX + ')'

# regex for identifying start of a header line
START_METADATA = ZERO_OR_MORE_WHITESPACE_REGEX + '\%'

# Regex to extract the timestamp from the FCoeff log file path (path/to/FCoeffYYMMDDHHmm.txt)
FILE_NAME_REGEX = r'.+FCoeff([0-9]+)\.txt'
FILE_NAME_MATCHER = re.compile(FILE_NAME_REGEX, re.DOTALL)


# A regex used to match a date in the format YYMMDDHHmm
DATE_TIME_REGEX = r"""
(?P<year>       \d{2})
(?P<month>      \d{2})
(?P<day>        \d{2})
(?P<hour>       \d{2})
(?P<minute>     \d{2})"""

DATE_TIME_MATCHER = re.compile(DATE_TIME_REGEX, re.VERBOSE|re.DOTALL)


# Header data:
HEADER_PATTERN = START_METADATA    # Metadata starts with '%' or ' %'
HEADER_PATTERN += ANY_CHARS_REGEX         # followed by text
HEADER_PATTERN += END_OF_LINE_REGEX         # followed by newline
HEADER_MATCHER = re.compile(HEADER_PATTERN, re.DOTALL)

# Extract num_dir and num_freq from the following metadata line
# % <num_fields> Directions and <num_freq> Frequencies
DIR_FREQ_PATTERN = START_METADATA + ONE_OR_MORE_WHITESPACE_REGEX + INT_GROUP_REGEX + \
                   ONE_OR_MORE_WHITESPACE_REGEX + 'Fields and' + \
                   ONE_OR_MORE_WHITESPACE_REGEX + INT_GROUP_REGEX + \
                   ONE_OR_MORE_WHITESPACE_REGEX + 'Frequencies' + END_OF_LINE_REGEX

DIR_FREQ_MATCHER = re.compile(
    START_METADATA + ONE_OR_MORE_WHITESPACE_REGEX + INT_GROUP_REGEX +
    ONE_OR_MORE_WHITESPACE_REGEX + 'Fields and' +
    ONE_OR_MORE_WHITESPACE_REGEX + INT_GROUP_REGEX +
    ONE_OR_MORE_WHITESPACE_REGEX + 'Frequencies' + END_OF_LINE_REGEX
    , re.DOTALL)


# Extract freq_w_band and freq_0 from the following metadata line
# % Frequency Bands are <freq_w_band> Hz wide(first frequency band is centered at <freq_0>)
FREQ_BAND_PATTERN = START_METADATA + ONE_OR_MORE_WHITESPACE_REGEX + 'Frequency Bands are' + \
                    ONE_OR_MORE_WHITESPACE_REGEX + FLOAT_GROUP_REGEX + ONE_OR_MORE_WHITESPACE_REGEX + \
                    'Hz wide\(first frequency band is centered at' + ONE_OR_MORE_WHITESPACE_REGEX + \
                    FLOAT_GROUP_REGEX + '\)' + END_OF_LINE_REGEX

FREQ_BAND_MATCHER = re.compile(FREQ_BAND_PATTERN, re.DOTALL)


# List of possible matchers for header data
HEADER_MATCHER_LIST = [DIR_FREQ_MATCHER, FREQ_BAND_MATCHER]


# Regex for identifying a single record of FCoeff data
FCOEFF_DATA_REGEX = r'((' + SPACE_REGEX + FLOAT_REGEX + ')+)' + END_OF_LINE_REGEX
FCOEFF_DATA_MATCHER = re.compile(FCOEFF_DATA_REGEX, re.DOTALL)

FCOEFF_DATA_MAP = [
    ('file_time',           0,  str),
    ('num_fields',          1,  int),
    ('num_freq',            2,  int),
    ('freq_w_band',         3,  float),
    ('freq_0',              4,  float),
    ('frequency_band',      5,  lambda x: [float(y) for y in x]),
    ('bandwidth_band',      6,  lambda x: [float(y) for y in x]),
    ('energy_density_band', 7,  lambda x: [float(y) for y in x]),
    ('direction_band',      8,  lambda x: [float(y) for y in x]),
    ('a1_band',             9,  lambda x: [float(y) for y in x]),
    ('b1_band',             10, lambda x: [float(y) for y in x]),
    ('a2_band',             11, lambda x: [float(y) for y in x]),
    ('b2_band',             12, lambda x: [float(y) for y in x]),
    ('check_factor_band',   13, lambda x: [float(y) for y in x])
]

# Position of 'file_time' in FCOEFF_DATA_MAP
FILE_TIME_POSITION = 0


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m FCoeff recovered data
    """
    SAMPLE = 'adcpt_m_instrument_fcoeff_recovered'  # instrument data particle


# class AdcptMFCoeffDataParticleKey(BaseEnum):
#     """
#     Class that defines fields that need to be extracted from the data
#     """
#     FILE_TIME = "file_time"
#     NUM_DIR = "num_dir"
#     NUM_FREQ = "num_freq"
#     FREQ_W_BAND = "freq_w_band"
#     FREQ_0 = "freq_0"
#     START_DIR = "start_dir"
#     DIR_SURFACE_SPECTRUM = "directional_surface_spectrum"


class AdcptMFCoeffInstrumentDataParticle(DataParticle):
    """
    Class for generating the adcpt_m_instrument_fcoeff_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def __init__(self, raw_data, *args, **kwargs):

        super(AdcptMFCoeffInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

        # construct the timestamp from the file time
        file_time = self.raw_data[FILE_TIME_POSITION]

        match = DATE_TIME_MATCHER.match(file_time)

        if match:
            timestamp = (
                int(match.group('year')) + 2000,
                int(match.group('month')),
                int(match.group('day')),
                int(match.group('hour')),
                int(match.group('minute')),
                0.0, 0, 0, 0)

            elapsed_seconds = calendar.timegm(timestamp)
            self.set_internal_timestamp(unix_time=elapsed_seconds)
        else:
            raise SampleException("AdcptMFCoeffInstrumentDataParticle: Unable to construct \
                                  internal timestamp from file time: %s", file_time)

        self.instrument_particle_map = FCOEFF_DATA_MAP


    def _build_parsed_values(self):
        """
        Build parsed values for the Instrument Data Particle.
        """

        # Generate a particle by calling encode_value for each entry
        # in the Instrument Particle Mapping table,
        # where each entry is a tuple containing the particle field name,
        # an index into the match groups (which is what has been stored in raw_data),
        # and a function to use for data conversion.

        data_particle = [self._encode_value(name, self.raw_data[group], function)
                         for name, group, function in self.instrument_particle_map]

        #log.debug("Data particle: %s" % data_particle)

        return data_particle


class AdcptMFCoeffParser(SimpleParser):

    """
    Parser for adcpt_m FCoeff*.txt files.
    """

    def parse_file(self):
        """
        Parse the FCoeff*.txt file. Create a chunk from valid data in the file.
        Build a data particle from the chunk.
        """

        # Set default file time to January 1, 2000 - in case it cannot be parsed
        file_time = '0001010000'
        num_fields = 0
        num_freq = 0
        freq_w_band = 0.0
        freq_0 = 0.0

        fcoeff_matrix = []

        # Extract the file time from the file name
        input_file_name = self._stream_handle.name

        #log.debug("Input file name is %s" % input_file_name)

        match = FILE_NAME_MATCHER.match(input_file_name)

        if match:
            file_time = match.group(1)
        else:
            error_message = 'Unable to extract file time from FCoeff input file name: %s '\
                            % input_file_name
            log.warn(error_message)
            self._exception_callback(RecoverableSampleException(error_message))

        # read the first line in the file
        line = self._stream_handle.readline()

        while line:

            #log.debug('Read line: %s' % line)

            if EMPTY_LINE_MATCHER.match(line):
                # ignore blank lines, do nothing
                #log.debug('This was an empty line!')
                pass

            elif HEADER_MATCHER.match(line):
                # we need header records to extract useful information

                #log.debug('This was a header line!')

                for matcher in HEADER_MATCHER_LIST:
                    header_match = matcher.match(line)

                    if header_match is not None:

                        if matcher is DIR_FREQ_MATCHER:
                            num_fields = int(header_match.group(1))
                            num_freq = int(header_match.group(2))

                        elif matcher is FREQ_BAND_MATCHER:
                            freq_w_band = header_match.group(1)
                            freq_0 = header_match.group(2)

                        else:
                            #ignore
                            pass

            elif FCOEFF_DATA_MATCHER.match(line):

                #log.debug('This was a data line!')
                # Extract a row of the Directional Surface Spectrum matrix
                sensor_match = FCOEFF_DATA_MATCHER.match(line)
                data = sensor_match.group(1)
                # values = [float(x) for x in data.split()]
                values = data.split()

                num_values = len(values)

                # If the number of values in a line of data doesn't match num_dir,
                # Drop the record, throw a recoverable exception and continue parsing
                if num_values != num_fields:
                    error_message = 'Unexpected Number of fields in line: expected %s, got %s'\
                                    % (num_fields, num_values)
                    log.warn(error_message)
                    self._exception_callback(RecoverableSampleException(error_message))
                else:
                    # Add the row to the fcoeff matrix
                    fcoeff_matrix.append(values)

            else:
                # Generate a warning for unknown data
                error_message = 'Unexpected data found in line %s' % line
                log.warn(error_message)
                self._exception_callback(RecoverableSampleException(error_message))

            # read the next line in the file
            line = self._stream_handle.readline()

        # Check to see if the specified number of directions and frequencies were retrieved from the data

        fcoeff_matrix_length = len(fcoeff_matrix)
        if fcoeff_matrix_length != num_freq:
            error_message = 'Unexpected Number of frequencies in FCoeff Matrix: expected %s, got %s'\
                            % (num_freq, fcoeff_matrix_length)
            log.warn(error_message)
            self._exception_callback(RecoverableSampleException(error_message))

        # Construct the parsed data list to hand over to the Data Particle class for particle creation

        np_array = numpy.array(fcoeff_matrix)

        parsed_data = [
            file_time,      # ('file_time', 0, str),
            num_fields,     # ('num_dir', 1, int),
            num_freq,       # ('num_freq', 2, int),
            freq_w_band,    # ('freq_w_band', 3, float),
            freq_0          # ('freq_0', 4, float),
        ]

        parsed_data.extend(np_array.transpose().tolist())

        # log.debug('Parsed data: %s' % parsed_data)

        # Extract a particle and append it to the record buffer
        particle = self._extract_sample(AdcptMFCoeffInstrumentDataParticle, None, parsed_data, None)
        log.debug('Parsed particle: %s' % particle.generate_dict())
        self._record_buffer.append(particle)