#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_m_dspec
@file marine-integrations/mi/dataset/parser/adcpt_m_dspec.py
@author Tapana Gupta
@brief Parser for the adcpt_m_dspec dataset driver

This file contains ...

__author__ = 'tgupta'

Release notes:

Initial Release
"""

__author__ = 'Tapana Gupta'
__license__ = 'Apache 2.0'


import calendar
import numpy
import re

from mi.core.exceptions import \
    SampleException, \
    RecoverableSampleException, \
    UnexpectedDataException

from mi.dataset.dataset_parser import SimpleParser

from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey

from mi.core.log import get_logger
log = get_logger()

from mi.core.common import BaseEnum

from mi.dataset.parser.common_regexes import \
    UNSIGNED_INT_REGEX, \
    INT_REGEX, \
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
INT_OR_FLOAT_GROUP_REGEX = r'(' + INT_REGEX + '|' + FLOAT_REGEX + ')'

# regex for identifying start of a header line
START_METADATA = ZERO_OR_MORE_WHITESPACE_REGEX + '\%'

# Regex to extract the timestamp from the DSpec log file path (path/to/DSpecYYMMDDHHmm.txt)
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



# Code alg
"""
While
    try matcher on line
    if fails
        if '# matches' == 'N' and "current_#_of_matches" > 1 (not first time using matcher)
            try next matcher, maybe store line & error
        else
            output error line & other pending errors?
    else
        call processing_function()
        if successful, extend parsed_data[] with output
        if '# matches' == 1
            move to next
        else '# matches' is > "current_#_of_matches" or  == 'N'
            same matcher

    # keep track/count of errors, don't publish in event of errors


    line = readline()

check tracked errors...

final_error_checking()

output particle()
"""
PARTICLE_MULTI_LINE_MAP = [
    # Matcher, # matches, Error message if match fails, extend_ouput f(raw_data)
    (DIR_FREQ_MATCHER,      1, "Error excepting blah"),
    (FREQ_BAND_MATCHER,     1, "Error excepting blah"),
    (FCOEFF_DATA_MATCHER,   'N', "Error excepting blah")

]


FCOEFF_DATA_MAP = [
    ('file_time',           0, str),
    ('num_fields',          1, int),
    ('num_freq',            2, int),
    ('freq_w_band',         3, float),
    ('freq_0',              4, float),
    ('frequency_band',      5, list),
    ('bandwidth_band',      6, list),
    ('energy_density_band', 7, list),
    ('direction_band',      8, list),
    ('a1_band',             9, list),
    ('b1_band',             10, list),
    ('a2_band',             11, list),
    ('b2_band',             12, list),
    ('check_factor_band',   13, list)
]

# Position of 'file_time' in DSPEC_DATA_MAP
FILE_TIME_POSITION = 0


class DataParticleType(BaseEnum):
    """
    Class that defines the data particles generated from the adcpt_m Dspec recovered data
    """
    SAMPLE = 'adcpt_m_instrument_fcoeff_recovered'  # instrument data particle


# class AdcptMDspecDataParticleKey(BaseEnum):
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


class AdcptMDspecInstrumentDataParticle(DataParticle):
    """
    Class for generating the adcpt_m_instrument_dspec_recovered data particle.
    """

    _data_particle_type = DataParticleType.SAMPLE

    def __init__(self, raw_data, *args, **kwargs):

        super(AdcptMDspecInstrumentDataParticle, self).__init__(raw_data, *args, **kwargs)

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
            raise SampleException("AdcptMDspecInstrumentDataParticle: Unable to construct \
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


class AdcptMDspecParser(SimpleParser):

    """
    Parser for adcpt_m DSpec*.txt files.
    """
    def __init__(self,
                 config,
                 stream_handle,
                 exception_callback):


        super(AdcptMDspecParser, self).__init__(config,
                                              stream_handle,
                                              exception_callback)

    def parse_file(self):
        """
        Parse the DSpec*.txt file. Create a chunk from valid data in the file.
        Build a data particle from the chunk.
        """

        # Set default file time to January 1, 2000 - in case it cannot be parsed
        file_time = '0001010000'
        num_fields = 0
        num_freq = 0
        freq_w_band = 0.0
        freq_0 = 0.0
        start_dir = 0.0

        dspec_matrix = []

        # Extract the file time from the file name
        input_file_name = self._stream_handle.name

        #log.debug("Input file name is %s" % input_file_name)

        match = FILE_NAME_MATCHER.match(input_file_name)

        if match:
            file_time = match.group(1)
        else:
            error_message = 'Unable to extract file time from DSpec input file name: %s '\
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
                values = [float(x) for x in data.split()]

                num_values = len(values)

                # If the number of values in a line of data doesn't match num_dir,
                # Drop the record, throw a recoverable exception and continue parsing
                if num_values != num_fields:
                    error_message = 'Unexpected Number of fields in line: expected %s, got %s'\
                                    % (num_fields, num_values)
                    log.warn(error_message)
                    self._exception_callback(RecoverableSampleException(error_message))
                else:
                    # Add the row to the dspec matrix
                    dspec_matrix.append(values)

            else:
                # Generate a warning for unknown data
                error_message = 'Unexpected data found in line %s' % line
                log.warn(error_message)
                self._exception_callback(RecoverableSampleException(error_message))

            # read the next line in the file
            line = self._stream_handle.readline()

        # Check to see if the specified number of directions and frequencies were retrieved from the data

        dspec_matrix_length = len(dspec_matrix)
        if dspec_matrix_length != num_freq:
            error_message = 'Unexpected Number of frequencies in FCoeff Matrix: expected %s, got %s'\
                            % (num_freq, dspec_matrix_length)
            log.warn(error_message)
            self._exception_callback(RecoverableSampleException(error_message))



        # Construct the parsed data list to hand over to the Data Particle class for particle creation
        parsed_data = [
            file_time,  # ('file_time', 0, str),
            num_fields,  # ('num_dir', 1, int),
            num_freq,  # ('num_freq', 2, int),
            freq_w_band,  # ('freq_w_band', 3, float),
            freq_0,  # ('freq_0', 4, float),
            dspec_matrix  # ('directional_surface_spectrum', 6, list)]
        ]

        #log.debug('Parsed data: %s' % parsed_data)

        # Extract a particle and append it to the record buffer
        particle = self._extract_sample(AdcptMDspecInstrumentDataParticle, None, parsed_data, None)
        self._record_buffer.append(particle)