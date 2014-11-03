#!/usr/bin/env python

"""
@package mi.dataset.parser.adcpt_acfgm_dcl_pd8
@file marine-integrations/mi/dataset/parser/adcpt_acfgm_dcl_pd8.py
@author Sung Ahn
@brief Parser for the adcpt_acfgm_dcl_pd8 dataset driver

This file contains code for the adcpt_acfgm_dcl_pd8 parsers and code to produce data particles.
instrument and instrument recovered.

All records start with a timestamp.
Metadata records: timestamp [text] more text newline.
Sensor Data records: timestamp sensor_data newline.
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.

Release notes:

Initial Release
"""

__author__ = 'Sung Ahn'
__license__ = 'Apache 2.0'

import re
import numpy
from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, DclFileCommonParser, \
    TIMESTAMP, START_METADATA, END_METADATA, START_GROUP, END_GROUP, SENSOR_GROUP_TIMESTAMP

from mi.core.exceptions import UnexpectedDataException

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, UNSIGNED_INT_REGEX, INT_REGEX, SPACE_REGEX, ANY_CHARS_REGEX, ASCII_HEX_CHAR_REGEX

# Basic patterns
UINT = '(' + UNSIGNED_INT_REGEX + ')'  # unsigned integer as a group
SINT = '(' + INT_REGEX + ')'  # signed integer as a group
FLOAT = '(' + FLOAT_REGEX + ')'  # floating point as a captured group
MULTI_SPACE = SPACE_REGEX + '+'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
SENSOR_DATE = r'(\d{4}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2}.\d{2})'  # Sensor Time: HH:MM:SS.mm
TWO_HEX = '(' + ASCII_HEX_CHAR_REGEX + ')' + '(' + ASCII_HEX_CHAR_REGEX + ')'

# Line 1
# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Line 2
SENSOR_TIME_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_TIME_PATTERN += START_GROUP + SENSOR_DATE + MULTI_SPACE  # sensor date
SENSOR_TIME_PATTERN += SENSOR_TIME + END_GROUP + MULTI_SPACE  # sensor time
SENSOR_TIME_PATTERN += UINT + END_OF_LINE_REGEX  # Ensemble Number
SENSOR_TIME_MATCHER = re.compile(SENSOR_TIME_PATTERN)

# Line 3
SENSOR_HEAD_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_HEAD_PATTERN += 'Hdg:' + MULTI_SPACE + FLOAT + MULTI_SPACE  # Hdg
SENSOR_HEAD_PATTERN += 'Pitch:' + MULTI_SPACE + FLOAT + MULTI_SPACE  # Pitch
SENSOR_HEAD_PATTERN += 'Roll:' + MULTI_SPACE + FLOAT + END_OF_LINE_REGEX  # Roll
SENSOR_HEAD_MATCHER = re.compile(SENSOR_HEAD_PATTERN)

# Line 4
SENSOR_TEMP_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_TEMP_PATTERN += 'Temp:' + MULTI_SPACE + FLOAT + MULTI_SPACE  # temp
SENSOR_TEMP_PATTERN += 'SoS:' + MULTI_SPACE + SINT + MULTI_SPACE  # SoS
SENSOR_TEMP_PATTERN += 'BIT:' + MULTI_SPACE + TWO_HEX + END_OF_LINE_REGEX  # sensor BIT
SENSOR_TEMP_MATCHER = re.compile(SENSOR_TEMP_PATTERN)

# Line 5
IGNORE_HEAD_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
IGNORE_HEAD_PATTERN += 'Bin    Dir    Mag     E/W     N/S    Vert     Err   Echo1  Echo2  Echo3  Echo4'
IGNORE_HEAD_PATTERN += END_OF_LINE_REGEX
IGNORE_HEAD_MATCHER = re.compile(IGNORE_HEAD_PATTERN)

IGNORE_EMPTY_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
IGNORE_EMPTY_PATTERN += END_OF_LINE_REGEX
IGNORE_EMPTY_MATCHER = re.compile(IGNORE_EMPTY_PATTERN)

# Sensor data record:
SENSOR_DATA_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE   # bin
SENSOR_DATA_PATTERN += FLOAT + MULTI_SPACE  # Dir
SENSOR_DATA_PATTERN += FLOAT + MULTI_SPACE  # Mag
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE   # E/W
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE   # N/S
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE   # Vert
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE   # Err
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE   # Echo1
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE   # Echo2
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE   # Echo3
SENSOR_DATA_PATTERN += UINT  # Echo4
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX  # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# Sieve function to read a chunck
PACKET_PATTERN = "^" + METADATA_PATTERN
PACKET_PATTERN = SENSOR_TIME_PATTERN
PACKET_PATTERN += SENSOR_HEAD_PATTERN
PACKET_PATTERN += SENSOR_TEMP_PATTERN
PACKET_PATTERN += IGNORE_HEAD_PATTERN
PACKET_PATTERN += "(" + SENSOR_DATA_PATTERN + ")+"
PACKET_PATTERN += IGNORE_EMPTY_PATTERN
PACKET_PATTERN += METADATA_PATTERN
PACKET_MATCHER = re.compile(PACKET_PATTERN, re.DOTALL)

MATCHER_LIST = [SENSOR_TIME_MATCHER, SENSOR_HEAD_MATCHER, SENSOR_TEMP_MATCHER, IGNORE_HEAD_MATCHER,
                SENSOR_DATA_MATCHER, IGNORE_EMPTY_MATCHER]

MATCHER_MAP = [
    #
    (SENSOR_TIME_MATCHER,   1,      0),
    (SENSOR_HEAD_MATCHER,   2,      0),
    (SENSOR_TEMP_MATCHER,   3,      0),
    (IGNORE_HEAD_MATCHER,   4,      0),
    (SENSOR_DATA_MATCHER,   4,      5),
    (IGNORE_EMPTY_MATCHER,  0,      0)
]

SENSOR_DATA_BIN = 8
SENSOR_DATA_DIR = 9
SENSOR_DATA_MAG = 10
SENSOR_DATA_EW = 11
SENSOR_DATA_NS = 12
SENSOR_DATA_VERT = 13
SENSOR_DATA_ERR = 14
SENSOR_DATA_ECHO1 = 15
SENSOR_DATA_ECHO2 = 16
SENSOR_DATA_ECHO3 = 17
SENSOR_DATA_ECHO4 = 18

SENSOR_TIME_SENSOR_DATE_TIME = 8
SENSOR_TIME_SENSOR_DATE = 9
SENSOR_TIME_SENSOR_TIME = 10
SENSOR_TIME_ENSEMBLE = 11

HEAD_HEADING = 8
HEAD_PITCH = 9
HEAD_ROLL = 10

TEMP_TEMP = 8
TEMP_SOS = 9
TEMP_HEX1 = 10
TEMP_HEX2 = 11

PD8_DATA_MAP = [
    ('dcl_controller_timestamp', 0, str),
    ('dcl_controller_starting_timestamp', 8, str),
    ('ensemble_number', 9, int),
    ('instrument_timestamp', 10, str),

    ('heading', 11, float),
    ('pitch', 12, float),
    ('roll', 13, float),

    ('temperature', 14, float),
    ('speed_of_sound', 15, int),

    ('bit_result_demod_1', 16, int),
    ('bit_result_demod_0', 17, int),
    ('bit_result_timing', 18, int),

    ('num_cells', 19, int),
    ('water_direction', 20, lambda x: [float(y) for y in x]),
    ('water_velocity', 21, lambda x: [float(y) for y in x]),
    ('water_velocity_east', 22, lambda x: [int(y) for y in x]),
    ('water_velocity_north', 23, lambda x: [int(y) for y in x]),
    ('water_velocity_up', 24, lambda x: [int(y) for y in x]),
    ('error_velocity', 25, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam2', 27, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam1', 26, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam3', 28, lambda x: [int(y) for y in x]),
    ('echo_intensity_beam4', 29, lambda x: [int(y) for y in x])
]


class DataParticleType(BaseEnum):
    ADCP_PD8_TELEMETERED = 'adcpt_acfgm_pd8_dcl_instrument'
    ADCP_PD8_RECOVERED = 'adcpt_acfgm_pd8_dcl_instrument_recovered'


class AdcpPd8InstrumentDataParticle(DclInstrumentDataParticle):
    """
    Class for generating the adcpt_acfgm_dcl_pd8 instrument particle.
    """

    def __init__(self, raw_data, *args, **kwargs):
        super(AdcpPd8InstrumentDataParticle, self).__init__(
            raw_data, PD8_DATA_MAP, *args, **kwargs)


class AdcptPd8TelemeteredInstrumentDataParticle(AdcpPd8InstrumentDataParticle):
    """
    Class for generating Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.ADCP_PD8_TELEMETERED


class AdcptPd8ARecoveredInstrumentDataParticle(AdcpPd8InstrumentDataParticle):
    """
    Class for generating Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.ADCP_PD8_RECOVERED


class AdcpPd8Parser(DclFileCommonParser):
    """
    ADCPT PD8 Parser.
    """

    sensor_data_list = []
    # sensor_temp_list = []
    # sensor_head_list = []
    # sensor_time_list = []
    # end_time_list = []
    farsed_data = []

    def __init__(self, *args, **kwargs):

        super(AdcpPd8Parser, self).__init__(SENSOR_DATA_MATCHER,
                                            METADATA_MATCHER,
                                            *args, **kwargs)

    def check_bit(self, hex_value, index):

        """
        Check if the hex has the bit set
        index :From right to left zeroed index
        """
        value = 0
        if 10 > int(hex_value) >= 0:
            value = int(hex_value)
        elif hex_value == 'A' or hex == 'a':
            value = 10
        elif hex_value == 'B' or hex == 'b':
            value = 11
        elif hex_value == 'C' or hex == 'c':
            value = 12
        elif hex_value == 'D' or hex == 'd':
            value = 13
        elif hex_value == 'E' or hex == 'e':
            value = 14
        elif hex_value == 'F' or hex == 'f':
            value = 15
        else:
            error_message = 'Unknown data found BIT in chunk %s' % hex_value
            log.warn(error_message)
            self._exception_callback(UnexpectedDataException(error_message))

        binary = '{0:04b}'.format(value)

        if index > 3:
            error_message = 'Out of bound bit found BIT'
            log.warn(error_message)
            self._exception_callback(UnexpectedDataException(error_message))

        if binary.find('1', index, index + 1) > -1:
            return 1
        else:
            return 0

    def _load_particle_buffer(self):
        """
        Load up the internal record buffer with some particles based on a
        gather from the get_block method.
        """
        while self.get_block(size=4096):
            result = self.parse_chunks()
            self._record_buffer.extend(result)

    def read_chunk(self):
        nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
        timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        return chunk

    # Overwrite the one in dcl_file_common
    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state.
        """
        parsed_data = []
        result_particles = []


        index = 0
        MATCHER = 0
        NEXT = 1
        OTHER = 2

        chunk = self.read_chunk()
        while chunk:
        # loop is here
            # try matching a dcl log

            # else match using list
            # line_match = MATCHER_LIST[index].match(chunk)
            # if line_match is not None:
            #     # Move to the next matcher
            #     index = (index + 1) % len(MATCHER_LIST)
            #
            #     # Get the relevant data
            #     pass

            # line_match = MATCHER_MAP[index][MATCHER].match(chunk)
            # if line_match is not None:
            #     # Move to the next matcher
            #     index = MATCHER_MAP[index][NEXT].match(chunk)
            #
            #     # Get the relevant data
            # else:
            #     index = MATCHER_MAP[index][OTHER].match(chunk)

            for matcher in MATCHER_LIST:
                line_match = matcher.match(chunk)

                if line_match is not None:
                    break

            if line_match is not None:

                if matcher is SENSOR_TIME_MATCHER:                  # Line 1
                    self.farsed_data.append(line_match.groups()[SENSOR_GROUP_TIMESTAMP])
                    self.farsed_data.append(line_match.groups()[SENSOR_TIME_ENSEMBLE])
                    self.farsed_data.append(line_match.groups()[SENSOR_TIME_SENSOR_DATE_TIME])

                elif matcher is SENSOR_HEAD_MATCHER:                # Line 2
                    self.farsed_data.append(line_match.groups()[HEAD_HEADING])
                    self.farsed_data.append(line_match.groups()[HEAD_PITCH])
                    self.farsed_data.append(line_match.groups()[HEAD_ROLL])

                elif matcher is SENSOR_TEMP_MATCHER:                # Line 3
                    self.farsed_data.append(line_match.groups()[TEMP_TEMP])
                    self.farsed_data.append(line_match.groups()[TEMP_SOS])
                    self.farsed_data.append(self.check_bit(line_match.groups()[TEMP_HEX1], 3))
                    self.farsed_data.append(self.check_bit(line_match.groups()[TEMP_HEX2], 0))
                    self.farsed_data.append(self.check_bit(line_match.groups()[TEMP_HEX2], 2))

                elif matcher is SENSOR_DATA_MATCHER:                # Multi line
                    self.sensor_data_list.append(line_match.groups()[SENSOR_DATA_BIN:])

                elif matcher is IGNORE_EMPTY_MATCHER:               # Last Line
                    final_data = []
                    final_data.extend(line_match.groups())

                    self.farsed_data.append(self.sensor_data_list[-1][0])
                    tran_array = numpy.array(self.sensor_data_list)
                    self.farsed_data.extend(tran_array.transpose().tolist()[1:])

                    final_data.extend(self.farsed_data)

                    log.warn("TEST PRINT: %s", final_data)

                    if final_data is not None:  # is this possible?
                        particle = self._extract_sample(self._particle_class,
                                                        None,
                                                        final_data,
                                                        None)
                        if particle is not None:
                            # log.warn("PARTICLE: %s", particle.generate_dict())
                            result_particles.append((particle, None))

                    del self.farsed_data[:]
                    del self.sensor_data_list[:]

                else:
                    pass  # ignore
            else:
                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.
                meta_match = self.metadata_matcher.match(chunk)
                # if meta_match is not None:
                #     log.warn("MATCHER META:\n%s\n%s", self.metadata_matcher.pattern, chunk)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk [not meta] %s' % chunk
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))

            chunk = self.read_chunk()

        return result_particles