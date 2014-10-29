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
from mi.core.log import get_logger

from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    DclFileCommonParser, TIMESTAMP, \
    START_METADATA, END_METADATA, START_GROUP, END_GROUP

from mi.core.exceptions import UnexpectedDataException

# Ron: This is from an older version, this moves to the driver
MODULE_NAME = 'mi.dataset.parser.adcpt_acfgm_dcl_pd8'
RECOVERED_PARTICLE_CLASS = 'AdcptPd8ARecoveredInstrumentDataParticle'
TELEMETERED_PARTICLE_CLASS = 'AdcptPd8TelemeteredInstrumentDataParticle'

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, UNSIGNED_INT_REGEX, INT_REGEX, SPACE_REGEX, ANY_CHARS_REGEX
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

log = get_logger()

# Basic patterns
UINT = '(' + UNSIGNED_INT_REGEX + ')'  # unsigned integer as a group
SINT = '(' + INT_REGEX + ')'  # signed integer as a group
FLOAT = '(' + FLOAT_REGEX + ')'  # floating point as a captured group
MULTI_SPACE = SPACE_REGEX + '+'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
SENSOR_DATE = r'(\d{4}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2}.\d{2})'  # Sensor Time: HH:MM:SS.mm
HEX = r'[0-9a-fA-F]'
TWO_HEX = '(' + HEX + ')' + '(' + HEX + ')'

# Line 1
# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ":?"  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

METADATA_PATTERN2 = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
METADATA_PATTERN2 += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN2 += ANY_CHARS_REGEX  # followed by text
METADATA_PATTERN2 += END_METADATA  # followed by ']'
METADATA_PATTERN2 += ANY_CHARS_REGEX  # followed by more text

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
IGNORE_HEAD_PATTERN += 'Bin' + ANY_CHARS_REGEX
IGNORE_HEAD_PATTERN += END_OF_LINE_REGEX
IGNORE_HEAD_MATCHER = re.compile(IGNORE_HEAD_PATTERN)

IGNORE_EMPTY_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
IGNORE_EMPTY_PATTERN += END_OF_LINE_REGEX
IGNORE_EMPTY_MATCHER = re.compile(IGNORE_EMPTY_PATTERN)

# Sensor data record:
SENSOR_DATA_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE  # bin
SENSOR_DATA_PATTERN += FLOAT + MULTI_SPACE  # Dir
SENSOR_DATA_PATTERN += FLOAT + MULTI_SPACE  # Mag
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE  # E/W
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE  # N/S
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE  # Vert
SENSOR_DATA_PATTERN += SINT + MULTI_SPACE  # Err
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE  # Echo1
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE  # Echo2
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE  # Echo3
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
# PACKET_PATTERN += ANY_CHARS_REGEX
PACKET_PATTERN += IGNORE_EMPTY_PATTERN
PACKET_PATTERN += METADATA_PATTERN
PACKET_MATCHER = re.compile(PACKET_PATTERN, re.DOTALL)

MATCHER_LIST = [SENSOR_DATA_MATCHER, SENSOR_TEMP_MATCHER, SENSOR_HEAD_MATCHER, SENSOR_TIME_MATCHER,
                IGNORE_HEAD_MATCHER, IGNORE_EMPTY_MATCHER]

SENSOR_DATA_TIMESTAMP = 0
SENSOR_DATA_YEAR = 1
SENSOR_DATA_MONTH = 2
SENSOR_DATA_DAY = 3
SENSOR_DATA_HOUR = 4
SENSOR_DATA_MINUTE = 5
SENSOR_DATA_SECOND = 6
SENSOR_DATA_MILLISECOND = 7

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

SENSOR_TIME_TIMESTAMP = 0
SENSOR_TIME_YEAR = 1
SENSOR_TIME_MONTH = 2
SENSOR_TIME_DAY = 3
SENSOR_TIME_HOUR = 4
SENSOR_TIME_MINUTE = 5
SENSOR_TIME_SECOND = 6
SENSOR_TIME_MILLISECOND = 7

SENSOR_TIME_SENSOR_DATE_TIME = 8
SENSOR_TIME_SENSOR_DATE = 9
SENSOR_TIME_SENSOR_TIME = 10
SENSOR_TIME_ENSEMBLE = 11

HEAD_TIMESTAMP = 0
HEAD_YEAR = 1
HEAD_MONTH = 2
HEAD_DAY = 3
HEAD_HOUR = 4
HEAD_MINUTE = 5
HEAD_SECOND = 6
HEAD_MILLISECOND = 7

HEAD_HEADING = 8
HEAD_PITCH = 9
HEAD_ROLL = 10

TEMP_TIMESTAMP = 0
TEMP_YEAR = 1
TEMP_MONTH = 2
TEMP_DAY = 3
TEMP_HOUR = 4
TEMP_MINUTE = 5
TEMP_SECOND = 6
TEMP_MILLISECOND = 7

TEMP_TEMP = 8
TEMP_SOS = 9
TEMP_HEX1 = 10
TEMP_HEX2 = 11

PD8_DATA_MAP = [
    ('dcl_controller_timestamp', 0, str),
    ('dcl_controller_starting_timestamp', 8, str),
    ('num_cells', 9, int),
    ('ensemble_number', 10, int),
    ('instrument_timestamp', 11, str),
    ('bit_result_demod_1', 12, int),
    ('bit_result_demod_0', 13, int),
    ('bit_result_timing', 14, int),
    ('speed_of_sound', 15, int),
    ('heading', 16, float),
    ('pitch', 17, float),
    ('roll', 18, float),
    ('temperature', 19, float),
    ('water_direction', 20, list),
    ('water_velocity', 21, list),
    ('water_velocity_east', 22, list),
    ('water_velocity_north', 23, list),
    ('water_velocity_up', 24, list),
    ('error_velocity', 25, list),
    ('echo_intensity_beam1', 26, list),
    ('echo_intensity_beam2', 27, list),
    ('echo_intensity_beam3', 28, list),
    ('echo_intensity_beam4', 29, list)
]

# Ron: This is from an older version, this moves to the driver
def process(source_file_path, particle_data_hdlr_obj, particle_class):
    with open(source_file_path, "r") as stream_handle:
        parser = AdcpPd8Parser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            stream_handle,
            lambda state, ingested: None,
            lambda data: log.trace("Found data: %s", data),
            lambda ex: particle_data_hdlr_obj.setParticleDataCaptureFailure()
        )
        driver = DataSetDriver(parser, particle_data_hdlr_obj)
        driver.processFileStream()


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

    def __init__(self, *args, **kwargs):

        super(AdcpPd8Parser, self).__init__(SENSOR_DATA_MATCHER,
                                            METADATA_MATCHER,
                                            # record_matcher=PACKET_MATCHER,
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

    # Overwrite the one in dcl_file_common
    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state.
        """
        result = []
        result_particles = []
        nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
        timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)

        # log.warn("PRINT ALL:\n%s\n%s %s\n%s\n%s %s", non_data, non_start, non_end, chunk, start, end)
        self.handle_non_data(non_data, non_end, start)

        sensor_data_list = []
        sensor_temp_list = []
        sensor_head_list = []
        sensor_time_list = []
        end_time_list = []

        # log.warn("CHUNK: %s, %s, %s", chunk, start, end)

        while chunk:

            # Read line by line
            # lines = chunk.splitlines(1)
            # for line in lines:

                # log.warn("LINE: %s", line)
            log.warn("PARSE CHUNK: %s", chunk)

            for matcher in MATCHER_LIST:
                line_match = matcher.match(chunk)

                if line_match is not None:
                    # log.warn("MATCHER:\n%s\n%s", matcher.pattern, chunk)
                    break

            if line_match is not None:
                if matcher is SENSOR_DATA_MATCHER:                  # Multi line
                    sensor_data_list.append(line_match.groups())
                    log.warn("CAPTURED: %s", line_match.groups())
                elif matcher is SENSOR_TEMP_MATCHER:                # Line 4
                    sensor_temp_list.append(line_match.groups())
                    log.warn("CAPTURED: %s", line_match.groups())
                    log.warn("PRINT sensor_temp_list: %s", sensor_temp_list)
                elif matcher is SENSOR_HEAD_MATCHER:                # Line 3
                    sensor_head_list.append(line_match.groups())
                    log.warn("CAPTURED: %s", line_match.groups())
                    log.warn("PRINT sensor_head_list: %s", sensor_head_list)
                elif matcher is SENSOR_TIME_MATCHER:                # Line 2
                    sensor_time_list.append(line_match.groups())
                    log.warn("CAPTURED: %s", line_match.groups())
                    log.warn("PRINT sensor_time_list: %s", sensor_time_list)
                elif matcher is IGNORE_EMPTY_MATCHER:
                    end_time_list.append(line_match.groups())
                    log.warn("CAPTURED: %s", line_match.groups())
                    log.warn("PRINT end_time_list: %s", end_time_list)
                    # Make a particle

                    log.warn("PRINT sensor_data_list: %s", sensor_data_list)

                    # bin_lst = []
                    # dir_lst = []
                    # mag_lst = []
                    # ew_lst = []
                    # ns_lst = []
                    # vert_lst = []
                    # error_lst = []
                    # echo1_lst = []
                    # echo2_lst = []
                    # echo3_lst = []
                    # echo4_lst = []
                    # sensor_head_len = len(sensor_head_list)
                    # sensor_temp_len = len(sensor_temp_list)
                    # sensor_time_len = len(sensor_time_list)
                    # end_time_list_len = len(end_time_list)
                    # # bit_string1 = sensor_temp_list[0][TEMP_HEX1]
                    # # bit_string2 = sensor_temp_list[0][TEMP_HEX2]
                    # bit_string1 = 4
                    # bit_string2 = 4
                    #
                    #
                    #
                    # # There should be only one instance of the following record
                    # if sensor_head_len != 1 or sensor_temp_len != 1 or sensor_time_len != 1 or end_time_list_len != 1:
                    #     error_message = 'Unknown data found in chunk [ERROR] %s %s %s %s %s' % (chunk, sensor_head_len, sensor_temp_len, sensor_time_len, end_time_list_len)
                    #     log.warn(error_message)
                    #     self._exception_callback(UnexpectedDataException(error_message))
                    #     continue
                    #
                    # for record in sensor_data_list:
                    #     bin_lst.append(record[SENSOR_DATA_BIN])
                    #     dir_lst.append(record[SENSOR_DATA_DIR])
                    #     mag_lst.append(record[SENSOR_DATA_MAG])
                    #     ew_lst.append(record[SENSOR_DATA_EW])
                    #     ns_lst.append(record[SENSOR_DATA_NS])
                    #     vert_lst.append(record[SENSOR_DATA_VERT])
                    #     error_lst.append(record[SENSOR_DATA_ERR])
                    #     echo1_lst.append(record[SENSOR_DATA_ECHO1])
                    #     echo2_lst.append(record[SENSOR_DATA_ECHO2])
                    #     echo3_lst.append(record[SENSOR_DATA_ECHO3])
                    #     echo4_lst.append(record[SENSOR_DATA_ECHO4])
                    #
                    # result = [
                    #     end_time_list[0][0],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][1],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][2],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][3],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][4],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][5],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][6],  # ('dcl_controller_timestamp', 0, str),
                    #     end_time_list[0][7],  # ('dcl_controller_timestamp', 0, str),
                    #     sensor_time_list[0][0],  # ('dcl_controller_starting_timestamp', 1, str),
                    #     bin_lst[len(bin_lst) - 1],  # ('num_cells', 2, int),
                    #     sensor_time_list[0][SENSOR_TIME_ENSEMBLE],  # ('ensemble_number', 3, int),
                    #     sensor_time_list[0][SENSOR_TIME_SENSOR_DATE_TIME],  # ('instrument_timestamp', 4, str),
                    #
                    #     self.check_bit(bit_string1, 3),  # ('bit_result_demod_1', 5, int),
                    #     self.check_bit(bit_string2, 0),  # ('bit_result_demod_0', 6, int),
                    #     self.check_bit(bit_string2, 2),  # ('bit_result_timing', 7, int),
                    #     sensor_temp_list[0][TEMP_SOS],  # ('speed_of_sound', 8, int),
                    #     sensor_head_list[0][HEAD_HEADING],  # ('heading', 9, float),
                    #     sensor_head_list[0][HEAD_PITCH],  # ('pitch', 10, float),
                    #     sensor_head_list[0][HEAD_ROLL],  # ('roll', 11, float),
                    #     sensor_temp_list[0][TEMP_TEMP],  # ('temperature', 12, float),
                    #     dir_lst,  # ('water_direction', 13, list),
                    #     mag_lst,  # ('water_velocity', 14, list),
                    #     ew_lst,  # ('water_velocity_east', 15, list),
                    #     ns_lst,  # ('water_velocity_north', 16, list),
                    #     vert_lst,  # ('water_velocity_up', 17, list),
                    #     error_lst,  # ('error_velocity', 18, list),
                    #     echo1_lst,  # ('echo_intensity_beam1', 19, list),
                    #     echo2_lst,  # ('echo_intensity_beam2', 20, list),
                    #     echo3_lst,  # ('echo_intensity_beam3', 21, list),
                    #     echo4_lst  # ('echo_intensity_beam4', 22, list)
                    # ]
                    #
                    #
                    # log.warn("TEST HERE: %s", result)
                    #
                    # if result is not None:
                    #     particle = self._extract_sample(self._particle_class,
                    #                                     None,
                    #                                     result,
                    #                                     None)
                    #     if particle is not None:
                    #         result_particles.append((particle, None))

                    del sensor_data_list[:]
                    del sensor_temp_list[:]
                    del sensor_head_list[:]
                    del sensor_time_list[:]
                    del end_time_list[:]

                else:
                    pass  # ignore
            else:
                # If it's a valid metadata record, ignore it.
                # Otherwise generate warning for unknown data.
                meta_match = self.metadata_matcher.match(chunk)     # Line 1
                # if meta_match is not None:
                    # log.warn("MATCHER META:\n%s\n%s", self.metadata_matcher.pattern, chunk)
                if meta_match is None:
                    error_message = 'Unknown data found in chunk [not meta] %s' % chunk
                    log.warn(error_message)
                    self._exception_callback(UnexpectedDataException(error_message))

            nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
            timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)



        return result_particles