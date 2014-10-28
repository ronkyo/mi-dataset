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

MODULE_NAME = 'mi.dataset.parser.adcpt_acfgm_dcl_pd8'
RECOVERED_PARTICLE_CLASS = 'adcpt_acfgm_pd8_dcl_instrument'
TELEMETERED_PARTICLE_CLASS = 'adcpt_acfgm_pd8_dcl_instrument_recovered'

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, \
    FLOAT_REGEX, UNSIGNED_INT_REGEX, INT_REGEX, SPACE_REGEX, ANY_CHARS_REGEX
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys

log = get_logger()

# Basic patterns
UINT = '(' + UNSIGNED_INT_REGEX + ')'  # unsigned integer as a group
SINT = '(' + INT_REGEX + ')'  # signed integer as a group
FLOAT = '(' + FLOAT_REGEX + ')'  # floating point as a captured group
MULTI_SPACE = SPACE_REGEX + '*'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
SENSOR_DATE = r'(\d{4}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY
SENSOR_TIME = r'(\d{2}:\d{2}:\d{2}.\d{2})'  # Sensor Time: HH:MM:SS.mm
HEX = r'[0-9a-fA-F]'
TWO_HEX = '(' + HEX + ')' + '(' + HEX + ')'

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
METADATA_PATTERN += START_METADATA  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by text
METADATA_PATTERN += END_METADATA  # followed by ']'
METADATA_PATTERN += ANY_CHARS_REGEX  # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX  # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

SENSOR_TIME_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_TIME_PATTERN += START_GROUP + SENSOR_DATE + MULTI_SPACE  # sensor date
SENSOR_TIME_PATTERN += SENSOR_TIME + END_GROUP + MULTI_SPACE  # sensor time
SENSOR_TIME_PATTERN += UINT + END_OF_LINE_REGEX  # Ensemble Number
SENSOR_TIME_MATCHER = re.compile(SENSOR_TIME_PATTERN)

SENSOR_HEAD_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_HEAD_PATTERN += 'Hdg:' + MULTI_SPACE + FLOAT + MULTI_SPACE  # Hdg
SENSOR_HEAD_PATTERN += 'Pitch:' + MULTI_SPACE + FLOAT + MULTI_SPACE  # Pitch
SENSOR_HEAD_PATTERN += 'Roll:' + MULTI_SPACE + FLOAT + END_OF_LINE_REGEX  # Roll
SENSOR_HEAD_MATCHER = re.compile(SENSOR_TIME_PATTERN)

SENSOR_TEMP_PATTERN = TIMESTAMP + MULTI_SPACE  # dcl controller timestamp
SENSOR_TEMP_PATTERN += 'Temp:' + MULTI_SPACE + FLOAT + MULTI_SPACE  # temp
SENSOR_TEMP_PATTERN += 'SoS:' + MULTI_SPACE + SINT + MULTI_SPACE  # SoS
SENSOR_TEMP_PATTERN += 'BIT:' + MULTI_SPACE + TWO_HEX + END_OF_LINE_REGEX  # sensor BIT
SENSOR_TEMP_MATCHER = re.compile(SENSOR_TEMP_PATTERN)

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
SENSOR_DATA_PATTERN += UINT + MULTI_SPACE  # Echo4
SENSOR_DATA_PATTERN += ANY_CHARS_REGEX  # Any data
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX  # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# Sieve function to read a chunck
PACKET_PATTERN = '('
PACKET_PATTERN += METADATA_PATTERN
PACKET_PATTERN += SENSOR_TIME_PATTERN
PACKET_PATTERN += SENSOR_HEAD_PATTERN
PACKET_PATTERN += SENSOR_TEMP_PATTERN
PACKET_PATTERN += ANY_CHARS_REGEX
PACKET_PATTERN += METADATA_PATTERN
PACKET_PATTERN += ')'
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

SENSOR_DATA_BIN = 7
SENSOR_DATA_DIR = 8
SENSOR_DATA_MAG = 9
SENSOR_DATA_EW = 10
SENSOR_DATA_NS = 11
SENSOR_DATA_VERT = 12
SENSOR_DATA_ERR = 13
SENSOR_DATA_ECHO1 = 14
SENSOR_DATA_ECHO2 = 15
SENSOR_DATA_ECHO3 = 16
SENSOR_DATA_ECHO4 = 17

SENSOR_TIME_TIMESTAMP = 0
SENSOR_TIME_YEAR = 1
SENSOR_TIME_MONTH = 2
SENSOR_TIME_DAY = 3
SENSOR_TIME_HOUR = 4
SENSOR_TIME_MINUTE = 5
SENSOR_TIME_SECOND = 6

SENSOR_TIME_SENSOR_DATE_TIME = 7
SENSOR_TIME_SENSOR_DATE = 8
SENSOR_TIME_SENSOR_TIME = 9
SENSOR_TIME_ENSEMBLE = 10

HEAD_TIMESTAMP = 0
HEAD_YEAR = 1
HEAD_MONTH = 2
HEAD_DAY = 3
HEAD_HOUR = 4
HEAD_MINUTE = 5
HEAD_SECOND = 6

HEAD_HEADING = 7
HEAD_PITCH = 8
HEAD_ROLL = 9

TEMP_TIMESTAMP = 0
TEMP_YEAR = 1
TEMP_MONTH = 2
TEMP_DAY = 3
TEMP_HOUR = 4
TEMP_MINUTE = 5
TEMP_SECOND = 6

TEMP_TEMP = 7
TEMP_SOS = 8
TEMP_HEX1 = 9
TEMP_HEX2 = 10

PD8_DATA_MAP = [
    ('dcl_controller_timestamp', 0, str),
    ('dcl_controller_starting_timestamp', 1, str),
    ('num_cells', 2, int),
    ('ensemble_number', 3, int),
    ('instrument_timestamp', 4, str),
    ('bit_result_demod_1', 5, int),
    ('bit_result_demod_0', 6, int),
    ('bit_result_timing', 7, int),
    ('speed_of_sound', 8, int),
    ('heading', 9, float),
    ('pitch', 10, float),
    ('roll', 11, float),
    ('temperature', 12, float),
    ('water_direction', 13, list),
    ('water_velocity', 14, list),
    ('water_velocity_east', 15, list),
    ('water_velocity_north', 16, list),
    ('water_velocity_up', 17, list),
    ('error_velocity', 18, list),
    ('echo_intensity_beam1', 19, list),
    ('echo_intensity_beam2', 20, list),
    ('echo_intensity_beam3', 21, list),
    ('echo_intensity_beam4', 22, list)
]


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
    _data_particle_type = DataParticleType.ADCP_PD8_TELEMETERED

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

        super(AdcpPd8Parser, self).__init__(None,
                                            METADATA_MATCHER,
                                            record_matcher=PACKET_MATCHER,
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
        self.handle_non_data(non_data, non_end, start)

        sensor_data_list = []
        sensor_temp_list = []
        sensor_head_list = []
        sensor_time_list = []
        end_time_list = []
        while chunk:
            # Read line by line
            lines = chunk.split('\n')
            for line in lines:
                for matcher in MATCHER_LIST:
                    line_match = matcher.match(line)
                    if line_match is not None:
                        if matcher is SENSOR_DATA_MATCHER:
                            sensor_data_list.append(line_match.groups())
                        elif matcher is SENSOR_TEMP_MATCHER:
                            sensor_temp_list.append(line_match.groups())
                        elif matcher is SENSOR_HEAD_MATCHER:
                            sensor_head_list.append(line_match.groups())
                        elif matcher is SENSOR_TIME_MATCHER:
                            sensor_time_list.append(line_match.groups())
                        elif matcher is IGNORE_EMPTY_MATCHER:
                            end_time_list.append(line_match.groups())
                        else:
                            pass  # ignore
                    else:
                        # If it's a valid metadata record, ignore it.
                        # Otherwise generate warning for unknown data.
                        meta_match = self.metadata_matcher.match(chunk)
                        if meta_match is None:
                            error_message = 'Unknown data found in chunk %s' % chunk
                            log.warn(error_message)
                            self._exception_callback(UnexpectedDataException(error_message))

            bin_lst = []
            dir_lst = []
            mag_lst = []
            ew_lst = []
            ns_lst = []
            vert_lst = []
            error_lst = []
            echo1_lst = []
            echo2_lst = []
            echo3_lst = []
            echo4_lst = []
            sensor_head_len = len(sensor_head_list)
            sensor_temp_len = len(sensor_temp_list)
            sensor_time_len = len(sensor_time_list)
            end_time_list_len = len(end_time_list)
            bit_string1 = sensor_temp_list[0][TEMP_HEX1]
            bit_string2 = sensor_temp_list[0][TEMP_HEX2]

            # There should be only one instance of the following record
            if sensor_head_len != 1 or sensor_temp_len != 1 or sensor_time_len != 1 or end_time_list_len != 1:
                error_message = 'Unknown data found in chunk %s' % chunk
                log.warn(error_message)
                self._exception_callback(UnexpectedDataException(error_message))

            for record in sensor_data_list:
                bin_lst.append(record[SENSOR_DATA_BIN])
                dir_lst.append(record[SENSOR_DATA_DIR])
                mag_lst.append(record[SENSOR_DATA_MAG])
                ew_lst.append(record[SENSOR_DATA_EW])
                ns_lst.append(record[SENSOR_DATA_NS])
                vert_lst.append(record[SENSOR_DATA_VERT])
                error_lst.append(record[SENSOR_DATA_ERR])
                echo1_lst.append(record[SENSOR_DATA_ECHO1])
                echo2_lst.append(record[SENSOR_DATA_ECHO2])
                echo3_lst.append(record[SENSOR_DATA_ECHO3])
                echo4_lst.append(record[SENSOR_DATA_ECHO4])

                result = [
                    end_time_list[0][0],  # ('dcl_controller_timestamp', 0, str),
                    sensor_time_list[0][0],  # ('dcl_controller_starting_timestamp', 1, str),
                    bin_lst[len(bin_lst) - 1],  # ('num_cells', 2, int),
                    sensor_time_list[0][SENSOR_TIME_ENSEMBLE],  # ('ensemble_number', 3, int),
                    sensor_time_list[0][SENSOR_TIME_SENSOR_DATE_TIME],  # ('instrument_timestamp', 4, str),

                    self.check_bit(bit_string1, 3),  # ('bit_result_demod_1', 5, int),
                    self.check_bit(bit_string2, 0),  # ('bit_result_demod_0', 6, int),
                    self.check_bit(bit_string2, 2),  # ('bit_result_timing', 7, int),
                    sensor_temp_list[0][TEMP_SOS],  # ('speed_of_sound', 8, int),
                    sensor_head_list[0][HEAD_HEADING],  # ('heading', 9, float),
                    sensor_head_list[0][HEAD_PITCH],  # ('pitch', 10, float),
                    sensor_head_list[0][HEAD_ROLL],  # ('roll', 11, float),
                    sensor_temp_list[0][TEMP_TEMP],  # ('temperature', 12, float),
                    dir_lst,  # ('water_direction', 13, list),
                    mag_lst,  # ('water_velocity', 14, list),
                    ew_lst,  # ('water_velocity_east', 15, list),
                    ns_lst,  # ('water_velocity_north', 16, list),
                    vert_lst,  # ('water_velocity_up', 17, list),
                    error_lst,  # ('error_velocity', 18, list),
                    echo1_lst,  # ('echo_intensity_beam1', 19, list),
                    echo2_lst,  # ('echo_intensity_beam2', 20, list),
                    echo3_lst,  # ('echo_intensity_beam3', 21, list),
                    echo4_lst  # ('echo_intensity_beam4', 22, list)
                ]

            if result is not None:
                particle = self._extract_sample(self._particle_class,
                                                None,
                                                result,
                                                None)
                if particle is not None:
                    result_particles.append((particle, None))

            nd_timestamp, non_data, non_start, non_end = self._chunker.get_next_non_data_with_index(clean=False)
            timestamp, chunk, start, end = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles