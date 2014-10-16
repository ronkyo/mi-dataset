#!/usr/bin/env python

"""
@package mi.dataset.parser.pco2a_a_dcl
@file marine-integrations/mi/dataset/parser/pco2a_a_dcl.py
@author Sung Ahn
@brief Parser for the pco2a_a_dcl dataset driver

This file contains code for the pco2a_a_dcl parser and code to produce data particles.
For instrument telemetered data, there is one driver which produces two(air/water) types of data particle.
For instrument recover data, there is one driver which produces two(air/water) types of data particle.
The input files and the content of the data particles are the same for both
instrument telemetered and instrument recovered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 2 types of records.
Records are separated by a newline.
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
log = get_logger()

from mi.core.common import BaseEnum

from mi.dataset.parser.dcl_file_common import DclInstrumentDataParticle, \
    DclFileCommonParser, SENSOR_GROUP_TIMESTAMP, TIMESTAMP,\
    START_METADATA, END_METADATA, START_GROUP, END_GROUP

from mi.core.exceptions import NotImplementedException

from mi.dataset.parser.common_regexes import END_OF_LINE_REGEX, SPACE_REGEX, \
    FLOAT_REGEX, UNSIGNED_INT_REGEX, TIME_HR_MIN_SEC_REGEX, ANY_CHARS_REGEX

# Basic patterns
UINT = '('+UNSIGNED_INT_REGEX+')'   # unsigned integer as a group
FLOAT = '('+FLOAT_REGEX+')'         # floating point as a captured group
W_A_CHAR = r'([W|A])'
COMMA = ','
SHARP = '#'
CHAR_M = ' *M'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
SENSOR_DATE = r'(\d{4}/\d{2}/\d{2})'  # Sensor Date: MM/DD/YY

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + SPACE_REGEX      # dcl controller timestamp
METADATA_PATTERN += START_METADATA              # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS_REGEX             # followed by text
METADATA_PATTERN += END_METADATA                # followed by ']'
METADATA_PATTERN += ANY_CHARS_REGEX             # followed by more text
METADATA_PATTERN += END_OF_LINE_REGEX           # metadata record ends with LF
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# Sensor data record:
#   Timestamp Date<space>Time<space>SensorData
#   where SensorData are comma-separated unsigned integer numbers
SENSOR_DATA_PATTERN = TIMESTAMP + SPACE_REGEX  # dcl controller timestamp
SENSOR_DATA_PATTERN += SHARP + START_GROUP + SENSOR_DATE + SPACE_REGEX  # sensor date
SENSOR_DATA_PATTERN += TIME_HR_MIN_SEC_REGEX + END_GROUP + COMMA + CHAR_M + COMMA  # sensor time
SENSOR_DATA_PATTERN += UINT + COMMA         # measurement wavelength beta
SENSOR_DATA_PATTERN += UINT + COMMA         # raw signal beta
SENSOR_DATA_PATTERN += FLOAT + COMMA        # measurement wavelength chl
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal chl
SENSOR_DATA_PATTERN += FLOAT + COMMA        # measurement wavelength cdom
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal cdom
SENSOR_DATA_PATTERN += UINT + COMMA         # raw signal beta
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal cdom
SENSOR_DATA_PATTERN += FLOAT + COMMA        # raw signal cdom
SENSOR_DATA_PATTERN += W_A_CHAR             # raw internal temperature
SENSOR_DATA_PATTERN += END_OF_LINE_REGEX    # sensor data ends with CR-LF
SENSOR_DATA_MATCHER = re.compile(SENSOR_DATA_PATTERN)

# Manual test is below
# >>me = re.match(r"((\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})) #((\d{4}/\d{2}/\d{2})
#                (\d{2}):(\d{2}):(\d{2})), *M,(\d*),(\d*),(\d+.\d+),(\d+.\d+),(\d+.\d+),(\d+.\d+),(\d*),
#                (\d+.\d+),(\d+.\d+),(\D)",
#                "2014/08/10 00:20:24.274 #3765/07/27 01:00:11, M,43032,40423,397.04,40.1,21.221,
#                 28.480,1026,39.9,40.4,W")
# >>> me.group()
# '2014/08/10 00:20:24.274 #3765/07/27 01:00:11, M,43032,40423,397.04,40.1,21.221,28.480,1026,39.9,40.4,W'

# SENSOR_DATA_MATCHER produces the following groups.
# The following are indices into groups() produced by SENSOR_DATA_MATCHER.
# i.e, match.groups()[INDEX]
SENSOR_GROUP_SENSOR_DATE_TIME = 8
SENSOR_GROUP_SENSOR_DATE = 9
SENSOR_GROUP_SENSOR_HOUR = 10
SENSOR_GROUP_SENSOR_MINUTE = 11
SENSOR_GROUP_SENSOR_SECOND = 12
SENSOR_GROUP_ZERO_A2D = 13
SENSOR_GROUP_CURRENT_A2D = 14
SENSOR_GROUP_CO2 = 15
SENSOR_GROUP_AVG_IRGA_TEMP = 16
SENSOR_GROUP_HUMIDITY = 17
SENSOR_GROUP_HUMIDITY_TEMP = 18
SENSOR_GROUP_STREAM_PRESSURE = 19
SENSOR_GROUP_DETECTOR_TEMP = 20
SENSOR_GROUP_SOURCE_TEMP = 21
SENSOR_GROUP_SAMPLE_TYPE = 22

INSTRUMENT_PARTICLE_AIR_MAP = [
    ('dcl_controller_timestamp', SENSOR_GROUP_TIMESTAMP, str),
    ('date_time_string', SENSOR_GROUP_SENSOR_DATE_TIME, str),
    ('zero_a2d', SENSOR_GROUP_ZERO_A2D, int),
    ('current_a2d', SENSOR_GROUP_CURRENT_A2D, int),
    ('measured_air_co2', SENSOR_GROUP_CO2, float),
    ('avg_irga_temperature', SENSOR_GROUP_AVG_IRGA_TEMP, float),
    ('humidity', SENSOR_GROUP_HUMIDITY, float),
    ('humidity_temperature', SENSOR_GROUP_HUMIDITY_TEMP, float),
    ('gas_stream_pressure', SENSOR_GROUP_STREAM_PRESSURE, int),
    ('irga_detector_temperature', SENSOR_GROUP_DETECTOR_TEMP, float),
    ('irga_source_temperature', SENSOR_GROUP_SOURCE_TEMP, float)
]

INSTRUMENT_PARTICLE_WATER_MAP = [
    ('dcl_controller_timestamp', SENSOR_GROUP_TIMESTAMP, str),
    ('date_time_string', SENSOR_GROUP_SENSOR_DATE_TIME, str),
    ('zero_a2d', SENSOR_GROUP_ZERO_A2D, int),
    ('current_a2d', SENSOR_GROUP_CURRENT_A2D, int),
    ('measured_water_co2', SENSOR_GROUP_CO2, float),
    ('avg_irga_temperature', SENSOR_GROUP_AVG_IRGA_TEMP, float),
    ('humidity', SENSOR_GROUP_HUMIDITY, float),
    ('humidity_temperature', SENSOR_GROUP_HUMIDITY_TEMP, float),
    ('gas_stream_pressure', SENSOR_GROUP_STREAM_PRESSURE, int),
    ('irga_detector_temperature', SENSOR_GROUP_DETECTOR_TEMP, float),
    ('irga_source_temperature', SENSOR_GROUP_SOURCE_TEMP, float)
]


class DataParticleType(BaseEnum):
    PCO2A_INSTRUMENT_AIR_PARTICLE = 'pco2a_a_dcl_instrument_air'
    PCO2A_INSTRUMENT_WATER_PARTICLE = 'pco2a_a_dcl_instrument_water'
    PCO2A_INSTRUMENT_AIR_RECOVERED_PARTICLE = 'pco2a_a_dcl_instrument_air_recovered'
    PCO2A_INSTRUMENT_WATER_RECOVERED_PARTICLE = 'pco2a_a_dcl_instrument_water_recovered'


class Pco2aADclInstrumentDataParticle(DclInstrumentDataParticle):
    """
    Class for generating the Pco2a_a_dcl instrument particles.
    """

    # Dynamically set instrument_particle_map parameter when the
    # mi/dataset/dataset_parser.py:Parser:_extract_sample() method is called.
    particle_maps = {'A': INSTRUMENT_PARTICLE_AIR_MAP,
                     'W': INSTRUMENT_PARTICLE_WATER_MAP}

    def __init__(self, raw_data, *args, **kwargs):

        super(Pco2aADclInstrumentDataParticle, self).__init__(
            raw_data,
            self.particle_maps[raw_data[SENSOR_GROUP_SAMPLE_TYPE]],
            *args, **kwargs)

    def data_particle_type(self):
        """
        Overridden method from DataParticle Class in mi/core/instrument/data_particle.py
        Sets & return the data particle type (aka stream name)
        @raise: NotImplementedException if _data_particle_type is not set
        """

        self._data_particle_type = self._data_particle_dict[self.raw_data[SENSOR_GROUP_SAMPLE_TYPE]]

        if self._data_particle_type is None:
            raise NotImplementedException("_data_particle_type not initialized")

        return self._data_particle_type

class Pco2aADclTelemeteredInstrumentDataParticle(Pco2aADclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered air data.
    """

    # Let the DataParticle base class _data_particle_type variable default to None.
    # _data_particle_type will be dynamically set by the data_particle_type() accessor.
    _data_particle_dict = {
        'A': DataParticleType.PCO2A_INSTRUMENT_AIR_PARTICLE,
        'W': DataParticleType.PCO2A_INSTRUMENT_WATER_PARTICLE}


class Pco2aADclRecoveredInstrumentDataParticle(Pco2aADclInstrumentDataParticle):
    """
    Class for generating Offset Data Particles from Recovered air data.
    """

    # Let the DataParticle base class _data_particle_type variable default to None,
    # _data_particle_type will be dynamically set by the data_particle_type() accessor.
    _data_particle_dict = {
        'A': DataParticleType.PCO2A_INSTRUMENT_AIR_RECOVERED_PARTICLE,
        'W': DataParticleType.PCO2A_INSTRUMENT_WATER_RECOVERED_PARTICLE}


class Pco2aADclParser(DclFileCommonParser):
    """
    This is the entry point for the Metbk_a_dcl parser.
    """
    def __init__(self, *args, **kwargs):

        super(Pco2aADclParser, self).__init__(SENSOR_DATA_MATCHER,
                                             METADATA_MATCHER,
                                             *args, **kwargs)