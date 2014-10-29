#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_adcpt_acfgm
@fid marine-integrations/mi/dataset/parser/test/test_adcps_jln.py
@author Jeff Roy
@brief Test code for a Adcps_jln data parser
Parts of this test code were taken from test_adcpa.py
Due to the nature of the records in PD0 files, (large binary records with hundreds of parameters)
this code verifies select items in the parsed data particle
"""

from nose.plugins.attrib import attr
import os

from mi.core.log import get_logger
log = get_logger()

from mi.core.exceptions import UnexpectedDataException

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.parser.adcpt_acfgm_dcl_pd8 import AdcpPd8Parser, \
    MODULE_NAME, RECOVERED_PARTICLE_CLASS, TELEMETERED_PARTICLE_CLASS

from mi.dataset.test.test_parser import BASE_RESOURCE_PATH
RESOURCE_PATH = os.path.join(BASE_RESOURCE_PATH, 'adcpt_acfgm', 'dcl', 'pd8', 'resource')



@attr('UNIT', group='mi')
class AdcpPd8ParserUnitTestCase(ParserUnitTestCase):
    """
    Adcp_jln Parser unit test suite
    """

    def create_parser(self, particle_class, file_handle):
        """
        This function creates a MetbkADcl parser for recovered data.
        """
        parser = AdcpPd8Parser(
            {DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
             DataSetDriverConfigKeys.PARTICLE_CLASS: particle_class},
            file_handle,
            lambda state, ingested: None,
            self.publish_callback,
            self.exception_callback)
        return parser

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

    def test_parse_input(self):
        """
        Read a large file and verify that all expected particles can be read.
        Verification is not done at this time, but will be done in the
        tests below.
        """
        log.debug('===== START TEST BIG GIANT INPUT RECOVERED =====')
        in_file = self.open_file('20131201.adcp.log')
        parser = self.create_parser(RECOVERED_PARTICLE_CLASS, in_file)

        # In a single read, get all particles in this file.
        result = parser.get_records(5)
        self.assertEqual(len(result), 1)

        in_file.close()
        self.assertListEqual(self.exception_callback_value, [])

        log.debug('===== END TEST BIG GIANT INPUT =====')

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write('  - _index: %d\n' %(i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def create_yml(self):
        """
        This utility creates a yml file
        """

        #ADCP_data_20130702.PD0 has one record in it
        fid = open(os.path.join(RESOURCE_PATH, 'NE051400.PD0'), 'rb')

        self.stream_handle = fid
        self.parser = AdcpPd8Parser(self.config_recov, None, self.stream_handle,
                                    self.state_callback, self.publish_callback, self.exception_callback)

        particles = self.parser.get_records(250)

        self.particle_to_yml(particles, 'NE051400.yml')
        fid.close()

    def trim_file(self):
        """
        This utility routine can be used to trim large PD0 files down
        to a more manageable size.  It uses the sieve in the parser to
        create a copy of the file with a specified number of records
        """

        #define these values as needed

        input_file = 'ND072022.PD0'
        output_file = 'ND072022.PD0'
        num_rec = 3
        first_rec = 1
        log.info("opening file")
        infid = open(os.path.join(RESOURCE_PATH, input_file), 'rb')
        in_buffer = infid.read()
        log.info("file read")
        stream_handle = infid
        #parser needs a stream handle even though it won't use it
        parser = AdcpPd8Parser(self.config_recov, None, stream_handle,
                               self.state_callback, self.publish_callback, self.exception_callback)
        log.info("parser created, calling sieve")
        indices = parser.sieve_function(in_buffer)
        #get the start and ends of all the records
        log.info("sieve returned %d indeces", len(indices))
        if len(indices) < first_rec + num_rec:
            log.info('trim_file: not enough records in file no output created')
            return

        first_byte = indices[first_rec-1][0]
        last_byte = indices[first_rec-1 + num_rec-1][1]
        log.info('first byte is %d last_byte is %d', first_byte, last_byte)

        outfid = open(os.path.join(RESOURCE_PATH, output_file), 'wb')
        outfid.write(in_buffer[first_byte:last_byte])
        outfid.close()
        infid.close()

    def test_simple_recov(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        fid = open(os.path.join(RESOURCE_PATH, 'ND072022.PD0'), 'rb')

        self.stream_handle = fid
        self.parser = AdcpPd8Parser(self.config_recov, None, self.stream_handle,
                                    self.state_callback, self.publish_callback, self.exception_callback)

        particles = self.parser.get_records(1)

        log.debug('got back %d particles', len(particles))

        self.assert_particles(particles, 'ND072022_recov.yml', RESOURCE_PATH)

        fid.close()

    def test_simple_telem(self):
        """
        Read test data and pull out data particles one at a time.
        Assert that the results are those we expected.
        The contents of ADCP_data_20130702.000 are the expected results
        from the IDD.  These results for the that record were manually verified
        and are the entire parsed particle is represented in ADCP_data_20130702.yml
        """

        # ND072022.PD0 contains a single ADCPA ensemble
        fid = open(os.path.join(RESOURCE_PATH, 'ND072022.PD0'), 'rb')

        self.stream_handle = fid
        self.parser = AdcpPd8Parser(self.config_telem, None, self.stream_handle,
                                    self.state_callback, self.publish_callback, self.exception_callback)

        particles = self.parser.get_records(1)

        log.debug('got back %d particles', len(particles))

        self.assert_particles(particles, 'ND072022_telem.yml', RESOURCE_PATH)

        fid.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        #LA101636_20.PD0 has 20 records in it
        fid = open(os.path.join(RESOURCE_PATH, 'ND072023.PD0'), 'rb')

        self.stream_handle = fid
        self.parser = AdcpPd8Parser(self.config_recov, None, self.stream_handle,
                                    self.state_callback, self.publish_callback, self.exception_callback)

        particles = self.parser.get_records(54)
        log.info('got back %d records', len(particles))

        self.assert_particles(particles, 'ND072023_recov.yml', RESOURCE_PATH)


        fid.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        #LB180210_3_corrupted.PD0 has three records in it, the 2nd record was corrupted
        fid = open(os.path.join(RESOURCE_PATH, 'LB180210_3_corrupted.PD0'), 'rb')

        self.stream_handle = fid
        self.parser = AdcpPd8Parser(self.config_recov, None, self.stream_handle,
                                    self.state_callback, self.publish_callback, self.exception_callback)

        #try to get 3 particles, should only get 2 back
        #the second one should correspond to ensemble 3
        self.parser.get_records(3)

        log.debug('Exceptions : %s', self.exception_callback_value)

        self.assert_(isinstance(self.exception_callback_value[0], UnexpectedDataException))

        fid.close()
