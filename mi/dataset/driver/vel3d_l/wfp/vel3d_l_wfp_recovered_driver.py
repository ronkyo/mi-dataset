#!/usr/local/bin/python2.7
##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##
__author__ = 'Ronald Ronquillo'

import os
from mi.core.log import get_logger
log = get_logger()

from mi.logging import config
from mi.dataset.dataset_parser import DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.vel3d_l_wfp import Vel3dLWfpParser


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.vel3d_l_wfp',
        DataSetDriverConfigKeys.PARTICLE_CLASS: ['Vel3dLWfpMetadataRecoveredParticle',
                                                 'Vel3dLWfpInstrumentRecoveredParticle']
    }

    def exception_callback(exception):
        log.debug("ERROR: " + exception)
        particleDataHdlrObj.setParticleDataCaptureFailure()

    with open(sourceFilePath, 'rb') as stream_handle:
        parser = Vel3dLWfpParser(parser_config,
                                 None,
                                 stream_handle,
                                 lambda state, ingested: None,
                                 lambda data: None,
                                 exception_callback)

        driver = DataSetDriver(parser, particleDataHdlrObj)
        driver.processFileStream()


    return particleDataHdlrObj