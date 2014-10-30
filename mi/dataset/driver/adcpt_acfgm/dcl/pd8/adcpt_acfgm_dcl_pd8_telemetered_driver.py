#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Jeff Roy"

import os

from mi.logging import config

from mi.dataset.driver.adcpt_acfgm.dcl.pd8.adcpt_acfgm_dcl_pd8_driver_common import AdcptAcfgmDclPd0Driver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE:  'mi.dataset.parser.adcpt_acfgm_dcl_pd8',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptPd8TelemeteredInstrumentDataParticle',
    }

    driver = AdcptAcfgmDclPd8Driver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
