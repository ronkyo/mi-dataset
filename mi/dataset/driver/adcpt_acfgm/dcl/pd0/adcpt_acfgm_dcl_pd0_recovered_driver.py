#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Ronald Ronquillo"

import os

from mi.logging import config

from mi.dataset.driver.adcpt_acfgm.dcl.pd0.adcpt_acfgm_dcl_pd0_driver_common import AdcptAcfgmDclPd0Driver
from mi.dataset.dataset_parser import DataSetDriverConfigKeys


def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):

    config.add_configuration(os.path.join(basePythonCodePath, 'res', 'config', 'mi-logging.yml'))

    parser_config = {
        DataSetDriverConfigKeys.PARTICLE_MODULE:  'mi.dataset.parser.adcpt_acfgm_dcl_pd0',
        DataSetDriverConfigKeys.PARTICLE_CLASS: 'AdcptAcfgmPd0DclInstrumentRecoveredParticle',
    }

    driver = AdcptAcfgmDclPd0Driver(sourceFilePath, particleDataHdlrObj, parser_config)

    return driver.process()
