#!/usr/bin/env python

# ##
# OOIPLACEHOLDER
#
# Copyright 2014 Raytheon Co.
##

__author__ = "Ronald Ronquillo"

from mi.core.log import get_logger
log = get_logger()

from mi.dataset.dataset_driver import DataSetDriver
from mi.dataset.parser.adcpt_acfgm_dcl_pd8 import AdcpPd8Parser


MODULE_NAME = 'mi.dataset.parser.adcpt_acfgm_dcl_pd8'
RECOVERED_PARTICLE_CLASS = 'AdcptPd8ARecoveredInstrumentDataParticle'
TELEMETERED_PARTICLE_CLASS = 'AdcptPd8TelemeteredInstrumentDataParticle'


class AdcptAcfgmDclPd8Driver:

    def __init__(self, sourceFilePath, particleDataHdlrObj, parser_config):
        
        self._sourceFilePath = sourceFilePath
        self._particleDataHdlrObj = particleDataHdlrObj
        self._parser_config = parser_config

    def process(self):
        
        with open(self._sourceFilePath, "r") as file_handle:

            def exception_callback(exception):
                log.debug("Exception: %s", exception)
                self._particleDataHdlrObj.setParticleDataCaptureFailure()

            parser = AdcpPd8Parser(self._parser_config,
                                   file_handle,
                                   lambda state, ingested: None,
                                   lambda data: None,
                                   exception_callback)

            driver = DataSetDriver(parser, self._particleDataHdlrObj)

            driver.processFileStream()

        return self._particleDataHdlrObj

