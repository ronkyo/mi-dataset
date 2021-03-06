#!/usr/bin/env python

"""
@package mi.dataset.driver.dosta_abcdjm_ctdbp.dcl
@file mi-dataset/mi/dataset/driver/dosta_abcdjm_ctdbp/dcl_cp/dosta_abcdjm_ctdbp_dcl_telemetered_driver.py
@author Jeff Roy
@brief Driver for the dosta_abcdjm_ctdbp_dcl instrument (Telemetered Data)

Release notes:

Initial Release
"""

from mi.dataset.dataset_driver import SimpleDatasetDriver
from mi.dataset.parser.dosta_abcdjm_ctdbp_dcl import DostaAbcdjmCtdbpDclParser
from mi.core.versioning import version

MODULE_NAME = 'mi.dataset.parser.dosta_abcdjm_ctdb_dcl'


@version("15.6.0")
def parse(basePythonCodePath, sourceFilePath, particleDataHdlrObj):
    """
    This is the method called by Uframe
    :param basePythonCodePath This is the file system location of mi-dataset
    :param sourceFilePath This is the full path and filename of the file to be parsed
    :param particleDataHdlrObj Java Object to consume the output of the parser
    :return particleDataHdlrObj
    """

    with open(sourceFilePath, 'rU') as stream_handle:

        # create an instance of the concrete driver class defined below
        driver = DostaAbcdjmCtdbpDclTelemeteredDriver(basePythonCodePath, stream_handle, particleDataHdlrObj)
        driver.processFileStream()

    return particleDataHdlrObj


class DostaAbcdjmCtdbpDclTelemeteredDriver(SimpleDatasetDriver):
    """
    Derived dosta_abcdjm_ctdbp_dcl driver class
    All this needs to do is create a concrete _build_parser method
    """

    def _build_parser(self, stream_handle):

        # The parser inherits from simple parser - other callbacks not needed here
        parser = DostaAbcdjmCtdbpDclParser(True,
                                           stream_handle,
                                           self._exception_callback)

        return parser