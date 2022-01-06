from sp_api.api import Reports
from sp_api.api import Feeds
from sp_api.base import SellingApiException
from sp_api.base.reportTypes import ReportType
from datetime import datetime, timedelta
import json, time


# report request
_jobList = [
    {
        "_jobId": 1,
        "_clientId": 65,
        "_marketplace": "IN",
        "_module": "reports",
        "_reportStartTime": "2021-12-15T00:00:00.000Z",
        "_reportEndTime": "2021-12-16T23:59:59.999Z",
        "_clientActiveMarketplaceIds": ["A21TJRUUN4KGV"],
        "_reportType": "GET_V2_SELLER_PERFORMANCE_REPORT",
        "_defaultApplicationAccount": "default",
    }
]

for _jobMessageObj in _jobList:
    # TODO: Validate if reportType is valid.
    # TODO: Validate if MarketPlace is valid.
    # TODO: Validate the time is isoFormat / add casting if only date is provided.
    if _jobMessageObj.get("_module") == "reports":
        try:
            # ~~~ Extra Initializations ~~~ #
            _jobMessageObj["_jobTrace"] = {}
            _jobStarttime = round(time.time() * 1000)
            _jobMessageObj["_jobTrace"]["_jobStarttime"] = _jobStarttime

            # *** Instantiate Class *** #
            _clsReports = Reports(
                _marketplace=_jobMessageObj.get("_marketplace"),
                account=_jobMessageObj.get("_defaultApplicationAccount"),
            )

            # ~~~~~~~~~~~~~ CREATE REPORT REQUEST ~~~~~~~~~~~~~ #
            # TODO: Handle rateLimit and burst count
            
            _cr = {}
            _resp = _clsReports.create_report(
                reportType=ReportType[_jobMessageObj.get("_reportType")],
                marketplaceIds=_jobMessageObj.get("_clientActiveMarketplaceIds"),
                dataStartTime=_jobMessageObj.get("_reportStartTime"),
                dataEndTime=_jobMessageObj.get("_reportEndTime"),
            )
            _cr["x-amzn-RequestId"] = _resp.headers.get("x-amzn-RequestId")
            _cr["x-amzn-RateLimit-Limit"] = _resp.headers.get("x-amzn-RateLimit-Limit")
            _cr["_reportId"] = str((_resp).reportId)
            _jobMessageObj["_jobTrace"]["_cr"] = _cr

            # ~~~~~~~~~~~~~ Report Status Check ~~~~~~~~~~~~~ #
            # TODO: Handle rateLimit and burst count
            
            _rs = {}
            _statusChecked = False
            _statusCheckCount = 0
            _rs['_statusChecked'] = False
            _rs['_statusCheckCount'] = 0
            if _cr.get("_reportId"):
                while _statusChecked is False and _statusCheckCount < 20:
                    _resp = _clsReports.get_report(reportId=_cr.get("_reportId"))
                    _rs["x-amzn-RequestId"] = _resp.headers.get("x-amzn-RequestId")
                    _rs["x-amzn-RateLimit-Limit"] = _resp.headers.get(
                        "x-amzn-RateLimit-Limit"
                    )
                    _rs["_processingStatus"] = _resp.processingStatus
                    _rs["_next_token"] = _resp.next_token
                    _rs["_pagination"] = _resp.pagination
                    _rs["_errors"] = _resp.errors

                    if _rs.get("_processingStatus") == "DONE":
                        _statusChecked = True
                        _rs["_reportDocumentId"] = _resp.reportDocumentId
                    elif _rs.get("_processingStatus") == "CANCELLED":
                        _statusChecked = True
                    elif _rs.get("_processingStatus") == "FATAL":
                        _statusChecked = True
                    else:
                        _statusChecked = False
                    
                    _statusCheckCount = _statusCheckCount + 1
                    _rs["_statusCheckCount"] = _statusCheckCount
                    print('Execution onStandby for 20 sec')
                    time.sleep(20)
            else:
                print('_reportID is Null')
            _jobMessageObj["_jobTrace"]["_rs"] = _rs

            # ~~~~~~~~~~~~~ Get Report URL ~~~~~~~~~~~~~ #
            # TODO: Handle rateLimit and burst count

            _gr = {}
            if _rs.get("_reportDocumentId"):
                _resp = _clsReports.get_report_document(reportDocumentId=_rs.get("_reportDocumentId"))
                _gr["x-amzn-RequestId"] = _resp.headers.get("x-amzn-RequestId")
                _gr["x-amzn-RateLimit-Limit"] = _resp.headers.get(
                    "x-amzn-RateLimit-Limit"
                )
                _gr["_next_token"] = _resp.next_token
                _gr["_pagination"] = _resp.pagination
                _gr["_errors"] = _resp.errors
                _gr["_compressionAlgorithm"] = _resp.compressionAlgorithm
                _gr["_reportDocumentId"] = _resp.reportDocumentId
                _gr["_url"] = _resp.url
            else:
                print('_reportDocumentId is Null')
            _jobMessageObj["_jobTrace"]["_gr"] = _gr

            # ~~~~~~~~~~~~~ Download & Parse Report ~~~~~~~~~~~~~ #
            # TODO: Handle rateLimit and burst count

            _dr = {}
            _jobMessageObj["_jobTrace"]["_dr"] = _dr
            # ~~~ Final Object Information to save ~~~ #
            _jobEndtime = round(time.time() * 1000)
            _jobMessageObj["_jobTrace"]["_jobEndtime"] = _jobEndtime
            print(json.dumps(_jobMessageObj))

        except Exception as e:
            print(str(e))

    else:
        print("Unknown Module Requested. ERR: 101")

        # # Get Report Status
        # r_ready = False
        # r_status = None
        # while r_ready is False and r_status is None:
        #     time.sleep(20)
        #     print('wait over')
        #     _resp = _clsReports.get_report(reportId=r_id)
        #     _processingStatus = _resp.processingStatus
        #     print('_processingStatus', _processingStatus)
        #     if _processingStatus == 'DONE':
        #         r_status = 'DONE'
        #         r_ready = True
        #         r_doc_id = _resp.reportDocumentId
        #         getReportData = _clsReports.get_report_document(reportDocumentId=r_doc_id)
        #         print(getReportData)
        #     else:
        #         r_status = None
        #         r_ready = False
