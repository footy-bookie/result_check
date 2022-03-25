import datetime
import logging
from string import Template

from flask import jsonify

from bookie_result_check.src.xgb_result_check import ResultCheck


def main(request):
    """
        Args:
            request: http request from cloud scheduler BODY
        Returns:
            none, but writes results to bq or mysql table
        """
    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template("Cloud Function was triggered on $time")
        logging.info(log_message.safe_substitute(time=current_time))

        print("Start with bookie_result ms")
        rc = ResultCheck()
        rc.possible_win()
        print("Done with bookie_result ms")
        return jsonify(status="success"), 200
    except Exception as error:
        log_message = Template("$error").substitute(error=error)
        logging.error(log_message)
        return jsonify(status="failure"), 200