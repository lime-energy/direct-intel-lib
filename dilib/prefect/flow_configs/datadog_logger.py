import logging
import sys

import prefect
from prefect import task
from prefect.utilities.logging import get_logger
from pythonjsonlogger import jsonlogger


class DatadogFormatter(jsonlogger.JsonFormatter):
    def __init__(self):
        super().__init__(timestamp=True)

    def add_fields(self, log_record, record, message_dict):
        super(DatadogFormatter, self).add_fields(log_record, record, message_dict)
        log_record["status"] = record.levelname.lower()
        log_record["logger"] = {"name": record.name}
        if record.exc_info:
            log_record["error"] = {
                "kind": record.exc_info[0].__name__,
                "stack": message_dict.get("stack_info"),
                "message": message_dict.get("exc_info"),
            }
            log_record.pop("exc_info", None)
            log_record.pop("stack_info", None)

        if log_record['message'].startswith('Flow run FAILED:'):
            log_record['status'] = 'failure'

        log_record['ddsource'] = 'prefect'
        log_record['ddtags'] = 'test:123,foo:bar'
        log_record['service'] = 'Datadog Test'
        log_record['host'] = 'eks'

@task()
def default_logger():
    handler = logging.StreamHandler()
    handler.setFormatter(DatadogFormatter())

    logger = get_logger()

    logger.addHandler(handler)
    logger.info(f'Beginning Flow run for \'{prefect.context.flow_name}\'')
    logger.info(f'Task \'{prefect.context.task_name}\': Starting task run...')


flow_config = dict(
    tasks=[default_logger]
)
