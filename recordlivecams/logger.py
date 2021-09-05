import logging
from contextvars import ContextVar

WorkerName = ContextVar("worker_name")

logging.basicConfig(
    format="%(asctime)s %(worker_name)s %(levelname)s %(message)s",
    level=logging.Debug,
    datefmt="%y-%m-%d %H:%M:%S",
)


class WorkerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs.setdefault("extra", {})["worker_name"] = WorkerName.get()
        return msg, kwargs


def get_logger():
    return WorkerAdapter(logging.getLogger(__name__), None)
