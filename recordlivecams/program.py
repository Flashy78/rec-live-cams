import logging
from pathlib import Path

from recordlivecams.monitor import Monitor

config_path = Path("/app/config")
config_template_path = Path("/app/recordlivecams/config_template.yaml")
video_path = Path("/app/download")  # no trailing slash


def main():
    # setup logger
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.DEBUG,
        datefmt="%y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()
    logger.info("Starting up")

    monitor = Monitor(logger, config_path, config_template_path, video_path)
    monitor.run()

    logger.warning("Main loop finished")


if __name__ == "__main__":
    main()
