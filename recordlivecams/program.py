import logging
from pathlib import Path

from recordlivecams.monitor import Monitor
from recordlivecams.database import migrator


def main():
    # setup logger
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.DEBUG,
        datefmt="%y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()
    logger.info("Starting up")

    config_path = Path("/app/config")
    config_template_path = Path("/app/recordlivecams/config_template.yaml")
    video_path = Path("/app/download")  # no trailing slash
    completed_path = Path("/app/completed")  # no trailing slash

    migrator.migrate(logger, config_path / "db.sqlite3")

    # completed_path may not be mounted into Docker, in that case just use video_path as the default location
    if not completed_path.exists():
        logger.warning(f"completed_path not set, assigning as {video_path}")
        completed_path = video_path

    monitor = Monitor(
        logger, config_path, config_template_path, video_path, completed_path
    )
    monitor.run()

    logger.warning("Main loop finished")


if __name__ == "__main__":
    main()
