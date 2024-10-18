from logging import Logger
from queue import Queue
import time
from datetime import datetime, timedelta
from typing import Dict

import streamlink
from websocket import _exceptions

from recordlivecams.classes import Record, Site, Streamer

# Object that signals shutdown
_sentinel = object()


def check_who_is_online(
    logger: Logger,
    start_recording_q: Queue,
    config: Dict,
    sites: Dict[str, Site],
    streamers: Dict[int, Streamer],
) -> None:
    """
    Goes through all streamers in the config looking for who is online.
    This is only run for sites in the config that do not have api_url, so Cam4 and MFC
    """

    # logger.debug("check_who_is_online started")

    sl = streamlink.Streamlink()
    plugin_count = len(sl.plugins.get_names())
    # logger.debug("Side loading Streamlink plugins")
    sl.plugins.load_path(config["streamlink_plugin_path"])
    plugin_count_new = len(sl.plugins.get_names())
    if plugin_count == plugin_count_new:
        logger.warning("No new plugins were loaded")

    last_week = datetime.now() - timedelta(weeks=7)
    last_month = datetime.now() - timedelta(days=30)

    for streamer in list(streamers.values()):
        if streamer.record == Record.NEVER or streamer.is_recording:
            continue

        # Should we skip this streamer because they haven't been online in a long
        # time and we've recently checked their online status?
        diff_in_sec = datetime.now() - streamer.last_checked_at
        if streamer.last_online > last_week:
            sec_to_sleep = (
                config["online_check_sec"]["in_last_week"] - diff_in_sec.total_seconds()
            )
        elif streamer.last_online > last_month:
            sec_to_sleep = (
                config["online_check_sec"]["in_last_month"]
                - diff_in_sec.total_seconds()
            )
        else:
            sec_to_sleep = (
                config["online_check_sec"]["over_month"] - diff_in_sec.total_seconds()
            )

        if sec_to_sleep > 0:
            continue

        streamer.last_checked_at = datetime.now()

        for site in streamer.sites:
            # Don't run this streamer by streamer check if the site has an API
            if "api_url" in config["sites"][site]:
                continue

            # Check if we need to pause for rate limiting when pinging a site
            diff_in_sec = datetime.now() - sites[site].last_checked_at
            sec_to_sleep = sites[site].rate_limit_sec - diff_in_sec.total_seconds()
            if sec_to_sleep > 0:
                time.sleep(sec_to_sleep)

            username = streamer.sites[site].username
            sites[site].last_checked_at = datetime.now()
            streams = {}
            try:
                url = sites[site].url.replace("{username}", username)
                streams = sl.streams(url)
            except streamlink.exceptions.PluginError as ex:
                rate_limit = "429 Client Error: Too Many Requests for url"
                if rate_limit in str(ex):
                    logger.info(f"{site}: {rate_limit}")
                else:
                    logger.debug(
                        f"Streamlink plugin error while checking is_online: {ex}"
                    )
            except _exceptions.WebSocketConnectionClosedException as ex:
                logger.info(
                    f"Websocket closed exception while checking: {username} at {site}"
                )
            except Exception:
                logger.exception(
                    f"check_who_is_online unexpected exception while checking: {username} at {site}"
                )

            if len(streams) > 1:
                streamer.last_online = datetime.now()
                start_recording_q.put((streamer.id, site))
                break

    # logger.debug("check_who_is_online completed")
