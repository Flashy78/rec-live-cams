from logging import log
import time
from datetime import datetime, timedelta
from typing import Any, Dict

import streamlink

# Object that signals shutdown
_sentinel = object()


def check_who_is_online(logger, start_recording_q, config, sites, streamers):
    """Goes through all streamers in the config looking for who is online"""

    # logger.debug("check_who_is_online started")

    sl = streamlink.Streamlink()
    plugin_count = len(sl.plugins)
    # logger.debug("Side loading Streamlink plugins")
    sl.load_plugins(config["streamlink_plugin_path"])
    plugin_count_new = len(sl.plugins)
    if plugin_count == plugin_count_new:
        logger.warning("No new plugins were loaded")

    last_week = datetime.now() - timedelta(weeks=7)
    last_month = datetime.now() - timedelta(days=30)

    for streamer in list(streamers.values()):
        if streamer.is_recording:
            continue

        # Should we skip this streamer because they haven't been online in a long
        # time and we've recently checked their online status?
        diff_in_sec = datetime.now() - streamer.last_checked_at
        if streamer.started_at > last_week:
            sec_to_sleep = (
                config["online_check_sec"]["in_last_week"] - diff_in_sec.total_seconds()
            )
        elif streamer.started_at > last_month:
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
            # Check if we need to pause for rate limiting when pinging a site
            diff_in_sec = datetime.now() - sites[site].last_checked_at
            sec_to_sleep = sites[site].rate_limit_sec - diff_in_sec.total_seconds()
            if sec_to_sleep > 0:
                time.sleep(sec_to_sleep)

            username = streamer.sites[site]
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
            except Exception:
                logger.exception("is_online exception")

            if len(streams) > 1:
                start_recording_q.put((streamer.name, site))
                break

    # logger.debug("check_who_is_online completed")
