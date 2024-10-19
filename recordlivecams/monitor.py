import json
import logging
import os
import re
import shutil
import signal
from sqlite3 import IntegrityError
import stat
import subprocess
import time
import multiprocessing as mp

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Dict, List
from urllib.parse import unquote, urlparse

import ffmpeg
import requests
import streamlink
import streamlink.options
import yaml
from deepdiff import DeepDiff

from recordlivecams.classes import CamStatus, Record, Site, Streamer, Streamer_Site
from recordlivecams.database.common import get_conn, run_sql, set_db_path
from recordlivecams.processes import check_who_is_online
from recordlivecams.face_detector import cvlib

_version = "1.0.0"


class Monitor:
    config = None
    processes = {}
    index_processes = {}
    thumb_processes = {}

    def __init__(
        self,
        logger: logging.Logger,
        config_path: Path,
        config_template_path: Path,
        video_path: Path,
        completed_path: Path,
    ):
        self.logger = logger
        self.logger.info(f"Version: {_version}")
        self.running = True
        self.shutting_down = False
        self.check_thread = None

        self.video_path_in_progress = video_path / "in_progress"
        self.video_path_in_progress.mkdir(parents=True, exist_ok=True)
        self.video_path_to_process = video_path / "to_process"
        self.video_path_to_process.mkdir(parents=True, exist_ok=True)
        self.completed_path = completed_path
        self.completed_path_prv = completed_path / "private"
        self.completed_path_prv.mkdir(parents=True, exist_ok=True)
        self.completed_path_failed = completed_path / "failed"
        self.completed_path_failed.mkdir(parents=True, exist_ok=True)
        self.streamer_thumb_path = (
            video_path / "streamer_thumbnails" / "first_seen_online"
        )
        self.streamer_thumb_path.mkdir(parents=True, exist_ok=True)
        self.streamer_new_thumb_path = video_path / "streamer_thumbnails" / "new"
        self.streamer_new_thumb_path.mkdir(parents=True, exist_ok=True)
        self.streamer_new_couple_thumb_path = (
            video_path / "streamer_thumbnails" / "new_couple"
        )
        self.streamer_new_couple_thumb_path.mkdir(parents=True, exist_ok=True)

        self.db_path = config_path / "db.sqlite3"
        set_db_path(self.db_path)

        self.config_timestamp = None
        self.config_path = config_path / "config.yaml"
        self.config_template_path = config_template_path
        self.video_path = video_path
        self.streamers: Dict[int, Streamer] = {}
        # Key is username-site_name
        self.username_by_site: Dict[str, int] = {}
        self.sites: Dict[str, Site] = {}
        self.currently_recording: List[int] = []
        self.orphan_file_list = []

        self.start_recording_q = Queue()

        # The order of these loads is important, should fix at some point
        self._load_streamers_from_db()
        self._reload_config(False)
        self._load_sites_from_db()

        # On startup, deal with any lingering files that need to be processed
        for file in self.video_path_in_progress.glob("*"):
            file.rename(self.video_path_to_process / file.name)

        threads = self.config.get("detect_faces_at_once", 2)
        if threads > 60 or threads > mp.cpu_count():
            self.logger.info(
                f"Face detection threads must be less than 61 or at most {mp.cpu_count()}"
            )
        self.mp_pool = mp.Pool(threads)

        self.streamlink = streamlink.Streamlink()
        plugin_count = len(self.streamlink.plugins.get_names())
        # self.logger.debug("Side loading Streamlink plugins")
        self.streamlink.plugins.load_path(self.config["streamlink_plugin_path"])
        plugin_count_new = len(self.streamlink.plugins.get_names())
        if plugin_count == plugin_count_new:
            self.logger.warning("No new plugins were loaded")

        # reg signals
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, signum, stack) -> None:
        if self.running:
            self.logger.info("Caught stop signal, stopping all recordings")
            self.running = False
            self.shutting_down = True
            self.mp_pool.close()

            for process in self.processes.values():
                # Send sigint, wait for end
                process["process"].send_signal(signal.SIGTERM)
                process["process"].wait()

            for process in self.index_processes.values():
                # Send sigint, wait for end
                process["process"].send_signal(signal.SIGTERM)
                process["process"].wait()

            for process in self.thumb_processes.values():
                # Send sigint, wait for end
                process["process"].send_signal(signal.SIGTERM)
                process["process"].wait()

    def _load_config(self) -> Dict:
        # opens file, returns json dict, returns None if error
        try:
            if not self.config_path.is_file():
                # We don't have an initial config, use the template to create one
                self.logger.info("Writing initial config from template")
                shutil.copy(self.config_template_path, self.config_path)

            modified_at = self.config_path.stat().st_mtime
            if self.config_timestamp == modified_at:
                return None
            else:
                self.config_timestamp = modified_at

            self.logger.debug(f"Loading config at: {self.config_path}")
            with open(self.config_path, "r") as f:
                return yaml.safe_load(f)

        except Exception as e:
            self.logger.exception("Error loading config")
            return None

    def _reload_config(self, check_if_new_streamers_online: bool = True) -> None:
        added = []
        updated = []
        removed = []
        try:
            conn = get_conn()
            c = conn.cursor()

            # Load the config and update streamers
            newConfig = self._load_config()
            if newConfig:
                # Let's figure out what streamers have been added/removed/changed
                affected_root_keys = []
                if not self.config:
                    # On initial load we can't run a diff, so just add everyone
                    affected_root_keys = newConfig["streamers"].keys()
                    self.config = {"streamers": {}}
                else:
                    diff = DeepDiff(self.config["streamers"], newConfig["streamers"])
                    affected_root_keys = diff.affected_root_keys

                # Need to go through all affected_root_keys and find out how they changed.
                for root_key in affected_root_keys:
                    if (
                        root_key in self.config["streamers"]
                        and root_key in newConfig["streamers"]
                    ):
                        updated.append(root_key)
                    elif root_key in self.config["streamers"]:
                        removed.append(root_key)
                    else:
                        added.append(root_key)

                # Removed
                for name in removed:
                    # Looks up their old dict entry by name and use one of the sites to get their model id
                    old = self.config["streamers"][name]
                    site = next(iter(old))
                    if site == "record":
                        site = next(iter(old))
                    site_name = self._get_full_site_name(site)
                    streamer_id = self.username_by_site[f"{old[site]}-{site_name}"]
                    if not streamer_id:
                        self.logger.warning(
                            f"Couldn't find an id for {name}-{site_name} during streamer removal"
                        )
                        continue

                    if self.streamers[streamer_id].record != Record.NEVER:
                        self.streamers[streamer_id].record = Record.NEVER
                        # Stop recording deleted streamers
                        if self.streamers[streamer_id].is_recording:
                            self._stop_recording_process(streamer_id)

                # Added
                for name in added:
                    # TODO: If they get a new username from a diff site in the config, how to add it?
                    # Maybe just update the new streamer.name field?

                    # Take the new site and look up model_id
                    new = newConfig["streamers"][name]
                    site = next(iter(new))
                    if site == "record":
                        site = next(iter(new))
                    site_name = self._get_full_site_name(site)
                    key = f"{new[site]}-{site_name}"
                    if key not in self.username_by_site:
                        # self.logger.warning(f"Couldn't find an id for {name}-{site_name} during streamer add")
                        # Ignore them for now until they are seen online
                        del newConfig["streamers"][name]
                        continue

                    streamer_id = self.username_by_site[key]
                    record = Record.ALWAYS

                    # Go through all sites assigned in the config and associate with this model id
                    # For now don't do anything with the orphaned streamer rows, they will get cleaned up in rewrite
                    self.streamers[streamer_id].name = name
                    del_from_dict = []
                    for site_name, streamer_name in new.items():
                        if site_name == "record":
                            if streamer_name == "priv":
                                record = Record.PRV
                            elif streamer_name == "ex_priv":
                                record = Record.EX_PRV
                            else:
                                record = Record.ALWAYS
                            continue

                        site_name = self._get_full_site_name(site_name)
                        key = f"{streamer_name}-{site_name}"
                        if key not in self.username_by_site:
                            del_from_dict.append(site)
                            continue

                        old_streamer_id = self.username_by_site[key]
                        self.username_by_site[key] = streamer_id

                        # Port the site over to the new streamer, updating streamer_id
                        self.streamers[streamer_id].sites[site_name] = replace(
                            self.streamers[old_streamer_id].sites[site_name]
                        )
                        self.streamers[streamer_id].sites[
                            site_name
                        ].streamer_id = streamer_id

                        try:
                            # Update the database record with the association
                            c.execute(
                                """
                                UPDATE streamer_sites
                                SET streamer_id = ?
                                WHERE name = ? AND site_name = ?;
                                """,
                                (streamer_id, streamer_name, site_name),
                            )
                            conn.commit()
                        except IntegrityError:
                            pass

                    for key in del_from_dict:
                        if key in newConfig["streamers"][name]:
                            del newConfig["streamers"][name][key]
                        elif (
                            self._get_full_site_name(key)
                            in newConfig["streamers"][name]
                        ):
                            del newConfig["streamers"][name][
                                self._get_full_site_name(key)
                            ]

                    self.streamers[streamer_id].record = record
                    if check_if_new_streamers_online:
                        self._start_recording(streamer_id)

                # Changed
                for name in updated:
                    new = newConfig["streamers"][name]
                    site = next(iter(new))
                    if site == "record":
                        site = next(iter(new))

                    site_name = self._get_full_site_name(site)
                    streamer_id = self.username_by_site[f"{new[site]}-{site_name}"]
                    record = Record.ALWAYS

                    # Go through any other sites assigned in the config and associate with this model id
                    # For now don't do anything with the orphaned streamer rows, they will get cleaned up in rewrite
                    self.streamers[streamer_id].name = name
                    del_from_dict = []
                    for site_name, streamer_name in new.items():
                        if site_name == "record":
                            # If we are limiting their recording to non-public, just let the current recording
                            # continue until it tries to restart. At that point it won't restart.
                            if streamer_name == "priv":
                                record = Record.PRV
                            elif streamer_name == "ex_priv":
                                record = Record.EX_PRV
                            else:
                                record = Record.ALWAYS
                            continue

                        # TODO: Site could have been removed from config
                        site_name = self._get_full_site_name(site_name)
                        key = f"{streamer_name}-{site_name}"
                        if key not in self.username_by_site:
                            del_from_dict.append(site)
                            continue

                        old_streamer_id = self.username_by_site[key]
                        self.username_by_site[key] = streamer_id

                        # Port the site over to the new streamer, updating streamer_id
                        self.streamers[streamer_id].sites[site_name] = replace(
                            self.streamers[old_streamer_id].sites[site_name]
                        )
                        self.streamers[streamer_id].sites[
                            site_name
                        ].streamer_id = streamer_id

                        # Update the database record with the association
                        c.execute(
                            """
                            UPDATE streamer_sites
                            SET streamer_id = ?
                            WHERE name = ? AND site_name = ?;
                            """,
                            (streamer_id, streamer_name, site_name),
                        )
                        conn.commit()

                    for key in del_from_dict:
                        if key in newConfig["streamers"][name]:
                            del newConfig["streamers"][name][key]
                        elif (
                            self._get_full_site_name(key)
                            in newConfig["streamers"][name]
                        ):
                            del newConfig["streamers"][name][
                                self._get_full_site_name(key)
                            ]

                    self.streamers[streamer_id].record = record
                    # Kick off recording because maybe a new site was added that they are active on
                    if not self.streamers[streamer_id].is_recording:
                        self._start_recording(streamer_id)

                self.config = newConfig
            else:
                return

            if added:
                self.logger.info(
                    f"Added: {(', ').join(sorted(added, key=str.casefold))}"
                )
            if updated:
                self.logger.info(
                    f"Updated: {(', ').join(sorted(updated, key=str.casefold))}"
                )
            if removed:
                self.logger.info(
                    f"Removed: {(', ').join(sorted(removed, key=str.casefold))}"
                )

            log_level = self.config["log_level"]
            self.logger.setLevel(log_level)
            if log_level == "DEBUG":
                # Don't log url requests for debug
                logging.getLogger("requests").setLevel(logging.WARNING)
                logging.getLogger("urllib3").setLevel(logging.WARNING)

        except Exception as e:
            self.logger.exception("Unable to load config")

    def _get_full_site_name(self, short_name: str) -> str:
        match short_name:
            case "bc":
                return "bongacams"
            case "c4":
                return "cam4"
            case "cb":
                return "chaturbate"
            case "ff":
                return "flirt4free"
            case "mfc":
                return "mfc"
            case "sc":
                return "stripchat"
            case _:
                return short_name

    def _get_short_site_name(self, full_name: str) -> str:
        match full_name:
            case "bongacams":
                return "bc"
            case "cam4":
                return "c4"
            case "chaturbate":
                return "cb"
            case "flirt4free":
                return "ff"
            case "mfc":
                return "mfc"
            case "stripchat":
                return "sc"
            case _:
                return full_name

    def _get_streamer_names_from_ids(self, ids: List[int]) -> List[str]:
        names = []
        for id in ids:
            names.append(self.streamers[id].name)

        return names

    def _list_who_is_recording(self) -> None:
        recording = []
        for id, streamer in self.streamers.items():
            if streamer.is_recording:
                recording.append(id)

        if recording and self.currently_recording != set(recording):
            self.currently_recording = set(recording)
            self.logger.info(
                f"Recording: {(', ').join(sorted(self._get_streamer_names_from_ids(recording), key=str.casefold))}"
            )

        if len(recording) != len(self.processes):
            self.logger.warn(
                f"Recording {len(recording)} streamers, but {len(self.processes)} threads running"
            )
            # Sometimes a streamer gets marked as being recorded, but the thread is cancelled and not caught
            for id in recording:
                if id not in self.processes:
                    self.logger.info(
                        f"Marking {self.streamers[id].name} as not being recorded"
                    )
                    self.streamers[id].is_recording = False

        index_procs = len(self.index_processes)
        thumb_procs = len(self.thumb_processes)
        if index_procs > 0 or thumb_procs > 0:
            self.logger.debug(
                f"Processing {index_procs} index and {thumb_procs} vcsi threads"
            )

    def _is_online(self, streamer_id: int) -> bool:
        """Returns true of the Streamlink plugin finds any public streams for the user
        TODO: Move this to processes.py
        """

        for site_name, site in self.streamers[streamer_id].sites.items():
            # Check if we need to pause for rate limiting when pinging a site
            diff_in_sec = datetime.now() - self.sites[site_name].last_checked_at
            sec_to_sleep = (
                self.sites[site_name].rate_limit_sec - diff_in_sec.total_seconds()
            )
            if sec_to_sleep > 0:
                time.sleep(sec_to_sleep)

            username = self.streamers[streamer_id].sites[site_name].username
            self.sites[site_name].last_checked_at = datetime.now()
            self.streamers[streamer_id].last_checked_at = datetime.now()
            streams = {}
            try:
                url = self.sites[site_name].url.replace("{username}", username)
                options = {}
                if site_name == "flirt4free":
                    options = {
                        "model_id": str(
                            self.streamers[streamer_id].sites[site].external_id
                        )
                    }

                streams = self.streamlink.streams(url, options)
            except streamlink.exceptions.PluginError as ex:
                self.logger.debug(
                    f"Streamlink plugin error while checking is_online: {ex}"
                )
            except Exception:
                self.logger.exception("is_online exception")

            if len(streams) > 1:
                self.streamers[streamer_id].sites[site_name].cam_status = CamStatus.PUB
                return True
        return False

    def _run_check_online_thread(self) -> None:
        if not self.check_thread or not self.check_thread.is_alive():
            # Loop through all streamers to see if they are online
            self.check_thread = Thread(
                target=check_who_is_online,
                args=(
                    self.logger,
                    self.start_recording_q,
                    self.config,
                    self.sites,
                    self.streamers,
                ),
            )
            self.check_thread.start()

    def _start_recording(self, streamer_id: int, desc: str = "") -> None:
        if self.streamers[streamer_id].is_recording or not self._should_be_recording(
            streamer_id
        ):
            return

        # Find a public stream
        name = self.streamers[streamer_id].name
        site = ""
        only_private_avail = False
        for site_name, item in self.streamers[streamer_id].sites.items():
            if item.last_checked_at < datetime.now() - timedelta(minutes=5):
                # This status is old, ignore it since we can't get an update from the API calls
                continue
            elif item.cam_status == CamStatus.PUB:
                only_private_avail = False
                site = site_name
                break
            elif item.cam_status in [
                CamStatus.EX_PRV,
                CamStatus.PRV,
                CamStatus.SHOW,
            ]:
                only_private_avail = True

        if not site:
            if only_private_avail:
                self.logger.info(f"Only private streams available for {name}")
            else:
                self.logger.info(f"No streams available for {name}")
            return

        self.logger.info(f"Starting to record {name} on {site}")
        self.streamers[streamer_id].is_recording = True
        self.streamers[streamer_id].started_at = datetime.now()

        timestamp = self.streamers[streamer_id].started_at.strftime("%y-%m-%d-%H%M")
        self.video_path_in_progress.mkdir(parents=True, exist_ok=True)
        if desc:
            desc = desc.title() + "-"
        filename = str(
            self.video_path_in_progress
            / f"{name}-{timestamp}-{desc}{self._get_short_site_name(site)}.mp4"
        )
        self.logger.debug(f"Saving to {filename}")

        username = self.streamers[streamer_id].sites[site].username
        url = self.sites[site].url.replace("{username}", username)
        cmd = (
            self.config["streamlink_cmd"].strip().split(" ")
            + self.config["streamlink_options"].strip().split(" ")
            + [
                url.strip(),
                "--plugin-dir",
                self.config["streamlink_plugin_path"].strip(),
                "--default-stream",
                self.config["streamlink_default_stream"].strip(),
            ]
        )
        if site == "flirt4free":
            cmd = cmd + [
                "--flirt4free-model-id",
                str(self.streamers[streamer_id].sites[site].external_id),
            ]
        cmd = cmd + [
            "-o",
            filename,
        ]
        self.logger.debug(f"Running: {' '.join(cmd)}")

        creationflags = 0
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        self.processes[streamer_id] = {
            "path": filename,
            "site": site,
            "process": subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                creationflags=creationflags,
            ),
        }

    def _stop_recording_process(self, streamer_id: int, restart: bool = False) -> None:
        #     # If restart is false, we can just go ahead and kill the process, however if
        #     # it's true, we want to ensure we've successfully started the new recording
        #     # before stopping the current one.
        #     if not restart or self.shutting_down:
        #         self._stop_video(name)
        #     else:
        #         # Need to mark them as not recording or _start_recording will ignore
        #         if name in self.streamers.keys():
        #             self.streamers[name].is_recording = False

        #         # They may still be active on another site
        #         if name in self.streamers:
        #             for site in self.streamers[name].sites:
        #                 if self._is_online(name, site):
        #                     self._start_recording(name, site)
        #                     break

        #     # Move file into the to_process folder from in_progress
        #     completed_path = Path(self.processes[name]["path"])
        #     self.video_path_to_process.mkdir(parents=True, exist_ok=True)
        #     to_process_path = self.video_path_to_process / completed_path.name
        #     completed_path.rename(to_process_path)

        #     # Remove from list of active processes
        #     del self.processes[name]

        #     # Start processing the file
        #     self._index_video(to_process_path)

        # def _stop_video(self, name: str) -> None:
        try:
            if os.name == "posix":
                self.processes[streamer_id]["process"].send_signal(signal.SIGINT)
            else:
                self.processes[streamer_id]["process"].send_signal(signal.CTRL_C_EVENT)

            self.processes[streamer_id]["process"].wait(timeout=3)
        except Exception:
            pass
        finally:
            try:
                self.processes[streamer_id]["process"].terminate()
                self.processes[streamer_id]["process"].kill()
                stdout, _ = self.processes[streamer_id]["process"].communicate(
                    timeout=5
                )
                self.logger.debug(f"STDOUT:{stdout}")
            except subprocess.TimeoutExpired:
                self.logger.error("Timeout communicating to process")

        name = self.streamers[streamer_id].name
        self.logger.info(
            f"Stopped recording {name} on {self.processes[streamer_id]['site']}"
        )

        if streamer_id in self.streamers.keys():
            self.streamers[streamer_id].is_recording = False

        # Move file into the to_process folder from in_progress
        completed_path = Path(self.processes[streamer_id]["path"])
        if completed_path.exists():
            self.video_path_to_process.mkdir(parents=True, exist_ok=True)
            to_process_path = self.video_path_to_process / completed_path.name
            completed_path.rename(to_process_path)
            self._index_video(to_process_path)

        # Remove from list of active processes
        del self.processes[streamer_id]

        # They may still be active on another site
        if restart and not self.shutting_down:
            if (
                streamer_id in self.streamers
                and self.streamers[streamer_id].record != Record.NEVER
            ):
                self._start_recording(streamer_id)

    def _should_be_recording(self, streamer_id: int) -> bool:
        """Based on a streamers settings and their current cam status on different sites,
        should we be recording them?"""

        if self.streamers[streamer_id].last_online < datetime.now() - timedelta(
            minutes=5
        ):
            # If we haven't seen them online on any sites, mark them as offline
            for item in self.streamers[streamer_id].sites.values():
                item.cam_status = CamStatus.OFFLINE
            return False

        record = self.streamers[streamer_id].record
        if record == Record.ALWAYS:
            # We always want to record so stop looking through sites
            return True
        elif record != Record.NEVER:
            # We only want to record if they are private on some site
            for item in self.streamers[streamer_id].sites.values():
                if item.cam_status in [
                    CamStatus.EX_PRV,
                    CamStatus.PRV,
                    CamStatus.SHOW,
                ]:
                    if (
                        record == Record.EX_PRV and item.cam_status == CamStatus.EX_PRV
                    ) or record == Record.PRV:
                        return True
        return False

    def _check_recording_processes(self) -> None:
        """Check status of current recording processes"""

        for streamer_id in list(self.processes):
            if self.processes[streamer_id]["process"].poll() is not None:
                self._stop_recording_process(streamer_id, True)
            elif streamer_id not in self.streamers.keys():
                # Streamer was removed from config, stop recording
                self._stop_recording_process(streamer_id)
            else:
                # Still running, restart if too long
                stop_at = self.streamers[streamer_id].started_at + timedelta(
                    minutes=self.config["max_recording_min"]
                )
                if stop_at < datetime.now():
                    name = self.streamers[streamer_id].name
                    self.logger.info(
                        f"{name} has been recording for over {self.config['max_recording_min']} min, restarting"
                    )

                    # Check if they are available to restart
                    is_online = False
                    if streamer_id in self.streamers:
                        if self._is_online(streamer_id):
                            is_online = True

                    if is_online:
                        self._stop_recording_process(streamer_id, True)
                    else:
                        # Reset the timer for them since we wouldn't be able to start a new recording
                        self.logger.info(
                            f"Unable to find a new stream for {name}, restarting the timer instead"
                        )
                        self.streamers[streamer_id].started_at = datetime.now()

    def _is_video_incorrect_length(self, video_path: Path) -> bool:
        """Returns true if video is over max_recording_min + 10"""
        try:
            probe = ffmpeg.probe(str(video_path), show_entries="format=duration")
            # self.logger.debug(f"Probe result: {probe}")
            seconds = int(float(probe["format"]["duration"]))
            minutes = seconds / 60
            if minutes > self.config["max_recording_min"] + 10:
                return True
        except:
            self.logger.debug("Unable to get a duration")
            # Try running the full file through ffmpeg to see if it can find duration
            # output, _ = (
            #    ffmpeg.input(str(video_path))
            #    .output("-", f="null")
            #    .run(capture_stdout=True, quiet=True)
            # )
            # self.logger.debug(_)
            # self.logger.warn(ex.stderr)

        return False

    def _index_video(self, video_path: Path) -> None:
        if not video_path.exists():
            # Sometimes there's a problem making the recording, so the file won't exist
            self.logger.warn(f"Recording does not exist: {video_path}")
            return

        video_path_out = str(video_path).replace(".mp4", ".mkv")
        run_copy_job = self.config.get("run_index_job", True)

        if not run_copy_job:
            # Config says not to run the index job, just try and generate thumbnails
            self._generate_thumbnails(str(video_path))
            return

        if self._is_video_incorrect_length(video_path):
            run_copy_job = False
            try:
                self.logger.debug(f"Running ffmpeg segment/concat on {video_path}")
                segment_path = str(video_path).replace(".mp4", ".txt")
                output_filename = str(video_path).replace(".mp4", "-%03d.mkv")
                (
                    ffmpeg.input(str(video_path))
                    .output(
                        output_filename,
                        c="copy",
                        map=0,
                        f="segment",
                        segment_time="01:00:00",
                        reset_timestamps=1,
                        segment_list=segment_path,
                        segment_list_entry_prefix=f"{self.video_path_to_process}{os.path.sep}",
                        segment_list_type="ffconcat",
                    )
                    .overwrite_output()
                    .run(quiet=True)
                )
                self.index_processes[str(video_path)] = {
                    "started_at": datetime.now(),
                    "process": (
                        ffmpeg.input(segment_path, format="concat", safe=0)
                        .output(video_path_out, c="copy")
                        .overwrite_output()
                        .run_async(quiet=True)
                    ),
                }
            except ffmpeg.Error:
                # Unable to split and concat the file, just try the regular copy job
                run_copy_job = True

        if run_copy_job:
            self.logger.debug(f"Running ffmpeg copy on {video_path}")
            stream = (
                ffmpeg.input(
                    str(video_path), **self.config.get("ffmpeg_input_options", {})
                )
                .output(video_path_out, **self.config.get("ffmpeg_output_options", {}))
                .overwrite_output()
            )
            self.logger.debug(" ".join(ffmpeg.compile(stream)))

            self.index_processes[str(video_path)] = {
                "started_at": datetime.now(),
                "process": ffmpeg.run_async(stream, quiet=True),
            }

    def _check_index_video_processes(self) -> None:
        for video_path in list(self.index_processes):
            process = self.index_processes[video_path]
            timed_out = False
            if (
                process["started_at"]
                + timedelta(minutes=self.config.get("index_video_timeout_min", 30))
            ) < datetime.now():
                timed_out = True
                process["process"].kill()

            if process["process"].poll() is not None:
                try:
                    stdout, stderr = process["process"].communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    process["process"].kill()
                    stdout, stderr = process["process"].communicate()
                finally:
                    if timed_out:
                        self.logger.debug(f"STDOUT: {stdout}")
                        self.logger.debug(f"STDERR: {stderr}")
                    pass

                # Remove from list of active processes
                del self.index_processes[video_path]

                # Check if the new file and old file are within 20% size wise
                orig_file = Path(video_path)
                idx_file = Path(f"{video_path}.idx2")
                new_file = Path(video_path.replace(".mp4", ".mkv"))
                if new_file.exists():
                    orig_file_size = orig_file.stat().st_size
                    new_file_size = new_file.stat().st_size

                    percent_changed = 100
                    if orig_file_size == new_file_size:
                        percent_changed = 0
                    elif orig_file_size == 0:
                        self.logger.warning(f"Got a size of 0 bytes for {video_path}")
                    elif new_file_size == 0:
                        percent_changed = 100
                    else:
                        percent_changed = (
                            abs(new_file_size - orig_file_size) / orig_file_size
                        ) * 100

                    # self.logger.debug(f"After indexing: {new_file.name} changed {percent_changed}%")
                    allowed_change = self.config.get("file_size_percent_change", 20)
                    if percent_changed < allowed_change:
                        try:
                            new_file.replace(video_path)
                        except Exception as e:
                            self.logger.error(f"Error replacing file: {e}")
                    else:
                        self.logger.warning(
                            f"Got larger than {allowed_change}% size change for {video_path}"
                        )
                        # The mkv file wasn't able to be created, delete it and just try to make thumbs
                        if new_file.exists():
                            new_file.unlink()

                if idx_file.exists():
                    idx_file.unlink()

                # Remove segment split files if they exist
                for file in orig_file.parent.glob(
                    orig_file.name.replace(".mp4", "-*.mkv")
                ):
                    file.unlink()

                # Remove segment list if it exists
                segment_file = Path(video_path.replace(".mp4", ".txt"))
                if segment_file.exists():
                    segment_file.unlink()

                # Now that the indexed file is available, generate thumbnails for it
                self._generate_thumbnails(video_path)

    def _generate_thumbnails(self, video_path: str) -> None:
        """Kick off a vcsi process to generate a thumbnail file"""

        if "vcsi_options" in self.config:
            self.logger.debug(f"Generating thumbnails for: {video_path}")
            cmd = ["vcsi", video_path] + self.config["vcsi_options"].split(" ")
            self.logger.debug(" ".join(cmd))
            self.thumb_processes[video_path] = {
                "started_at": datetime.now(),
                "process": subprocess.Popen(
                    cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                ),
            }

    def _check_generate_thumbnail_processes(self) -> None:
        for video_path in list(self.thumb_processes):
            process = self.thumb_processes[video_path]
            timed_out = False

            if (
                process["started_at"]
                + timedelta(minutes=self.config.get("thumbnail_gen_timeout_min", 30))
            ) < datetime.now():
                timed_out = True
                process["process"].kill()

            if process["process"].poll() is not None:
                try:
                    stdout, stderr = process["process"].communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    process["process"].kill()
                    stdout, stderr = process["process"].communicate()

                # Remove from list of active processes
                del self.thumb_processes[video_path]

                file_path = Path(video_path)
                thumb_path = Path(file_path.with_suffix(".mp4.jpg"))
                if not thumb_path.exists():
                    self.logger.debug(f"STDOUT: {stdout}")
                    self.logger.debug(f"STDERR: {stderr}")

                    if timed_out:
                        self.logger.warn(
                            f"Timed out generating thumbnails for {file_path}"
                        )
                    else:
                        self.logger.warn(
                            f"Unable to generate thumbnails for {file_path}"
                        )
                    # Move to failed folder
                    self._fix_permissions(file_path)
                    shutil.move(file_path, self.completed_path_failed / file_path.name)
                else:
                    # Move to completed folder
                    final_path = self.completed_path
                    if "Prv " in file_path.name:
                        final_path = self.completed_path_prv
                    self._fix_permissions(file_path)
                    shutil.move(file_path, final_path / file_path.name)
                    self._fix_permissions(thumb_path)
                    shutil.move(thumb_path, final_path / thumb_path.name)
            else:
                self.logger.debug(f"Thumbnail still running for {video_path}")

    def _cleanup_thumbnails(self) -> None:
        # The process seems to leave bmp files in the tmp folder that don't get cleaned up
        # Delete anything older than x mins
        cmd = [
            "find",
            "/tmp",
            "-name",
            "'*.bmp'",
            "-mmin",
            f"+{self.config['thumbnail_cleanup_min']}",
            "-delete",
        ]
        # self.logger.debug(" ".join(cmd))
        delete_process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def _check_orphaned_files(self) -> None:
        """Look for any files in the to_process folder which are not being processed.
        Only do a few at a time so the system isn't slammed."""
        counter = 0
        for file in self.video_path_to_process.glob("*.mp4"):
            if counter == self.config["process_files_at_once"]:
                break

            is_processing = False
            for video_path in self.index_processes:
                if str(file) == video_path:
                    is_processing = True
                    break

            if is_processing:
                continue

            for video_path in self.thumb_processes:
                if str(file) == video_path:
                    is_processing = True
                    break

            # Has the file already been processed recently?
            if str(file) in self.orphan_file_list:
                is_processing = True

            if not is_processing:
                counter += 1
                self.orphan_file_list.append(str(file))
                if len(self.orphan_file_list) > 10:
                    self.orphan_file_list.pop(0)
                self._index_video(file)

    def _stop_gracefully(self) -> None:
        """When a SIGTERM is received from Docker, stop all recording processes and then try to
        complete the required indexing/thumbnail generation before the SIGKILL is received.
        When using docker stop, ----time=600 will give it 10mins to stop."""
        self.logger.info(
            "Recording processes stopped, attempting to process the resulting files"
        )

        # The processes should all be stopped and ready to process
        self._check_processes()

        # Just wait for all of the processes to finish
        while len(self.index_processes) > 0 and len(self.thumb_processes) > 0:
            self._check_index_video_processes()
            self._check_generate_thumbnail_processes()
            time.sleep(3)

    def _load_sites_from_db(self) -> None:
        """Load the sites from the database, and add in data from the config file"""
        sites = run_sql("SELECT name, display_name FROM site;")
        for site in sites:
            config_site = self.config["sites"][site[0]]

            self.sites[site[0]] = Site(
                name=site[0],
                url=config_site["url"],
                api_key=config_site.get("api_key", None),
                api_url=config_site.get("api_url", None),
                rate_limit_sec=config_site["check_rate_limit_sec"],
                # Init all sites to have been last checked now
                last_checked_at=datetime.now(),
            )

    def _load_streamers_from_db(self) -> None:
        """Load the streamers from the database"""
        streamers = run_sql(
            """
            SELECT  streamer.id, streamer.first_online, streamer.last_online, streamer.days_online,
                    streamer_sites.name, streamer_sites.is_primary, streamer_sites.site_name, streamer_sites.site_id
            FROM streamer_sites
            JOIN streamer ON streamer.id = streamer_sites.streamer_id;
            """
        )

        for streamer in streamers:
            streamer_id = streamer["id"]
            name = streamer["name"]
            site = streamer["site_name"]

            # Set up username_by_site so everyone can be quickly looked up to their streamer id
            self.username_by_site[f"{name}-{site}"] = streamer_id

            if streamer_id not in self.streamers:
                # First entry
                self.streamers[streamer_id] = Streamer(
                    id=streamer_id,
                    name=name,
                    first_online=streamer["first_online"],
                    last_online=streamer["last_online"],
                    days_online=streamer["days_online"],
                    sites={
                        site: Streamer_Site(
                            streamer_id=streamer_id,
                            external_id=streamer["site_id"],
                            site_name=site,
                            username=name,
                            is_primary=True,
                        )
                    },
                )
            else:
                # Append subsequent sites
                self.streamers[streamer_id].sites[site] = Streamer_Site(
                    streamer_id=streamer_id,
                    external_id=streamer["site_id"],
                    site_name=site,
                    username=name,
                    is_primary=False,
                )

    def _make_site_api_call(self, site_name: str, url: str) -> None:
        r = requests.get(url)
        r.raise_for_status()

        if site_name == "flirt4free":
            data = r.text
            data = data[28:]
            data = data.split("'favorites':", 1)[0]
            data = data.replace("'models'", '"models"')
            data = data[:-12] + "]}"
            data = json.loads(data)
            data = data["models"]
        else:
            data = r.json()

        return data

    def _query_site_api(self, site_name: str) -> None:
        if not self.sites[site_name].api_url:
            return

        url = self.sites[site_name].api_url
        if self.sites[site_name].api_key:
            url = url.replace("{API_KEY}", self.sites[site_name].api_key)

        try:
            # SC only returns 1000 records at a time
            if site_name == "stripchat":
                data = []
                for i in range(50):
                    qualified_url = url.replace("{OFFSET}", f"{i}000")
                    output = self._make_site_api_call(site_name, qualified_url)
                    data += output["models"]
                    # self.logger.debug(f"Loaded {len(data)} models for Stripchat")

                    if output["count"] < 1000:
                        break
            else:
                data = self._make_site_api_call(site_name, url)
        except (
            requests.exceptions.RequestException,
            json.decoder.JSONDecodeError,
        ) as e:
            self.logger.exception(f"{site_name} Error")
            return

        conn = get_conn()
        c = conn.cursor()

        male_genders = ["Male", "Couple Male + Male", "m", "male", "males"]
        # Loop through the currently loaded streamers to find new ones
        streamers_to_add = []
        streamers_to_update = []
        streamers_new = []
        for streamer in data:
            username = streamer.get("username", None) or streamer.get("model_seo_name")

            # Skip male streamers
            if self.config.get("ignore_male_streamers", False):
                streamer_gender = streamer.get("gender", "not male")
                if streamer_gender in male_genders:
                    continue

            # Site assigned id instead of username
            external_id = (
                streamer.get("id", None) or streamer.get("model_id", None) or "NULL"
            )
            if username == "LornaDeere":
                pass
            cam_status = self._get_cam_status(site_name, streamer)
            username_key = f"{username}-{site_name}"

            # If they exist with a database id
            if username_key in self.username_by_site:
                streamer_id = self.username_by_site[username_key]

                counter = 0
                if self._update_online_count(streamer_id):
                    counter = 1

                self.streamers[streamer_id].last_online = datetime.now()
                self.streamers[streamer_id].days_online += counter
                streamers_to_update.append(
                    (
                        self.streamers[streamer_id].last_online.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        self.streamers[streamer_id].days_online,
                        streamer_id,
                    )
                )

                # Update the current status for the site
                old_cam_status = self.streamers[streamer_id].sites[site_name].cam_status
                self.streamers[streamer_id].sites[site_name].cam_status = cam_status
                self.streamers[streamer_id].sites[
                    site_name
                ].last_checked_at = datetime.now()

                if self.streamers[streamer_id].record != Record.NEVER:
                    # Their status has changed since the last update
                    if old_cam_status != cam_status:
                        if (
                            old_cam_status == CamStatus.OFFLINE
                            and cam_status == CamStatus.PUB
                        ):
                            # They just came online
                            self.streamers[streamer_id].current_status = cam_status
                            self._start_recording(streamer_id)
                        elif (
                            old_cam_status == CamStatus.PUB
                            and cam_status != CamStatus.PUB
                        ):
                            self.logger.info(
                                f"{self.streamers[streamer_id].name} has gone {cam_status.name} on {site_name}"
                            )
                            # They've gone into some kind of private, maybe we should start recording
                            self._start_recording(
                                streamer_id,
                                f"{cam_status.name} {self._get_short_site_name(site_name)}",
                            )
                        elif (
                            old_cam_status != CamStatus.OFFLINE
                            and old_cam_status != CamStatus.PUB
                            and cam_status == CamStatus.PUB
                        ):
                            self.logger.info(
                                f"{self.streamers[streamer_id].name} is now PUBLIC on {site_name}"
                            )
                            # They were private and are now public, maybe we should stop recording
                            self.streamers[streamer_id].sites[
                                site_name
                            ].cam_status = cam_status
                            if self.streamers[
                                streamer_id
                            ].is_recording and not self._should_be_recording(
                                streamer_id
                            ):
                                self._stop_recording_process(streamer_id)
            else:
                # First time we've ever seen them since they have no database entry
                c.execute(
                    f"INSERT INTO streamer (first_online, days_online) VALUES ('{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 1)"
                )
                # Python implicitly opens a transaction for INSERT and it must be committed
                conn.commit()
                streamer_id = c.lastrowid
                streamer["streamer_id"] = streamer_id
                streamers_to_add.append(streamer)

                c.execute(
                    f"INSERT INTO streamer_sites ('streamer_id', 'name', 'site_id', 'site_name') VALUES ({streamer_id}, '{username}', {external_id}, '{site_name}')"
                )
                conn.commit()

                # Set up streamers_by_site so everyone can be quickly looked up to their streamer id
                self.username_by_site[f"{username}-{site_name}"] = streamer_id

                # Create the in memory record for them
                self.streamers[streamer_id] = Streamer(
                    id=streamer_id,
                    name=username,
                    sites={
                        site_name: Streamer_Site(
                            streamer_id=streamer_id,
                            external_id=external_id,
                            site_name=site_name,
                            username=username,
                            cam_status=Record.NEVER,
                            last_checked_at=datetime.now(),
                        )
                    },
                )

            # Check if we consider them new and we don't record them, get their picture.
            if (
                self._is_streamer_new(streamer, streamer_id)
                and self.streamers[streamer_id].record == Record.NEVER
            ):
                streamer["streamer_id"] = streamer_id
                streamers_new.append(streamer)

        self.logger.debug(
            f"{site_name}: {len(streamers_to_update)} streamers to update"
        )
        if len(streamers_to_add) > 0:
            self.logger.debug(f"{site_name}: {len(streamers_to_add)} streamers to add")

        # Update everyone's last seen to now
        c.executemany(
            "UPDATE streamer SET last_online = ?, days_online = ? WHERE id = ?",
            streamers_to_update,
        )
        conn.commit()

        if self.config.get("download_streamer_thumb", False):
            # Get a picture of any streamer we've seen for the first time
            self._get_thumbnail(streamers_to_add, site_name)

        if self.config.get("download_new_streamer_thumb", False):
            # Get a picture for all streamers marked as new
            self._get_new_thumbnail(streamers_new, site_name)

    # Get the status of a stream for that site
    def _get_cam_status(self, site, data):
        match site:
            case "chaturbate":
                match data["current_show"]:
                    case "public":
                        return CamStatus.PUB
                    case "private":
                        return CamStatus.PRV
                    case "hidden":
                        return CamStatus.SHOW
                    case "away":
                        return CamStatus.OFFLINE

            case "stripchat":
                match data["status"]:
                    case "p2p":
                        return CamStatus.EX_PRV
                    case "private" | "virtualPrivate":
                        return CamStatus.PRV
                    case "groupShow":
                        return CamStatus.SHOW
                    case "public":
                        return CamStatus.PUB

            case "flirt4free":
                match data["room_status"]:
                    case "In Open":
                        return CamStatus.PUB
                    case "In Private":
                        if data["multi_user_private"] == "N":
                            return CamStatus.EX_PRV
                        return CamStatus.PRV

        # Assume they're online and public and let it error out downstream
        return CamStatus.PUB

    def _update_online_count(self, streamer_id: int) -> bool:
        """Update the in memory count of how many times they've been online"""
        # Did we last see them over 24 hours ago?
        if self.streamers[streamer_id].last_online < datetime.now() - timedelta(days=1):
            self.streamers[streamer_id].last_online = datetime.now()
            self.streamers[streamer_id].days_online += 1
            return True
        return False

    def _is_streamer_new(self, streamer: Dict, streamer_id: int) -> bool:
        """Find out if we consider a streamer as new."""
        if self.config["use_is_new_flag"]:
            if streamer.get("is_new", False) or streamer.get("isNew", False):
                return True
            return False

        # If first_online is longer than days_since_first_seen days ago, Not New
        if self.streamers[streamer_id].first_online < datetime.now() - timedelta(
            days=self.config["days_since_first_seen"]
        ):
            return False
        # How many times have we seen them, if more than no_longer_new, Not New
        elif (
            self.streamers[streamer_id].days_online > self.config["no_longer_new_days"]
        ):
            return False

        return True

    def _get_thumbnail(self, streamers: Dict, site: str, is_new: bool = False) -> None:
        for streamer in streamers:
            try:
                username = streamer.get("username", None) or streamer.get(
                    "model_seo_name"
                )
                json_url = None

                if site == "chaturbate":
                    json_url = streamer.get("image_url")
                elif site == "stripchat":
                    json_url = streamer.get(
                        "mlPreviewImage", streamer.get("snapshotUrl")
                    )
                elif site == "bongacams":
                    if "profile_images" in streamer:
                        json_url = streamer["profile_images"].get(
                            "thumbnail_image_big_live",
                            streamer["profile_images"].get(
                                "thumbnail_image_medium_live",
                                streamer["profile_images"].get(
                                    "thumbnail_image_small_live"
                                ),
                            ),
                        )
                    if json_url:
                        json_url = json_url.replace("//", "https://")

                if not json_url:
                    continue

                thumb_url = urlparse(json_url.replace("\\", ""))
                name_path = Path(unquote(thumb_url.path))

                if is_new:
                    folder = self.streamer_new_thumb_path / str(
                        self.streamers[streamer.streamer_id].days_online
                    ).zfill(4)
                else:
                    prefix = username[0].lower()
                    if not prefix.isalpha():
                        prefix = "#"
                    folder = self.streamer_thumb_path / prefix

                folder.mkdir(parents=True, exist_ok=True)
                timestamp = datetime.now().strftime("%y-%m-%d-%H%M")
                thumb_path = (
                    folder
                    / f"{username}-{timestamp}-{site}{name_path.suffix if name_path.suffix else '.jpg'}"
                )

                resp = requests.get(thumb_url.geturl())
                if resp.ok:
                    img_data = resp.content
                    if len(img_data) == 21971:
                        # Default chaturbate image
                        continue

                    with open(thumb_path, "wb") as handle:
                        handle.write(img_data)
                    # There some bug where binary files are not getting correct permissions
                    self._fix_permissions(thumb_path)
                    # thumb_path.chmod(0o0777)

                    if is_new:
                        # cvlib.detect_faces(
                        #     username,
                        #     thumb_path,
                        #     self.streamer_new_thumb_path,
                        #     self.streamer_new_couple_thumb_path,
                        # )
                        self.mp_pool.apply_async(
                            cvlib.detect_faces,
                            args=(
                                username,
                                thumb_path,
                                self.streamer_new_thumb_path,
                                self.streamer_new_couple_thumb_path,
                            ),
                        )
            except Exception as e:
                self.logger.error(f"Error getthing thumbnail: {e}")

    def _get_new_thumbnail(self, streamers: Dict, site: str) -> None:
        # Only get those we need new thumbs for
        streamers_to_get = []
        for streamer in streamers:
            if self.streamers[
                streamer.streamer_id
            ].last_picture_at < datetime.now() - timedelta(
                minutes=self.config["new_streamer_thumb_min"]
            ):
                self.streamers[streamer.streamer_id].last_picture_at = datetime.now()
                streamers_to_get.append(streamer)

        self.logger.debug(
            f"{site}: getting new thumbs for {len(streamers_to_get)} streamers"
        )
        self._get_thumbnail(streamers_to_get, site, True)

    def _fix_permissions(self, path: Path) -> None:
        shutil.chown(str(path), "nobody", "users")
        path.chmod(
            path.stat().st_mode
            | stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IRGRP
            | stat.S_IWGRP
        )

    def run(self):
        while self.running:
            try:
                # Reload config to check for streamer changes
                self._reload_config()

                # Query Chaturbate API
                self._query_site_api("chaturbate")
                # Query Stripchat API
                self._query_site_api("stripchat")
                # Query Bongacams API
                self._query_site_api("bongacams")
                # Query Flirt4Free API
                self._query_site_api("flirt4free")

                self._run_check_online_thread()
                while not self.start_recording_q.empty():
                    streamer_id, site = self.start_recording_q.get()
                    self.streamers[streamer_id].sites[site].cam_status = CamStatus.PUB
                    self._start_recording(streamer_id)

                self._check_recording_processes()
                self._check_index_video_processes()
                self._check_generate_thumbnail_processes()

                self._check_orphaned_files()
                self._cleanup_thumbnails()

                self._list_who_is_recording()

                # Wait in 1 second intervals before restarting loop
                for i in range(self.config["api_rate_limit_time_sec"]):
                    if not self.running:
                        break

                    time.sleep(1)

            except Exception as e:
                self.logger.exception("Loop error")
                time.sleep(1)

        # self._stop_gracefully()
        self.mp_pool.close()
        conn = get_conn()
        conn.close()
        self.logger.info("Fully stopped")
