import json
import logging
import os
import shutil
import signal
import stat
import subprocess
import time
import multiprocessing as mp

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any, Dict
from urllib.parse import unquote, urlparse

import ffmpeg
import requests
import streamlink
import yaml

from recordlivecams.database.common import get_conn, run_sql, set_db_path
from recordlivecams.processes import check_who_is_online
from recordlivecams.face_detector import cvlib

_version = "1.0.0"


@dataclass
class Streamer_Site:
    """Class for keeping track of a site setting for a streamer"""

    site_name: str
    streamer_name: str
    is_primary: bool = True
    # This is just to avoid trying to insert secondary site records every refresh
    is_in_db: bool = True


@dataclass
class Streamer:
    """Class for keeping track of streamers"""

    id: int
    name: str
    sites: Dict[str, Streamer_Site]
    watch: bool = False
    is_recording: bool = False
    # When did the current recording start
    started_at: datetime = datetime.now()
    # Stored in the db for the first time they were online.
    first_online: datetime = datetime.now()
    # Stored in the db from the last time they were online.
    last_online: datetime = datetime.now()
    # Stored in the db how many times have we seen them online (only counts once per 24h)?
    days_online: int = 1
    # For the site polling feature to make sure they aren't checked very often when
    # they haven't been seen online in a long time.
    last_checked_at: datetime = datetime.now()
    last_picture_at: datetime = datetime(1970, 1, 1)


@dataclass
class Site:
    """Class for data about a site"""

    name: str
    url: str
    api_key: str
    api_url: str
    rate_limit_sec: int
    last_checked_at: datetime


class Monitor:
    config = None
    processes = {}
    index_processes = {}
    thumb_processes = {}

    def __init__(
        self,
        logger,
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
        self.streamers = {}
        self.sites = {}
        self.currently_recording = []
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
        self.logger.debug("Side loading Streamlink plugins")
        self.streamlink.plugins.load_path(self.config["streamlink_plugin_path"])
        plugin_count_new = len(self.streamlink.plugins.get_names())
        if plugin_count == plugin_count_new:
            self.logger.warning("No new plugins were loaded")

        # reg signals
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, signum, stack):
        if self.running:
            self.logger.info("Caught stop signal, stopping all recordings")
            self.running = False
            self.shutting_down = True
            self.mp_pool.close()

            for process in self.processes.values():
                # Send sigint, wait for end
                process["process"].send_signal(signal.SIGINT)
                process["process"].wait()

            for process in self.index_processes.values():
                # Send sigint, wait for end
                process["process"].send_signal(signal.SIGINT)
                process["process"].wait()

            for process in self.thumb_processes.values():
                # Send sigint, wait for end
                process["process"].send_signal(signal.SIGINT)
                process["process"].wait()

    def _load_config(self) -> Any:
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

    def _reload_config(self, check_if_new_streamers_online=True):
        try:
            # Load the config and update streamers
            newConfig = self._load_config()
            if newConfig:
                self.config = newConfig
            else:
                return

            # Get the latest list of streamers to watch
            removed = []
            added = []
            # Stop watching deleted streamers
            for name in list(self.streamers.keys()):
                if name not in self.config["streamers"].keys():
                    if self.streamers[name].watch:
                        removed.append(name)
                    self.streamers[name].watch = False

            # Update all other streamers
            for name, sites in self.config["streamers"].items():
                # If they are brand new
                if name not in self.streamers.keys():
                    self.logger.warning(f"Streamer {name} not found in the database")
                    # Create the in memory record for them, with no id
                    streamer_sites = {}
                    for site, username in sites.items():
                        full_site_name = self._get_full_site_name(site)

                        is_primary = False
                        if username == name:
                            is_primary = True

                        streamer_sites[full_site_name] = Streamer_Site(
                            site_name=full_site_name,
                            streamer_name=username,
                            is_primary=is_primary,
                            is_in_db=False,
                        )

                    self.streamers[name] = Streamer(
                        id=None, name=name, sites=streamer_sites, watch=True
                    )

                    if check_if_new_streamers_online:
                        # Newly added, so let's immediately check if they're online
                        for full_site_name in self.streamers[name].sites:
                            if self._is_online(name, full_site_name):
                                self._start_recording(name, full_site_name)
                                break

                    added.append(name)
                else:
                    # TODO: If they get a new username from a diff site in the config, how to add it?
                    self.streamers[name].watch = True

                    # Remove any primary sites that are assigned
                    if sites:
                        for site, site_name in sites.items():
                            full_site_name = self._get_full_site_name(site)

                            if (
                                site_name != name
                                and full_site_name in self.streamers[name].sites
                                and self.streamers[name]
                                .sites[full_site_name]
                                .is_primary
                            ):
                                sites = run_sql(
                                    "UPDATE streamer_sites SET is_primary = 0 WHERE streamer_id = ? AND site_name = ?;",
                                    (self.streamers[name].id, full_site_name),
                                )

            if removed:
                self.logger.info(
                    f"Removed: {(', ').join(sorted(removed, key=str.casefold))}"
                )
            if added:
                self.logger.info(
                    f"Added: {(', ').join(sorted(added, key=str.casefold))}"
                )

            log_level = self.config["log_level"]
            self.logger.setLevel(log_level)
            if log_level == "DEBUG":
                # Don't log url requests for debug
                logging.getLogger("requests").setLevel(logging.WARNING)
                logging.getLogger("urllib3").setLevel(logging.WARNING)

        except Exception as e:
            self.logger.exception("Unable to load config")

    def _get_full_site_name(self, short_name):
        match short_name:
            case "bc":
                return "bongacams"
            case "c4":
                return "cam4"
            case "cb":
                return "chaturbate"
            case "mfc":
                return "mfc"
            case "sc":
                return "stripchat"
            case _:
                return short_name

    def _list_who_is_recording(self):
        recording = []
        for name, streamer in self.streamers.items():
            if streamer.is_recording:
                recording.append(name)

        if recording and self.currently_recording != set(recording):
            self.currently_recording = set(recording)
            self.logger.info(
                f"Recording: {(', ').join(sorted(recording, key=str.casefold))}"
            )

        if len(recording) != len(self.processes):
            self.logger.warn(
                f"Recording {len(recording)} streamers, but {len(self.processes)} threads running"
            )
            # Sometimes a streamer gets marked as being recorded, but the thread is cancelled and not caught
            for name in recording:
                if name not in self.processes:
                    self.logger.info(f"Marking {name} as not being recorded")
                    self.streamers[name].is_recording = False

        index_procs = len(self.index_processes)
        thumb_procs = len(self.thumb_processes)
        if index_procs > 0 or thumb_procs > 0:
            self.logger.debug(
                f"Processing {index_procs} index and {thumb_procs} vcsi threads"
            )

    def _is_online(self, name, site):
        """Returns true of the Streamlink plugin finds any streams for the user on the site
        TODO: Move this to processes.py
        """

        # Check if we need to pause for rate limiting when pinging a site
        diff_in_sec = datetime.now() - self.sites[site].last_checked_at
        sec_to_sleep = self.sites[site].rate_limit_sec - diff_in_sec.total_seconds()
        if sec_to_sleep > 0:
            time.sleep(sec_to_sleep)

        username = self.streamers[name].sites[site].streamer_name
        self.sites[site].last_checked_at = datetime.now()
        self.streamers[name].last_checked_at = datetime.now()
        streams = {}
        try:
            url = self.sites[site].url.replace("{username}", username)
            streams = self.streamlink.streams(url)
        except streamlink.exceptions.PluginError as ex:
            self.logger.debug(f"Streamlink plugin error while checking is_online: {ex}")
        except Exception:
            self.logger.exception("is_online exception")

        return len(streams) > 1

    def _run_check_online_thread(self):
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

    # def _start_recording(self, name, site, is_being_restarted=False):
    #     if not is_being_restarted:
    #         if self.streamers[name].is_recording:
    #             return

    #         self.streamers[name].is_recording = True
    #         # self.streamers[name].started_at = datetime.now()
    #         started_at = datetime.now()

    #         self._start_recording_process(name, site)
    #     else:
    #         self._start_recording_process(name, site)

    # def _start_recording_process(
    #     self, name: str, site: str, started_at: datetime
    # ) -> None:
    #     self.logger.info(f"Starting to record {name} on {site}")

    #     timestamp = started_at.strftime("%y-%m-%d-%H%M")

    def _start_recording(self, name, site):
        if self.streamers[name].is_recording:
            return

        self.streamers[name].is_recording = True
        self.streamers[name].started_at = datetime.now()

        self.logger.info(f"Starting to record {name} on {site}")

        timestamp = self.streamers[name].started_at.strftime("%y-%m-%d-%H%M")
        self.video_path_in_progress.mkdir(parents=True, exist_ok=True)
        filename = str(self.video_path_in_progress / f"{name}-{timestamp}-{site}.mp4")
        self.logger.debug(f"Saving to {filename}")

        username = self.streamers[name].sites[site].streamer_name
        url = self.sites[site].url.replace("{username}", username)
        cmd = self.config["streamlink_cmd"].split(" ") + [
            url,
            "--plugin-dirs",
            self.config["streamlink_plugin_path"],
            "--default-stream",
            self.config["streamlink_default_stream"],
            "-o",
            filename,
        ]
        self.logger.debug(f"Running: {' '.join(cmd)}")

        self.processes[name] = {
            "path": filename,
            "site": site,
            "process": subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            ),
        }

    def _stop_recording_process(self, name: str, restart: bool = False) -> None:
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
            stdout, stderr = self.processes[name]["process"].communicate(timeout=15)
            # self.logger.debug(f"STDOUT:{stdout}")
            # self.logger.debug(f"STDERR:{stderr}")

            self.processes[name]["process"].terminate()
            self.processes[name]["process"].wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.processes[name]["process"].kill()
        finally:
            stdout, stderr = self.processes[name]["process"].communicate()

        self.logger.info(f"Stopped recording {name} on {self.processes[name]['site']}")

        if name in self.streamers.keys():
            self.streamers[name].is_recording = False

        # Move file into the to_process folder from in_progress
        completed_path = Path(self.processes[name]["path"])
        if completed_path.exists():
            self.video_path_to_process.mkdir(parents=True, exist_ok=True)
            to_process_path = self.video_path_to_process / completed_path.name
            completed_path.rename(to_process_path)
            self._index_video(to_process_path)

        # Remove from list of active processes
        del self.processes[name]

        # They may still be active on another site
        if restart and not self.shutting_down:
            if name in self.streamers:
                for site in self.streamers[name].sites:
                    if self._is_online(name, site):
                        self._start_recording(name, site)
                        break

    def _check_recording_processes(self):
        """Check status of current recording processes"""

        for name in list(self.processes):
            if self.processes[name]["process"].poll() is not None:
                self._stop_recording_process(name, True)
            elif name not in self.streamers.keys():
                # Streamer was removed from config, stop recording
                self._stop_recording_process(name)
            else:
                # Still running, restart if too long
                stop_at = self.streamers[name].started_at + timedelta(
                    minutes=self.config["max_recording_min"]
                )
                if stop_at < datetime.now():
                    self.logger.info(
                        f"{name} has been recording for over {self.config['max_recording_min']} min, restarting"
                    )

                    # Check if they are available to restart
                    is_online = False
                    if name in self.streamers:
                        for site in self.streamers[name].sites:
                            if self._is_online(name, site):
                                is_online = True
                                break

                    if is_online:
                        self._stop_recording_process(name, True)
                    else:
                        # Reset the timer for them since we wouldn't be able to start a new recording
                        self.logger.info(
                            f"Unable to find a new stream for {name}, restarting the timer instead"
                        )
                        self.streamers[name].started_at = datetime.now()

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
            self.logger.debug(ffmpeg.compile(stream).join(" "))

            self.index_processes[str(video_path)] = {
                "started_at": datetime.now(),
                "process": ffmpeg.run_async(stream, quiet=True),
            }

    def _check_index_video_processes(self):
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

    def _generate_thumbnails(self, video_path):
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

    def _check_generate_thumbnail_processes(self):
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
                    self._fix_permissions(file_path)
                    shutil.move(file_path, self.completed_path / file_path.name)
                    self._fix_permissions(thumb_path)
                    shutil.move(thumb_path, self.completed_path / thumb_path.name)
            else:
                self.logger.debug(f"Thumbnail still running for {video_path}")

    def _cleanup_thumbnails(self):
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

    def _check_orphaned_files(self):
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

    def _stop_gracefully(self):
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

    def _load_sites_from_db(self):
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

    def _load_streamers_from_db(self):
        """Load the streamers from the database"""
        streamers = run_sql(
            """
            SELECT  streamer.id, streamer.first_online, streamer.last_online, streamer.days_online,
                    streamer_sites.name, streamer_sites.is_primary, streamer_sites.site_name
            FROM streamer_sites
            JOIN streamer ON streamer.id = streamer_sites.streamer_id;
            """
        )

        # Keep a map of id->name so the non-primary streamers can be quickly attached
        id_to_name = {}
        non_primary_streamers = []

        for streamer in streamers:
            if streamer["is_primary"]:
                id_to_name[streamer["id"]] = streamer["name"]

                self.streamers[streamer["name"]] = Streamer(
                    id=streamer["id"],
                    first_online=streamer["first_online"],
                    last_online=streamer["last_online"],
                    days_online=streamer["days_online"],
                    name=streamer["name"],
                    sites={
                        streamer["site_name"]: Streamer_Site(
                            site_name=streamer["site_name"],
                            streamer_name=streamer["name"],
                            is_primary=True,
                        )
                    },
                )
            else:
                non_primary_streamers.append(streamer)

        # Now we can add the non-primary streamers to their existing entry
        for non_primary_streamer in non_primary_streamers:
            if non_primary_streamer["id"] in id_to_name:
                self.streamers[id_to_name[non_primary_streamer["id"]]].sites[
                    non_primary_streamer["site_name"]
                ] = Streamer_Site(
                    site_name=non_primary_streamer["site_name"],
                    streamer_name=non_primary_streamer["name"],
                    is_primary=False,
                )

    def _query_site_api(self, site_name):
        if not self.sites[site_name].api_url:
            return

        url = self.sites[site_name].api_url
        if self.sites[site_name].api_key:
            url = url.replace("{API_KEY}", self.sites[site_name].api_key)

        try:
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
        except (
            requests.exceptions.RequestException,
            json.decoder.JSONDecodeError,
        ) as e:
            self.logger.exception(f"{site_name} Error")
            return

        if site_name == "stripchat":
            data = data["models"]

        male_genders = ["Male", "Couple Male + Male", "m", "male", "males"]
        # Loop through the currently loaded streamers to find new ones
        streamers_to_add = []
        streamers_to_update = []
        streamers_new = []
        for streamer in data:
            username = streamer["username"]

            # Skip male streamers
            if self.config.get("ignore_male_streamers", False):
                is_male = False
                for text in male_genders:
                    if streamer.get("gender") == text:
                        is_male = True
                        break
                if is_male:
                    continue

            # If they exist and have a database id
            if username in self.streamers and self.streamers[username].id:
                counter = 0
                if self._update_online_count(username):
                    counter = 1

                streamers_to_update.append(
                    (
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        counter,
                        self.streamers[username].id,
                    )
                )

                # Are they missing a record for this site?
                if site_name not in self.streamers[username].sites:
                    # Site assigned id instead of username
                    site_id = streamer.get("id", "NULL")

                    run_sql(
                        """
                        INSERT INTO streamer_sites(streamer_id, name, site_id, is_primary, site_name)
                        VALUES (?, ?, ?, ?, ?);
                        """,
                        (
                            self.streamers[username].id,
                            username,
                            site_id,
                            0,
                            site_name,
                        ),
                    )
                    self.streamers[username].sites[site_name] = Streamer_Site(
                        site_name=site_name,
                        streamer_name=username,
                        is_primary=False,
                        is_in_db=True,
                    )

                # Should we start recording them?
                if self.streamers[username].watch:
                    if site_name == "chaturbate":
                        if streamer["current_show"] == "public":
                            self._start_recording(username, site_name)
                    elif site_name == "stripchat":
                        if streamer["status"] == "public":
                            self._start_recording(username, site_name)
                    else:
                        self._start_recording(username, site_name)
            else:
                found = False

                # They don't have a primary record, maybe they are on multiple sites
                for existing_streamer in self.streamers.values():
                    if existing_streamer.id and site_name in existing_streamer.sites:
                        if existing_streamer.sites[site_name].streamer_name == username:
                            found = True

                            counter = 0
                            if self._update_online_count(existing_streamer.name):
                                counter = 1

                            streamers_to_update.append(
                                (
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    counter,
                                    existing_streamer.id,
                                )
                            )

                            # Create a streamer_site record for them
                            if not existing_streamer.sites[site_name].is_in_db:
                                # Site assigned id instead of username
                                site_id = streamer.get("id", "NULL")

                                streamers = run_sql(
                                    """
                                    INSERT INTO streamer_sites(streamer_id, name, site_id, is_primary, site_name)
                                    VALUES (?, ?, ?, ?, ?);
                                    """,
                                    (
                                        existing_streamer.id,
                                        username,
                                        site_id,
                                        0,
                                        site_name,
                                    ),
                                )
                                existing_streamer.sites[site_name].is_in_db = True

                            # Should we start recording them?
                            if existing_streamer.watch:
                                self._start_recording(existing_streamer.name, site_name)

                if not found:
                    streamers_to_add.append(streamer)

            # Check if we consider them new and we don't watch them, get their picture.
            if self._is_streamer_new(streamer):
                if username in self.streamers:
                    if not self.streamers[username].watch:
                        streamers_new.append(streamer)
                else:
                    streamers_new.append(streamer)

        self.logger.debug(
            f"{site_name}: {len(streamers_to_update)} streamers to update"
        )
        if len(streamers_to_add) > 0:
            self.logger.debug(f"{site_name}: {len(streamers_to_add)} streamers to add")

        conn = get_conn()
        c = conn.cursor()
        # Add everyone new to the db
        for streamer in streamers_to_add:
            username = streamer["username"]
            c.execute(
                f"INSERT INTO streamer (first_online, days_online) VALUES ('{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', 1)"
            )
            # Create/Update the in memory record for them
            if username in self.streamers:
                self.streamers[username].id = c.lastrowid

                if site_name not in self.streamers[username].sites:
                    self.streamers[username].sites[site_name] = Streamer_Site(
                        site_name=site_name, streamer_name=streamer["username"]
                    )

                # Should we start recording them?
                if self.streamers[username].watch:
                    self._start_recording(username, site_name)
            else:
                self.streamers[username] = Streamer(
                    id=c.lastrowid,
                    name=username,
                    sites={
                        site_name: Streamer_Site(
                            site_name=site_name, streamer_name=username
                        )
                    },
                )

            # Site assigned id instead of username
            site_id = streamer.get("id", "NULL")

            c.execute(
                f"INSERT INTO streamer_sites (streamer_id, name, site_id, site_name) VALUES ({c.lastrowid}, \"{streamer['username']}\", {site_id}, '{site_name}')"
            )
        conn.commit()

        # Update everyone's last seen to now
        c.executemany(
            "UPDATE streamer SET last_online = ?, days_online = days_online + ? WHERE id = ?",
            streamers_to_update,
        )
        conn.commit()

        if self.config.get("download_streamer_thumb", False):
            # Get a picture of any streamer we've seen for the first time
            self._get_thumbnail(streamers_to_add, site_name)

        if self.config.get("download_new_streamer_thumb", False):
            # Get a picture for all streamers marked as new
            self._get_new_thumbnail(streamers_new, site_name)

    def _update_online_count(self, username):
        """Update the in memory count of how many times they've been online"""
        # Did we last see them over 24 hours ago?
        if self.streamers[username].last_online < datetime.now() - timedelta(days=1):
            self.streamers[username].last_online = datetime.now()
            self.streamers[username].days_online += 1
            return True
        return False

    def _is_streamer_new(self, streamer):
        """Find out if we consider a streamer as new."""
        if self.config["use_is_new_flag"]:
            if streamer.get("is_new", False) or streamer.get("isNew", False):
                return True
            return False

        # Have we not seen them before?
        if streamer["username"] not in self.streamers:
            return True
        # If first_online is longer than days_since_first_seen days ago, Not New
        elif self.streamers[
            streamer["username"]
        ].first_online < datetime.now() - timedelta(
            days=self.config["days_since_first_seen"]
        ):
            return False
        # How many times have we seen them, if more than no_longer_new, Not New
        elif (
            self.streamers[streamer["username"]].days_online
            > self.config["no_longer_new_days"]
        ):
            return False

        return True

    def _get_thumbnail(self, streamers, site, is_new=False):
        for streamer in streamers:
            try:
                username = streamer["username"]
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
                        self.streamers[username].days_online
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

    def _get_new_thumbnail(self, streamers, site):
        # Only get those we need new thumbs for
        streamers_to_get = []
        for streamer in streamers:
            name = streamer["username"]

            if name in self.streamers and self.streamers[
                name
            ].last_picture_at < datetime.now() - timedelta(
                minutes=self.config["new_streamer_thumb_min"]
            ):
                self.streamers[streamer["username"]].last_picture_at = datetime.now()
                streamers_to_get.append(streamer)

        self.logger.debug(
            f"{site}: getting new thumbs for {len(streamers_to_get)} streamers"
        )
        self._get_thumbnail(streamers_to_get, site, True)

    def _fix_permissions(self, path):
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
                # Query Stripchat API
                self._query_site_api("bongacams")

                self._run_check_online_thread()
                while not self.start_recording_q.empty():
                    streamer_name, site = self.start_recording_q.get()
                    self._start_recording(streamer_name, site)

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
