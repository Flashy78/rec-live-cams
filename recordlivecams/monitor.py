import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from threading import Thread
from typing import Any, Dict

import ffmpeg
import streamlink
import yaml

from recordlivecams.processes import check_who_is_online

_version = "0.3.4"


@dataclass
class Streamer:
    """Class for keeping track of streamers"""

    name: str
    sites: Dict[str, str]
    is_recording: bool = False
    started_at: datetime = datetime.now()
    last_checked_at: datetime = datetime.now()


@dataclass
class Site:
    """Class for data about a site"""

    name: str
    url: str
    rate_limit_sec: int
    last_checked_at: datetime


class Monitor:
    config = None
    processes = {}
    index_processes = {}
    thumb_processes = {}

    def __init__(
        self, logger, config_path: Path, config_template_path: Path, video_path: Path
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
        self.video_path_failed = video_path / "failed"
        self.video_path_failed.mkdir(parents=True, exist_ok=True)
        self.streamers_path = video_path / "streamers"
        self.streamers_path.mkdir(parents=True, exist_ok=True)

        self.config_timestamp = None
        self.config_path = config_path / "config.yaml"
        self.config_template_path = config_template_path
        self.video_path = video_path
        self.streamers = {}
        self.sites = {}
        self.currently_recording = []
        self.orphan_file_list = []

        self.start_recording_q = Queue()

        self._reload_config(False)

        # On startup, deal with any lingering files that need to be processed
        for file in self.video_path_in_progress.glob("*"):
            file.rename(self.video_path_to_process / file.name)

        self.streamlink = streamlink.Streamlink()
        plugin_count = len(self.streamlink.plugins)
        self.logger.debug("Side loading Streamlink plugins")
        self.streamlink.load_plugins(self.config["streamlink_plugin_path"])
        plugin_count_new = len(self.streamlink.plugins)
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

    def _get_streamer_path(self, name) -> Path:
        return self.streamers_path / f"{name}.txt"

    def _load_streamer_file(self, name) -> datetime:
        """Load a streamer file which should contain the last time they were seen online"""
        streamer_path = self._get_streamer_path(name)
        if streamer_path.is_file():
            with open(streamer_path, "r") as f:
                date_text = f.read()
        else:
            # They're new, give them a starting file
            date_text = self._save_streamer_file(name)

        return datetime.strptime(date_text, "%y-%m-%d-%H%M")

    def _save_streamer_file(self, name) -> None:
        """Update a streamer file to indicate they were seen online now"""
        streamer_path = self._get_streamer_path(name)
        date_text = datetime.now().strftime("%y-%m-%d-%H%M")
        with open(streamer_path, "w") as f:
            f.write(date_text)
        return date_text

    def _load_config(self) -> Any:
        # opens file, returns json dict, returns None if error
        try:
            if not self.config_path.is_file():
                # We don't have an initial config, use the template to create one
                self.logger.info("Writing initial config from template")
                shutil.copy(self.config_template_path, self.config_path)

            modified_at = self.config_path.stat().st_mtime
            if self.config_timestamp and self.config_timestamp == modified_at:
                return None
            else:
                self.config_timestamp = modified_at

            self.logger.debug(f"Loading config at: {self.config_path}")
            with open(self.config_path, "r") as f:
                return yaml.safe_load(f)

        except Exception as e:
            print(e)
            return None

    def _reload_config(self, check_if_new_streamers_online=True):
        try:
            # Load the config and update streamers
            newConfig = self._load_config()
            if newConfig:
                self.config = newConfig
            else:
                return

            self.logger.setLevel(self.config["log_level"])

            # Init all sites to have been last checked now
            for site, data in self.config["sites"].items():
                self.sites[site] = Site(
                    name=site,
                    url=data["url"],
                    rate_limit_sec=data["check_rate_limit_sec"],
                    last_checked_at=datetime.now(),
                )

            removed = []
            added = []
            # Remove deleted streamers
            for name in list(self.streamers.keys()):
                if name not in self.config["streamers"].keys():
                    removed.append(name)
                    del self.streamers[name]

            # Update all other streamers
            if self.config["streamers"]:
                for name, sites in self.config["streamers"].items():
                    if name not in self.streamers.keys():
                        self.streamers[name] = Streamer(name=name, sites=sites)
                        # Check when last seen online
                        self.streamers[name].started_at = self._load_streamer_file(name)

                        if check_if_new_streamers_online:
                            # Newly added, so let's immediately check if they're online
                            for site in self.streamers[name].sites:
                                if self._is_online(name, site):
                                    self._start_recording(name, site)
                                    break

                        added.append(name)
                    else:
                        self.streamers[name].sites = sites

            if removed:
                self.logger.info(
                    f"Removed: {(', ').join(sorted(removed, key=str.casefold))}"
                )
            if added:
                self.logger.info(
                    f"Added: {(', ').join(sorted(added, key=str.casefold))}"
                )
        except Exception as e:
            self.logger.exception("Unable to load config")

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

        username = self.streamers[name].sites[site]
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

        self._save_streamer_file(name)

        username = self.streamers[name].sites[site]
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
        self.logger.debug(f"Running: {cmd}")

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
                    self._stop_recording_process(name, True)

    def _is_video_incorrect_length(self, video_path: Path) -> bool:
        """Returns true if video is over max_recording_min + 10"""
        try:
            probe = ffmpeg.probe(str(video_path), show_entries="format=duration")
            self.logger.debug(probe)
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
        run_copy_job = True

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
            self.index_processes[str(video_path)] = {
                "started_at": datetime.now(),
                "process": (
                    ffmpeg.input(str(video_path))
                    .output(video_path_out, c="copy")
                    .overwrite_output()
                    .run_async(quiet=True)
                ),
            }

    def _check_index_video_processes(self):
        for video_path in list(self.index_processes):
            process = self.index_processes[video_path]
            if (process["started_at"] + timedelta(minutes=30)) < datetime.now():
                process["process"].kill()

            if process["process"].poll() is not None:
                try:
                    stdout, stderr = process["process"].communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    process["process"].kill()
                    stdout, stderr = process["process"].communicate()
                finally:
                    # self.logger.debug(f"STDOUT: {stdout}")
                    # self.logger.debug(f"STDERR: {stderr}")
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

                    self.logger.debug(
                        f"After indexing: {new_file.name} changed {percent_changed}%"
                    )
                    if percent_changed < 20:
                        try:
                            new_file.replace(video_path)
                        except Exception as e:
                            self.logger.error(f"Error replacing file: {e}")
                    else:
                        self.logger.warning(
                            f"Got larger than 20% size change for {video_path}"
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
            if (process["started_at"] + timedelta(minutes=30)) < datetime.now():
                process["process"].kill()

            if process["process"].poll() is not None:
                try:
                    stdout, stderr = process["process"].communicate(timeout=15)
                except subprocess.TimeoutExpired:
                    process["process"].kill()
                    stdout, stderr = process["process"].communicate()
                finally:
                    # self.logger.debug(f"STDOUT: {stdout}")
                    # self.logger.debug(f"STDERR: {stderr}")
                    pass

                # Remove from list of active processes
                del self.thumb_processes[video_path]

                file_path = Path(video_path)
                thumb_path = Path(file_path.with_suffix(".mp4.jpg"))
                if not thumb_path.exists():
                    self.logger.warn(f"Unable to generate thumbnails for {file_path}")
                    # Move to failed folder
                    file_path.rename(self.video_path_failed / file_path.name)
                else:
                    # Move to completed folder
                    file_path.rename(self.video_path / file_path.name)
                    thumb_path.rename(self.video_path / thumb_path.name)
            else:
                self.logger.debug(f"Thumbnail still running for {video_path}")

    def _cleanup_thumbnails(self):
        # The process seems to leave bmp files in the tmp folder that don't get cleaned up
        # Delete anything older than x mins
        cmd = [
            "find",
            "/tmp",
            "-name",
            "*.bmp",
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
                if len(self.orphan_file_list) > 20:
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

    def run(self):
        while self.running:
            try:
                # Reload config to check for streamer changes
                self._reload_config()

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
                for i in range(5):
                    if not self.running:
                        break

                    time.sleep(1)

            except Exception as e:
                self.logger.exception("Loop error")
                time.sleep(1)

        # self._stop_gracefully()
        self.logger.info("Fully stopped")
