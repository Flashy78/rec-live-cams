from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict


class CamStatus(Enum):
    OFFLINE = 1
    PUB = 2
    PRV = 3
    EX_PRV = 4
    SHOW = 5


class Record(Enum):
    NEVER = 1
    ALWAYS = 2
    PRV = 3
    EX_PRV = 4


@dataclass
class Streamer_Site:
    """Class for keeping track of a site setting for a streamer"""

    streamer_id: int
    site_name: str
    username: str
    external_id: str
    is_primary: bool = True
    # This is just to avoid trying to insert secondary site records every refresh
    is_in_db: bool = True
    cam_status: CamStatus = CamStatus.OFFLINE
    last_checked_at: datetime = datetime(1970, 1, 1)


@dataclass
class Streamer:
    """Class for keeping track of streamers"""

    id: int
    # This is used for file names
    name: str
    sites: Dict[str, Streamer_Site]
    record: Record = Record.NEVER
    is_recording: bool = False
    # What site is it recording on
    recording_site = str
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

    def current_status(self):
        status = CamStatus.OFFLINE
        for item in self.sites.values():
            if item.cam_status == CamStatus.EX_PRV:
                return item.cam_status
            elif item.cam_status in [CamStatus.PRV, CamStatus.SHOW]:
                status = item.cam_status
            elif item.cam_status == CamStatus.PUB and status == CamStatus.OFFLINE:
                status = CamStatus.PUB
        return status


@dataclass
class Site:
    """Class for data about a site"""

    name: str
    url: str
    api_key: str
    api_url: str
    rate_limit_sec: int
    last_checked_at: datetime
