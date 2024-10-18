import json
import logging
import re

from contextlib import closing
from websocket import create_connection
from urllib.parse import urlunparse
from streamlink.exceptions import PluginError, NoStreamsError
from streamlink.logger import StreamlinkLogger
from streamlink.plugin.api import validate, useragents
from streamlink.plugin import Plugin, pluginargument, pluginmatcher
from streamlink.stream import HLSStream

logging.setLoggerClass(StreamlinkLogger)
log = logging.getLogger(".".join(__name__.split(".")[:-1]))


_url_re = re.compile(r"https?://(\w+\.)?flirt4free\.com/\?model=(?P<username>[\w-]+)")

# "c:\Program Files\Streamlink\bin\streamlink" https://www.flirt4free.com/?model=domino-halsted-and-mayda-collins --flirt4free-model-id 1393922 --loglevel=debug --plugin-dir C:\Users\Matt\code\rec-live-cams\plugins --default-stream best -o C:\download-test\in_progress\domino-halsted-and-mayda-collins-24-10-15-08:59-flirt4free.mp4


@pluginmatcher(re.compile(_url_re))
@pluginargument(
    "model-id", required=True, metavar="MODEL-ID", help="The internal model id"
)
class flirt4free(Plugin):
    schema = validate.Schema({"success": "true"})

    def _get_streams(self):
        match = _url_re.match(self.url)
        model_name = match.group("username")
        model_url = f"https://www.flirt4free.com/?model={model_name}"
        # online_url = "https://www.flirt4free.com/?tpl=index2&model=json"

        self.session.http.headers.update(
            {"User-Agent": useragents.CHROME, "Referer": model_url}
        )

        # r = self.session.http.get(online_url)
        # if r.status_code != 200:
        #    raise PluginError("Couldn't get list of online models")
        # data = r.text
        # data = data[28:]
        # data = data.split("'favorites':", 1)[0]
        # data = data.replace("'models'", '"models"')
        # data = data[:-12] + "]}"

        # data = json.loads(data)
        # video_host = ""
        # model_id = ""
        # for model in data["models"]:
        #     if model["model_seo_name"] == model_name:
        #         video_host = model["video_host"]
        #         model_id = model["model_id"]
        #         room_status = model["room_status"]

        #         if room_status != "In Open":
        #             raise PluginError(
        #                 f"Model room_status is not In Open, instead got {room_status}"
        #             )
        #         break

        model_id = self.get_option("model-id")
        if not model_id:
            raise PluginError(f"model_id is invalid: {model_id}")
        #     raise PluginError("Model was not in list of online models")

        stream_url = f"https://www.flirt4free.com/ws/chat/get-stream-urls.php?model_id={model_id}"
        r = self.session.http.get(stream_url)
        if r.status_code != 200:
            raise PluginError("Model get-stream-urls returned non-200 response")

        data = r.json()
        hls_manifest_url = data["data"]["hls"][0]["url"]

        token_url = f"https://www.flirt4free.com/ws/rooms/chat-room-interface.php?a=login_room&model_id={model_id}&browser=chrome&room_type=in_open&popunder_triggered=false&screen_resolution=1280x1024"
        r = self.session.http.get(token_url)
        if r.status_code != 200:
            raise PluginError("Model chat-room-interface returned non-200 response")
        data = r.json()
        token_enc = data["token_enc"]
        room = data["config"]["room"]
        chat = room["host"].split(".")[0]
        port_to_be = room["port_to_be"]

        ws_chat_url = f"wss://www.flirt4free.com/{chat}/chat?token={token_enc}&port_to_be={port_to_be}&model_id={model_id}"
        with closing(
            create_connection(
                ws_chat_url,
                origin="https://www.flirt4free.com",
                host="www.flirt4free.com",
            )
        ) as conn:
            for i in range(5):
                msg = conn.recv()
                msg_json = json.loads(msg)
                command = msg_json["command"]
                if command == "8011":
                    break

        if command != "8011":
            raise PluginError("Model WebSocket chat unable to get command 8011")

        video_key = msg_json["data"]["video_key"]

        # https://hls.vscdns.com/manifest.m3u8?key=421dae287f3acb352aefae4927b465bd&provider=cdn5&model_id=1391608&variant=720
        # https://hls.vscdns.com/chunklist_b2061593.m3u8?key=421dae287f3acb352aefae4927b465bd&model_id=1391608&provider=cdn5&variant=720
        hls_manifest_url = f"http:{hls_manifest_url}".replace(
            "key=nil", f"key={video_key}"
        )

        for s in HLSStream.parse_variant_playlist(
            self.session,
            hls_manifest_url,
            headers={"Referer": "https://flirt4free.com/"},
        ).items():
            yield s


__plugin__ = flirt4free

# https://live-screencaps.vscdns.com/1392629-desktop.jpg
