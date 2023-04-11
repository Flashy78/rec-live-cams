import re

from streamlink.plugin import Plugin, pluginmatcher
from streamlink.plugin.api import useragents, validate
from streamlink.stream import HLSStream
from streamlink.utils import parse_json


@pluginmatcher(re.compile(r"https?://([a-z]+\.)?cam4.com/.+"))
class Cam4(Plugin):
    _video_data_re = re.compile(
        r"flashData: (?P<flash_data>{.*}), hlsUrl: '(?P<hls_url>.+?)'"
    )

    _flash_data_schema = validate.Schema(
        validate.all(
            validate.transform(parse_json),
            validate.Schema(
                {
                    "playerUrl": validate.url(),
                    "flashVars": validate.Schema(
                        {
                            "videoPlayUrl": str,
                            "videoAppUrl": validate.url(scheme="rtmp"),
                        }
                    ),
                }
            ),
        )
    )

    def _get_streams(self):
        res = self.session.http.get(
            self.url, headers={"User-Agent": useragents.ANDROID}
        )
        match = self._video_data_re.search(res.text)
        if match is None:
            return

        hls_streams = HLSStream.parse_variant_playlist(
            self.session, match.group("hls_url"), headers={"Referer": self.url}
        )
        for s in hls_streams.items():
            yield s

        """ Removed support for RTMP in Streamlink 3.0.0
        
        rtmp_video = self._flash_data_schema.validate(match.group('flash_data'))
        rtmp_stream = RTMPStream(self.session, {
            'rtmp': rtmp_video['flashVars']['videoAppUrl'],
            'playpath': rtmp_video['flashVars']['videoPlayUrl'],
            'swfUrl': rtmp_video['playerUrl']
        })
        yield 'live', rtmp_stream
        """


__plugin__ = Cam4
