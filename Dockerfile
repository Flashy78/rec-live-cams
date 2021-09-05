FROM python:3.9-bullseye

ENV streamlinkVersion=2.4.0
ENV TZ=America/Los_Angeles

ADD https://github.com/streamlink/streamlink/releases/download/${streamlinkVersion}/streamlink-${streamlinkVersion}.tar.gz /opt/

# RUN apt-get update && apt-get install gosu

# Install Streamlink
RUN tar -xzf /opt/streamlink-${streamlinkVersion}.tar.gz -C /opt/ && \
    rm /opt/streamlink-${streamlinkVersion}.tar.gz && \
    cd /opt/streamlink-${streamlinkVersion}/ && \
    python setup.py install

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Install vcsi
RUN pip3 install vcsi

RUN mkdir -p /app/download && \
    mkdir /app/config

ADD ./plugins /app/plugins
ADD ./recordlivecams /app/recordlivecams

RUN pip3 install -r /app/recordlivecams/requirements.txt

WORKDIR /app

VOLUME /app/download
VOLUME /app/config

ENTRYPOINT ["python"]

CMD ["-m", "recordlivecams.program"]
