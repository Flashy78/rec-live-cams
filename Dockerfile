FROM python:3.10-bullseye

ENV TZ=America/Los_Angeles

# RUN apt-get update && apt-get install gosu

# If you don't have Debian backports already (see link below):
RUN echo "deb http://deb.debian.org/debian bullseye-backports main" | tee "/etc/apt/sources.list.d/streamlink.list"
RUN apt update && \
    apt -t bullseye-backports install -y streamlink && \
    rm -rf /var/lib/apt/lists/*

# Install cmake and vcsi
RUN pip3 install cmake vcsi

RUN pip3 install cvlib ffmpeg-python opencv-contrib-python pyyaml streamlink tensorflow

RUN mkdir -p /app/download && \
    mkdir /app/config

ADD ./plugins /app/plugins
ADD ./recordlivecams /app/recordlivecams

WORKDIR /app

VOLUME /app/download
VOLUME /app/config

ENTRYPOINT ["python"]

CMD ["-m", "recordlivecams.program"]
