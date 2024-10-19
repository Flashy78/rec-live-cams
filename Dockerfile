FROM python:3.10-bullseye

ENV TZ=America/Los_Angeles

# Download and install the latest git build of ffmpeg
RUN mkdir /ffmpeg-temp && \
    cd /ffmpeg-temp && \
    wget https://johnvansickle.com/ffmpeg/builds/ffmpeg-git-amd64-static.tar.xz && \
    tar xvf ffmpeg-git-amd64-static.tar.xz && \
    rm ffmpeg-git-amd64-static.tar.xz && \
    cd $(ls | head -1) && \
    mv ffmpeg ffprobe /usr/bin/

RUN pip3 install cmake "git+https://github.com/Flashy78/vcsi.git@empty-thumbnail" && \
    pip3 install pyyaml \
    streamlink \
    ffmpeg-python \
    cvlib opencv-contrib-python-headless tensorflow==2.10.0 \
    face_recognition deepdiff

RUN mkdir -p /app/download && \
    mkdir /app/config

ADD ./plugins /app/plugins
ADD ./recordlivecams /app/recordlivecams

WORKDIR /app

VOLUME /app/download
VOLUME /app/config

ENTRYPOINT ["python"]
CMD ["-m", "recordlivecams.program"]
