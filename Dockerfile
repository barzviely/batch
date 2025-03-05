FROM amazonlinux:2023

# Install dependencies
RUN dnf update -y && \
    dnf install -y python3 python3-pip wget tar xz && \
    pip3 install boto3 watchtower && \
    dnf clean all

# Install FFmpeg
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz && \
    tar xf ffmpeg-release-amd64-static.tar.xz && \
    mv ffmpeg-*-static/ffmpeg /usr/local/bin/ && \
    mv ffmpeg-*-static/ffprobe /usr/local/bin/ && \
    rm -rf ffmpeg-*

# Create working directory
WORKDIR /app

# Copy extraction script
COPY extract_frames.py /app/

# Set script as entrypoint
ENTRYPOINT ["python3", "/app/extract_frames.py"]