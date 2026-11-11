FROM python:3.10
# ffmpeg/ffprobe read uploaded audio and video (length, format, waveform)
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY . /app/app
WORKDIR /app/app
CMD ["python", "app/main.py", "--env", "docker"]