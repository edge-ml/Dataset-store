FROM python:3.10
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY . /app/app
WORKDIR /app/app
CMD ["python", "app/main.py", "--env", "docker"]