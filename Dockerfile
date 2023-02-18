FROM python:3.8
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY . /app/app
WORKDIR /app/app
CMD ["python", "main.py"]