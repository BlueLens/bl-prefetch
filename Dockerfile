#FROM bluelens/python:3.6
FROM bluelens/ubuntu-16.04:py3
MAINTAINER bluehackmaster <master@bluehack.net>

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]