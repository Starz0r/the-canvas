FROM python:3.9.5-slim-buster

RUN apt-get update
RUN apt-get install curl -y

RUN mkdir /app/
ADD . /app/
WORKDIR /app

RUN pip install pipenv
RUN pipenv install --system --deploy

HEALTHCHECK CMD curl --fail http://localhost:8765/healthz || exit 1

ENTRYPOINT [ "python", "./src/server/main.py" ]
CMD []
