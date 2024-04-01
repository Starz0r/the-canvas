FROM python:3.9.5-slim-buster

RUN apt-get update
RUN apt-get install curl ca-certificates -y

RUN mkdir /app/
ADD . /app/
WORKDIR /app

RUN pip install pipenv
RUN pipenv install --system --deploy

ENTRYPOINT [ "python", "./src/server/main.py" ]
CMD []