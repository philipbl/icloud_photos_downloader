FROM python:3.7

RUN mkdir /app

WORKDIR /app

RUN pip install pipenv

ADD Pipfile /app
ADD Pipfile.lock /app
RUN pipenv install --deploy --system

ADD . /app
