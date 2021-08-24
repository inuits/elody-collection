### Base stage
#FROM python:3.9-slim AS base-stage
FROM python:3.9-alpine AS base-stage

RUN adduser --disabled-password --home /app --shell /bin/ash app
USER app
WORKDIR /app

COPY docker/entrypoint*.sh /
COPY docker/run-tests.sh /
COPY docker/test-coverage.sh /
COPY requirements.txt ./
COPY requirements_dev.txt ./

ENTRYPOINT ["/entrypoint.sh"]

### Development stage
FROM base-stage AS development-stage

ENV APP_ENV=dev

RUN pip install -r requirements_dev.txt

### Build stage
FROM base-stage AS build-stage

ENV APP_ENV=prod

COPY . ./

RUN pip install -r requirements.txt

### Production stage
FROM build-stage AS production-stage

ENV GUNICORN_CMD_ARGS="--workers=3"
