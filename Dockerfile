FROM alpine:3.16 as builder

ARG BUILD_CORES

# Build the
RUN COLOUR='\e[1;93m' && \
  echo -e "${COLOUR}Installing build dependencies...\e[0m" && \
  apk --no-cache add --virtual=build-dependencies \
    build-base \
    py3-pip \
    git && \
  echo -e "${COLOUR}Done.\e[0m"

# Define the python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ADD https://api.github.com/repos/PatrickBaus/database_logger/git/refs/heads/master version.json
RUN COLOUR='\e[1;93m' && \
  echo -e "${COLOUR}Installing Kraken database logger...\e[0m" && \
  git clone https://github.com/PatrickBaus/database_logger  app && \
  pip install ./app && \
  echo -e "${COLOUR}Done.\e[0m"

FROM alpine:3.16
LABEL maintainer="Patrick Baus <patrick.baus@physik.tu-darmstadt.de>"
LABEL description="Kraken sensor data aggregator."

ARG WORKER_USER_ID=5555
ARG SENSOR_IP=localhost

# Upgrade installed packages,
# add a user called `worker`
# Then install Python dependency
RUN apk --no-cache upgrade && \
    addgroup -g ${WORKER_USER_ID} worker && \
    adduser -D -u ${WORKER_USER_ID} -G worker worker && \
    apk --no-cache add python3

COPY --from=builder /opt/venv /opt/venv

# Enable venv
ENV PATH="/opt/venv/bin:$PATH"

COPY --from=builder /app /app
RUN chown -R worker:worker /app

USER worker

CMD python3 -u /app/database_logger.py
