FROM alpine:3.18 as builder

ARG BUILD_CORES
ARG GIT_REPOSITORY
ARG SSH_DEPLOY_KEY

# Build the
RUN COLOUR='\e[1;93m' && \
  test -n "$GIT_REPOSITORY" || (echo "\e[0;31mGIT_REPOSITORY  not set.\e[0m" && false) && \
  echo -e "${COLOUR}Installing build dependencies...\e[0m" && \
  apk --no-cache add --virtual=build-dependencies \
    openssh-client-common \
    openssh-client-default \
    git \
    py3-pip && \
  echo -e "${COLOUR}Done.\e[0m"

# Define the python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN COLOUR='\e[1;93m' && \
  echo -e "${COLOUR}Installing Kraken database logger...\e[0m" && \
  mkdir /root/.ssh/ && \
  echo "${SSH_DEPLOY_KEY}" > /root/.ssh/id_rsa && \
  chmod 600 /root/.ssh/id_rsa && \
  ssh-keyscan github.com >> /root/.ssh/known_hosts && \
  git clone git@github.com:${GIT_REPOSITORY}.git app && \
  pip install ./app && \
  echo -e "${COLOUR}Done.\e[0m"

FROM alpine:3.18
LABEL maintainer="Patrick Baus <patrick.baus@physik.tu-darmstadt.de>"
LABEL description="Kraken sensor data aggregator."

ARG WORKER_USER_ID=5555
ENV DATABASE_PORT=5432

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

CMD python3 -OO -u /app/database_logger.py
