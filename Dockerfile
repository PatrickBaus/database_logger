FROM alpine:3.22.1 AS base


FROM base AS builder
ARG TARGETPLATFORM

COPY . /app

RUN COLOUR='\e[1;93m' && \
  echo -e "${COLOUR}Installing build dependencies...\e[0m" && \
  if [ "$TARGETPLATFORM" = "linux/arm/v6" ] || [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then BUILD_DEPS="python3-dev musl-dev gcc"; fi && \
  apk --no-cache add --virtual=build-dependencies \
    ${BUILD_DEPS} \
    openssh-client-common \
    openssh-client-default \
    git \
    py3-pip && \
  echo -e "${COLOUR}Done.\e[0m"

# Define the python virtual environment
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv --upgrade-deps $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN COLOUR='\e[1;93m' && \
  echo -e "${COLOUR}Installing Kraken database logger...\e[0m" && \
  pip install ./app && \
  echo -e "${COLOUR}Done.\e[0m"


FROM base AS runner
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

COPY --from=builder --chown=worker:worker /app /app

USER worker

CMD ["python3", "-OO", "-u", "/app/database_logger.py"]
