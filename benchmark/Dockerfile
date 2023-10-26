FROM ubuntu:22.04

ENV CSV_DIR /csv
ENV SERIALIZED_DIR /serialized
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get -y install \
    build-essential \
    ccache \
    cmake \
    curl \
    git \
    jq \
    libssl-dev \
    libcurl4-openssl-dev \
    ninja-build \
    python3-dev \
    python3-pip \
    python-is-python3

RUN pip3 install requests psutil

RUN mkdir -p $CSV_DIR $SERIALIZED_DIR 

RUN useradd --create-home runner
RUN chown -R runner:runner $CSV_DIR $SERIALIZED_DIR

USER runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner

RUN curl -o actions-runner-linux-x64-2.310.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.310.2/actions-runner-linux-x64-2.310.2.tar.gz
RUN echo "fb28a1c3715e0a6c5051af0e6eeff9c255009e2eec6fb08bc2708277fbb49f93  actions-runner-linux-x64-2.310.2.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.310.2.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
