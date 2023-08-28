FROM ubuntu:22.04

ENV CSV_DIR /csv
ENV SERIALIZED_DIR /serialized
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils curl ca-certificates apt-transport-https gnupg software-properties-common
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
RUN apt-get update && apt-get -y install python3-dev python3-pip python-is-python3 cmake nodejs jq curl git libssl-dev libcurl4-openssl-dev
RUN pip3 install requests psutil

RUN mkdir -p $CSV_DIR $SERIALIZED_DIR 

RUN useradd --create-home runner
RUN chown -R runner:runner $CSV_DIR $SERIALIZED_DIR

USER runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner

RUN curl -o actions-runner-linux-x64-2.308.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.308.0/actions-runner-linux-x64-2.308.0.tar.gz
RUN echo "9f994158d49c5af39f57a65bf1438cbae4968aec1e4fec132dd7992ad57c74fa  actions-runner-linux-x64-2.308.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.308.0.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
