FROM ubuntu:22.04

ENV CSV_DIR /csv
ENV SERIALIZED_DIR /serialized
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get update && apt-get -y install python3-dev python3-pip python-is-python3 cmake nodejs jq curl apt-transport-https gnupg sudo git
RUN pip3 install requests psutil

RUN mkdir -p $CSV_DIR $SERIALIZED_DIR 

RUN useradd --create-home runner
RUN chown -R runner:runner $CSV_DIR $SERIALIZED_DIR

USER runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner

RUN curl -o actions-runner-linux-x64-2.303.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.303.0/actions-runner-linux-x64-2.303.0.tar.gz
RUN echo "e4a9fb7269c1a156eb5d5369232d0cd62e06bec2fd2b321600e85ac914a9cc73  actions-runner-linux-x64-2.303.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.303.0.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
