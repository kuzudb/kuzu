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

RUN curl -o actions-runner-linux-x64-2.298.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.298.2/actions-runner-linux-x64-2.298.2.tar.gz
RUN echo "0bfd792196ce0ec6f1c65d2a9ad00215b2926ef2c416b8d97615265194477117  actions-runner-linux-x64-2.298.2.tar.gz" | shasum -a 256
RUN tar xzf ./actions-runner-linux-x64-2.298.2.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
