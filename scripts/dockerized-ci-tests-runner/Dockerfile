FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils curl ca-certificates apt-transport-https gnupg software-properties-common
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
RUN apt-get update && apt-get install -y g++ gcc clang-14 python3-dev python3-pip python-is-python3 cmake nodejs jq curl sudo git clang-format-11 lsb-release wget lcov libssl-dev libcurl4-openssl-dev openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN useradd --create-home runner
USER runner

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-update-default-toolchain
ENV PATH="/home/runner/.cargo/bin:${PATH}"
RUN /home/runner/.cargo/bin/rustup toolchain install 1.67


RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-x64-2.307.1.tar.gz -L https://github.com/actions/runner/releases/download/v2.307.1/actions-runner-linux-x64-2.307.1.tar.gz
RUN echo "038c9e98b3912c5fd6d0b277f2e4266b2a10accc1ff8ff981b9971a8e76b5441  actions-runner-linux-x64-2.307.1.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.307.1.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
