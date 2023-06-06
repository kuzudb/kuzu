FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils curl ca-certificates apt-transport-https gnupg software-properties-common
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
RUN apt-get update && apt-get install -y g++ gcc clang-14 python3-dev python3-pip python-is-python3 cmake nodejs jq curl sudo git clang-format-11 lsb-release wget lcov libssl-dev libcurl4-openssl-dev

RUN useradd --create-home runner
USER runner

RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-x64-2.304.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.304.0/actions-runner-linux-x64-2.304.0.tar.gz
RUN echo "292e8770bdeafca135c2c06cd5426f9dda49a775568f45fcc25cc2b576afc12f  actions-runner-linux-x64-2.304.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.304.0.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
