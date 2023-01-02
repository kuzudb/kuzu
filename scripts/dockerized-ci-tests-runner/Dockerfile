FROM python:3.9

ENV RUNNER_ALLOW_RUNASROOT=1

# Install dependencies
RUN apt-get update
RUN apt-get install -y apt-transport-https curl gnupg
RUN apt-get install -y cmake
RUN apt-get install -y clang
RUN apt-get install -y clang-format
RUN apt-get install -y clang-13
RUN apt-get install -y nodejs
RUN apt-get install -y jq
RUN apt-get install -y ca-certificates lsb-release wget
RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
RUN rm ./apache-arrow*
RUN apt-get update
RUN apt-get install -y libarrow-dev libparquet-dev

# Install GitHub action runner
RUN mkdir /actions-runner
WORKDIR /actions-runner
RUN curl -o actions-runner-linux-x64-2.298.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.298.2/actions-runner-linux-x64-2.298.2.tar.gz
RUN echo "0bfd792196ce0ec6f1c65d2a9ad00215b2926ef2c416b8d97615265194477117  actions-runner-linux-x64-2.298.2.tar.gz" | shasum -a 256
RUN tar xzf ./actions-runner-linux-x64-2.298.2.tar.gz

COPY start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
