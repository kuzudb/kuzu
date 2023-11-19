FROM quay.io/pypa/manylinux2014_x86_64

# Install dependencies
# ulimit workaround from https://stackoverflow.com/questions/74345206/centos-7-docker-yum-installation-gets-stuck
RUN ulimit -n 4096 && yum --disablerepo=epel -y upgrade curl ca-certificates
RUN ulimit -n 4096 && yum update -y
RUN ulimit -n 4096 && yum install -y cmake nodejs npm jq git perl-Digest-SHA libicu libicu-devel libcurl-devel openssl-devel java-11-openjdk-devel ccache devtoolset-11-toolchain

RUN useradd --create-home runner
USER runner

# Install GitHub action runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-x64-2.311.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-linux-x64-2.311.0.tar.gz
RUN echo "29fc8cf2dab4c195bb147384e7e2c94cfd4d4022c793b346a6175435265aa278  actions-runner-linux-x64-2.311.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.311.0.tar.gz

COPY --chown=runner:runner listener.sh listener.sh
RUN chmod +x listener.sh

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENTRYPOINT ["./listener.sh"]
