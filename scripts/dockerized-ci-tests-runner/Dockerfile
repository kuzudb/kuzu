FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends apt-utils curl ca-certificates apt-transport-https gnupg software-properties-common

# Setup nodejs (see https://github.com/nodesource/distributions#installation-instructions).
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update && apt-get install -y \
    build-essential \
    ccache \
    cmake \
    curl \
    git \
    gnupg \
    jq \
    lcov \
    libcurl4-openssl-dev \
    libssl-dev \
    lsb-release \
    ninja-build \
    nodejs \
    openjdk-17-jdk \
    python-is-python3 \
    python3-dev \
    python3-pip \
    software-properties-common \
    wget

RUN wget https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    yes | ./llvm.sh 17 all && \
    rm llvm.sh && \
    ln /usr/bin/clang++-17 /usr/bin/clang++ && \
    ln /usr/bin/clang-17 /usr/bin/clang && \
    ln /usr/bin/run-clang-tidy-17 /usr/bin/run-clang-tidy && \
    ln /usr/bin/clangd-17 /usr/bin/clangd

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN useradd --create-home runner
USER runner

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-update-default-toolchain
ENV PATH="/home/runner/.cargo/bin:${PATH}"
RUN /home/runner/.cargo/bin/rustup toolchain install 1.67


RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-x64-2.310.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.310.2/actions-runner-linux-x64-2.310.2.tar.gz
RUN echo "fb28a1c3715e0a6c5051af0e6eeff9c255009e2eec6fb08bc2708277fbb49f93  actions-runner-linux-x64-2.310.2.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.310.2.tar.gz

COPY --chown=runner:runner start.sh start.sh
RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]
