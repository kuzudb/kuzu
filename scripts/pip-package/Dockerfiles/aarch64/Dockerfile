FROM quay.io/pypa/manylinux_2_28_aarch64

# Install dependencies 
RUN yum upgrade curl ca-certificates
RUN yum install -y epel-release
RUN yum update -y
RUN yum install -y cmake jq git perl-Digest-SHA libicu libicu-devel libcurl-devel openssl-devel java-11-openjdk-devel lttng-ust openssl-libs zlib ccache
RUN yum install https://rpm.nodesource.com/pub_20.x/nodistro/repo/nodesource-release-nodistro-1.noarch.rpm -y
RUN yum install nodejs -y --setopt=nodesource-nodejs.module_hotfixes=1

RUN useradd --create-home runner
USER runner

# Install GitHub action runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-arm64-2.311.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.311.0/actions-runner-linux-arm64-2.311.0.tar.gz
RUN echo "5d13b77e0aa5306b6c03e234ad1da4d9c6aa7831d26fd7e37a3656e77153611e  actions-runner-linux-arm64-2.311.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-arm64-2.311.0.tar.gz

COPY --chown=runner:runner listener.sh listener.sh
RUN chmod +x listener.sh

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib:/usr/local/lib64:/usr/lib64
ENTRYPOINT ["./listener.sh"]
