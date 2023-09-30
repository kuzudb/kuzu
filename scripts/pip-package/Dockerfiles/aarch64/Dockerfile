FROM quay.io/pypa/manylinux_2_28_aarch64

# Install dependencies 
RUN yum upgrade curl ca-certificates
RUN yum install -y epel-release
RUN yum update -y
RUN yum install -y cmake jq git perl-Digest-SHA libicu libicu-devel libcurl-devel openssl-devel java-11-openjdk-devel lttng-ust openssl-libs zlib
RUN yum install https://rpm.nodesource.com/pub_20.x/nodistro/repo/nodesource-release-nodistro-1.noarch.rpm -y
RUN yum install nodejs -y --setopt=nodesource-nodejs.module_hotfixes=1

RUN useradd --create-home runner
USER runner

# Install GitHub action runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-arm64-2.309.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.309.0/actions-runner-linux-arm64-2.309.0.tar.gz
RUN echo "b172da68eef96d552534294e4fb0a3ff524e945fc5d955666bab24eccc6ed149  actions-runner-linux-arm64-2.309.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-arm64-2.309.0.tar.gz

COPY --chown=runner:runner listener.sh listener.sh
RUN chmod +x listener.sh

# Install pydoc-markdown
RUN /opt/python/cp310-cp310/bin/python -m pip install --user pipx
RUN /opt/python/cp310-cp310/bin/python -m pipx ensurepath
RUN /opt/python/cp310-cp310/bin/python -m pipx install pydoc-markdown

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/lib:/usr/local/lib64:/usr/lib64
ENTRYPOINT ["./listener.sh"]
