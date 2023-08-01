FROM quay.io/pypa/manylinux2014_x86_64

# Install dependencies
RUN yum --disablerepo=epel -y upgrade curl ca-certificates
RUN yum update -y
RUN yum install -y cmake nodejs npm jq git perl-Digest-SHA libicu libicu-devel libcurl-devel openssl-devel java-11-openjdk-devel

RUN useradd --create-home runner
USER runner

# Install GitHub action runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-x64-2.306.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.306.0/actions-runner-linux-x64-2.306.0.tar.gz
RUN echo "b0a090336f0d0a439dac7505475a1fb822f61bbb36420c7b3b3fe6b1bdc4dbaa  actions-runner-linux-x64-2.306.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.306.0.tar.gz

COPY --chown=runner:runner listener.sh listener.sh
RUN chmod +x listener.sh

# Install pydoc-markdown
RUN /opt/python/cp310-cp310/bin/python -m pip install --user pipx
RUN /opt/python/cp310-cp310/bin/python -m pipx ensurepath
RUN /opt/python/cp310-cp310/bin/python -m pipx install pydoc-markdown

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
ENTRYPOINT ["./listener.sh"]
