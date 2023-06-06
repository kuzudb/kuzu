FROM quay.io/pypa/manylinux2014_x86_64

# Install dependencies
RUN yum --disablerepo=epel -y upgrade curl ca-certificates
RUN yum update -y
RUN yum install -y cmake nodejs npm jq git perl-Digest-SHA libicu libicu-devel libcurl-devel openssl-devel

RUN useradd --create-home runner
USER runner

# Install GitHub action runner
RUN mkdir /home/runner/actions-runner
WORKDIR /home/runner/actions-runner
RUN curl -o actions-runner-linux-x64-2.304.0.tar.gz -L https://github.com/actions/runner/releases/download/v2.304.0/actions-runner-linux-x64-2.304.0.tar.gz
RUN echo "292e8770bdeafca135c2c06cd5426f9dda49a775568f45fcc25cc2b576afc12f  actions-runner-linux-x64-2.304.0.tar.gz" | shasum -a 256 -c
RUN tar xzf ./actions-runner-linux-x64-2.304.0.tar.gz

COPY --chown=runner:runner listener.sh listener.sh
RUN chmod +x listener.sh

# Install pydoc-markdown
RUN /opt/python/cp310-cp310/bin/python -m pip install --user pipx
RUN /opt/python/cp310-cp310/bin/python -m pipx ensurepath
RUN /opt/python/cp310-cp310/bin/python -m pipx install pydoc-markdown

ENTRYPOINT ["./listener.sh"]
