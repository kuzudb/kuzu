FROM quay.io/pypa/manylinux2014_x86_64

ENV RUNNER_ALLOW_RUNASROOT=1


# Install dependencies
RUN yum --disablerepo=epel -y upgrade curl ca-certificates
RUN yum update -y
RUN yum install -y cmake nodejs jq git perl-Digest-SHA libicu libicu-devel

# Install GitHub action runner
RUN mkdir /actions-runner
WORKDIR /actions-runner
RUN curl -o actions-runner-linux-x64-2.298.2.tar.gz -L https://github.com/actions/runner/releases/download/v2.298.2/actions-runner-linux-x64-2.298.2.tar.gz
RUN echo "0bfd792196ce0ec6f1c65d2a9ad00215b2926ef2c416b8d97615265194477117  actions-runner-linux-x64-2.298.2.tar.gz" | shasum -a 256
RUN tar xzf ./actions-runner-linux-x64-2.298.2.tar.gz

COPY listener.sh listener.sh
RUN chmod +x listener.sh

ENTRYPOINT ["./listener.sh"]
