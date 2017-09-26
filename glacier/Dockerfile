FROM debian:jessie

MAINTAINER Emanuele Disco <emanuele.disco@gmail.com>

ENV GLACIER_HOME=/usr/share/glacier/
COPY . / $GLACIER_HOME

RUN set -ex && \
    apt-get update && apt-get -y install \
      python3 \
      ca-certificates \
      curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    curl https://bootstrap.pypa.io/get-pip.py -o /usr/local/src/get-pip.py && \
    python3 /usr/local/src/get-pip.py --user && \
    ~/.local/bin/pip install awscli --upgrade --user && \
    ~/.local/bin/pip install -r $GLACIER_HOME/requirements.txt && \
    chmod +x /usr/share/glacier/*.py && \
    #curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip" && \
    #unzip awscli-bundle.zip && \
    #./awscli-bundle/install -b ~/bin/aws && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/* && \
    rm -rf /usr/local/src

WORKDIR $GLACIER_HOME
ENV PATH $GLACIER_HOME:$PATH

ENTRYPOINT ["aws_glacier.py"]
CMD ["--help"]

