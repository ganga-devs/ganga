FROM centos:7
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"

RUN yum install -y wget git python-virtualenv gcc

WORKDIR /root

COPY . ganga

RUN virtualenv venv && \
    . venv/bin/activate && \
    pip install --upgrade pip setuptools && \
    pip install -e ganga && \
    cd ganga && \
    pip install --upgrade -r requirements.txt

ENTRYPOINT /root/venv/bin/pytest --cov-report term --cov-report xml:cov-GangaCore.xml --cov /root/ganga/ganga/GangaCore/test --junitxml tests-GangaCore.xml /root/ganga/ganga/GangaCore/test
