FROM gitlab-registry.cern.ch/lhcb-core/lbdocker/centos7-build:v3
LABEL maintainer "Ulrik Egede <ulrik.egede@monash.edu>"

USER root

WORKDIR /root

RUN yum install -y python-virtualenv python3

COPY . ganga

ENTRYPOINT /bin/bash

ENTRYPOINT . /etc/entrypoint.sh /root/ganga/ganga/GangaLHCb/test/testrunner

