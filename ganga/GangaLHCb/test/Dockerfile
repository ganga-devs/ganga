FROM gitlab-registry.cern.ch/lhcb-core/lbdocker/centos7-build:v3
LABEL maintainer "Ulrik Egede <ulrik.egede@monash.edu>"

USER root

WORKDIR /root

COPY . ganga

ENTRYPOINT /bin/bash

ENTRYPOINT . /etc/entrypoint.sh /root/ganga/ganga/GangaLHCb/test/testrunner
