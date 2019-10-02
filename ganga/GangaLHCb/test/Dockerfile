FROM gitlab-registry.cern.ch/lhcb-core/lbdocker/centos7-build:v3
LABEL maintainer "Ulrik Egede <ulrik.egede@monash.edu>"

USER root

WORKDIR /root

RUN yum install -y python-virtualenv python3

COPY . ganga

ENTRYPOINT . /etc/entrypoint.sh &&\
#             export X509_CERT_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/certificates &&\
#             export X509_VOMS_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/vomsdir &&\
#             export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini &&\
#             export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config &&\
             virtualenv -p python3 venv &&\
             . venv/bin/activate &&\
             pip install --upgrade pip setuptools &&\
             pip install -e ganga &&\
             (cd ganga && pip install --upgrade -r requirements.txt) &&\
             lhcb-proxy-init &&\
             /root/venv/bin/pytest --testLHCb /root/ganga/ganga/GangaLHCb/test --cov-report term --cov-report xml:cov-GangaLHCb.xml --cov /root/ganga/ganga/GangaLHCb --junitxml tests-GangaLHCb.xml
