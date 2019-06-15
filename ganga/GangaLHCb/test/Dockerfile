FROM hepsw/cvmfs-lhcb:latest
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"

RUN yum install -y wget git python-virtualenv glibc-devel gcc

WORKDIR /root

COPY . ganga

ENTRYPOINT . /etc/cvmfs/run-cvmfs.sh &&\
           . ~/.bashrc &&\
            export X509_CERT_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/certificates &&\
            export X509_VOMS_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/vomsdir &&\
            export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(dirname $(dirname `which python`))/lib &&\
            export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini &&\
            export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config &&\
            virtualenv -p `which python` venv &&\
            . venv/bin/activate &&\
            pip install --upgrade pip setuptools &&\
            pip install -e ganga &&\
            (cd ganga && pip install --upgrade -r requirements.txt) &&\
            lhcb-proxy-init &&\
            /root/venv/bin/pytest --testLHCb /root/ganga/ganga/GangaLHCb/test --cov-report term --cov-report xml:cov-GangaLHCb.xml --cov /root/ganga/ganga/GangaLHCb --junitxml tests-GangaLHCb.xml
