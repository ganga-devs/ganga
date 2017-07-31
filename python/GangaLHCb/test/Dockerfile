FROM hepsw/cvmfs-lhcb:latest

COPY ./ /root/ganga

ENTRYPOINT . /etc/cvmfs/run-cvmfs.sh && \
           . ~/.bashrc && \
           export X509_CERT_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/certificates && \
           export X509_VOMS_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/vomsdir && \
           lhcb-proxy-init && \
           export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(dirname $(dirname `which python`))/lib && \
           curl https://bootstrap.pypa.io/get-pip.py | python - --user && \
           ~/.local/bin/pip install --user virtualenv && \
           ~/.local/bin/virtualenv ~/ganga_env && \
           export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini && \
           export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config && \
           . ~/ganga_env/bin/activate && \
           cd ganga && \
           pip install -r requirements.txt && \
           py.test python/GangaLHCb/test/Unit --cov-report xml --cov . --junitxml tests.xml
