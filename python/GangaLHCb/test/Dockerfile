FROM hepsw/cvmfs-lhcb:latest

COPY ./ /root/ganga

CMD ["lhcb-proxy-init && \
     export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(dirname $(dirname `which python`))/lib && \
     curl https://bootstrap.pypa.io/get-pip.py | python - --user && \
     ~/.local/bin/pip install --user virtualenv && \
     ~/.local/bin/virtualenv ~/ganga_env && \
     export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini && \
     export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config && \
     . ~/ganga_env/bin/activate && \
     cd ganga && \
     pip install requirements.txt && \
     py.test", \
     "python/GangaLHCb/test/Unit", \
     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]
