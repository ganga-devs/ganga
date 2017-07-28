FROM hepsw/cvmfs-lhcb:latest

RUN curl https://bootstrap.pypa.io/get-pip.py | python - --user && \
    ~/.local/bin/pip install --user virtualenv && \
    ~/.local/bin/virtualenv ~/ganga_env

COPY ./ ~/ganga

ENTRYPOINT ["lhcb-proxy-init && \
	     export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini && \
	     export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config && \
	     ~/ganga_env/bin/py.test"]

CMD ["~/ganga/python/GangaLHCb/test/Unit", \
     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]