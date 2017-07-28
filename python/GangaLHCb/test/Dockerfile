FROM hepsw/cvmfs-lhcb:latest

ENTRYPOINT ["lhcb-proxy-init && \
	     export GANGA_CONFIG_PATH=GangaLHCb/LHCb.ini && \
	     export GANGA_SITE_CONFIG_AREA=/cvmfs/lhcb.cern.ch/lib/GangaConfig/config && \
	     /home/ganga/ganga_env/bin/py.test"]


CMD ["/home/ganga/ganga/python/GangaLHCb/test/Unit", \
     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]