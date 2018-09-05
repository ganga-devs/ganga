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

RUN mkdir dirac_ui &&\
    cd dirac_ui &&\
    wget -np -O dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py &&\
    chmod u+x dirac-install &&\
    ./dirac-install -r v6r19p4 -i 27 -g v13r0


RUN echo -e "[DIRAC]\nDiracEnvSource = ~/dirac_ui/bashrc" > ~/.gangarc &&\
    echo -e "[Configuration]\nRUNTIME_PATH=GangaDirac" >> ~/.gangarc


ENTRYPOINT (. /root/dirac_ui/bashrc &&\
	   dirac-proxy-init -x &&\
	   dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I &&\
	   dirac-proxy-init -g ${vo}_user -M) &&\
	   echo -e "[defaults_DiracProxy]\ngroup=${vo}_user" >> ~/.gangarc &&\
	   yes | /root/venv/bin/ganga -g &&\
           . /root/ganga/ganga/GangaDirac/testlib/setup_env.sh &&\
	   /root/venv/bin/pytest /root/ganga/ganga/GangaDirac/test --cov-report term --cov-report xml:cov-GangaDirac.xml --cov /root/ganga/ganga/GangaDirac --junitxml tests-GangaDirac.xml
