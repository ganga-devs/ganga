# syntax = docker/dockerfile:1.0-experimental
ARG BRANCH_NAME
ARG BUILD_ID
FROM gangacoretest:${BRANCH_NAME}-${BUILD_ID}
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"
ARG VO

# Make directory for the incoming secrets (robot cert/key)
WORKDIR /
RUN mkdir -p /root/.globus

# Install DIRAC UI
##################
RUN mkdir /dirac_ui
WORKDIR /dirac_ui

# workdir doesn't seem to be working properly
RUN wget -np -O dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py
RUN chmod u+x dirac-install
RUN ./dirac-install -r v6r20p26 -i 27 -g v14r1
RUN --mount=type=secret,id=usercert,dst=/root/.globus/usercert.pem,readonly  --mount=type=secret,id=userkey,dst=/root/.globus/userkey.pem,readonly . /dirac_ui/bashrc && dirac-proxy-init -x
RUN . /dirac_ui/bashrc && dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I
RUN --mount=type=secret,id=usercert,dst=/root/.globus/usercert.pem,readonly  --mount=type=secret,id=userkey,dst=/root/.globus/userkey.pem,readonly . /dirac_ui/bashrc && dirac-proxy-init -g ${VO}_user -M

# Update the gangarc file to point to DIRAC UI
##############################################
WORKDIR /ganga
RUN echo -e "[DIRAC]\nDiracEnvSource = /dirac_ui/bashrc" > /root/.gangarc
RUN echo -e "[Configuration]\nRUNTIME_PATH=GangaDirac" >> /root/.gangarc
RUN echo -e "[defaults_DiracProxy]\ngroup=${VO}_user" >> /root/.gangarc
RUN . /ganga/venv/bin/activate && yes | ganga -g

CMD . /ganga/ganga/GangaDirac/testlib/setup_env.sh &&\
    /ganga/venv/bin/pytest ganga/GangaDirac/test --cov-report term --cov-report xml:cov-GangaDirac.xml --cov ganga/GangaDirac --junitxml tests-GangaDirac.xml
