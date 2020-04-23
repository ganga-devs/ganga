# syntax = docker/dockerfile:1.0-experimental
ARG BRANCH_NAME
ARG BUILD_ID

FROM gangacoretest:${BRANCH_NAME}-${BUILD_ID}
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"
ARG VO
ARG DIRAC_VERSION=v6r22p15
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
RUN ./dirac-install -r ${DIRAC_VERSION} -i 27 -g v14r11
RUN --mount=type=secret,id=usercert,dst=/root/.globus/usercert.pem,readonly  --mount=type=secret,id=userkey,dst=/root/.globus/userkey.pem,readonly . /dirac_ui/bashrc && dirac-proxy-init -x
RUN . /dirac_ui/bashrc && dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I
RUN --mount=type=secret,id=usercert,dst=/root/.globus/usercert.pem,readonly  --mount=type=secret,id=userkey,dst=/root/.globus/userkey.pem,readonly . /dirac_ui/bashrc && dirac-proxy-init -g ${VO}_user -M
#RUN cp /tmp/x509up_u0 /tmp/x509up_u0:${VO}_user
#ENV X509_USER_PROXY /tmp/x509up_u0:${VO}_user
RUN mkdir -p /root/.globus
RUN --mount=type=secret,id=usercert,dst=/tmp/usercert.pem,readonly cat /tmp/usercert.pem > /root/.globus/usercert.pem
RUN --mount=type=secret,id=userkey,dst=/tmp/userkey.pem,readonly cat /tmp/userkey.pem > /root/.globus/userkey.pem
RUN chmod 400 /root/.globus/userkey.pem
RUN chmod 644 /root/.globus/usercert.pem

# Update the gangarc file to point to DIRAC UI
##############################################
WORKDIR /ganga
RUN echo -e "[DIRAC]\nDiracEnvSource = /dirac_ui/bashrc" > /root/.gangarc
RUN echo -e "[Configuration]\nRUNTIME_PATH=GangaDirac" >> /root/.gangarc
RUN echo -e "[defaults_DiracProxy]\ngroup=${VO}_user" >> /root/.gangarc
#RUN . /ganga/venv/bin/activate && yes | ganga -g
ENV GANGA_CONFIG_FILE /root/.gangarc
ENV GANGA_CONFIG_PATH GangaDirac/Dirac.ini
ENV DIRAC_DEPRECATED_FAIL True

CMD /ganga/venv/bin/pytest ganga/GangaDirac/test --cov-report term --cov-report xml:cov-GangaDirac.xml --cov ganga/GangaDirac --junitxml tests-GangaDirac.xml
