# FROM taking build arg requires at least
#   Docker version 17.05.0-ce, build 89658be
#
# Note to run a container from this image, the vo must be given
# as an environment variable and the directory containing the users
# grid cert/key .pem files must be mounted at /home/ganga/.globus. e.g.
#   docker run -e "vo=gridpp" -v $HOME/.globus:/home/ganga/.globus -it ganga-dirac
#
ARG ganga_version=latest
FROM ganga-core:$ganga_version
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"
ARG dirac_version=v6r20p22

# Install DIRAC UI for Imperial HEP DIRAC Server
RUN mkdir dirac_ui && \
    cd dirac_ui && \
    wget -np -O dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py && \
    chmod u+x dirac-install && \
    ./dirac-install -r $dirac_version -i 27 -g v14r2 && \
    rm -f dirac-install

# Add DIRAC setup script to config options
RUN echo -e "[DIRAC]\nDiracEnvSource = ~/dirac_ui/bashrc" > ~/.gangarc && \
    echo -e "[Configuration]\nRUNTIME_PATH=GangaDirac" >> ~/.gangarc

ENTRYPOINT (. ~/dirac_ui/bashrc && \
           dirac-proxy-init -x && \
           dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I && \
           dirac-proxy-init -g ${vo}_user -M) && \
           cp /tmp/x509up_u`id -u ganga`{,:${vo}_user} && \
           echo -e "[defaults_DiracProxy]\ngroup=${vo}_user" >> ~/.gangarc && \
           . ~/ganga_env/bin/activate && \
           yes | ganga -g > /dev/null && \
           /bin/bash
