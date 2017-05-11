FROM alexanderrichards/dirac_ui:latest
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"
ARG ganga_version=6.5.2

USER root
RUN yum groupinstall -y 'development tools' && \
    yum install -y zlib-dev openssl-devel sqlite-devel bzip2-devel readline-devel && \
    wget -np -O Python-2.7.13.tar.xz https://www.python.org/ftp/python/2.7.13/Python-2.7.13.tar.xz && \
    tar -xvf Python-2.7.13.tar.xz && \
    cd Python-2.7.13 && \
    ./configure --prefix=/usr/local && \
    make && \
    make altinstall && \
    cd - && \
    rm -rf Python-2.7.13*

USER dirac
RUN curl https://bootstrap.pypa.io/get-pip.py | /usr/local/bin/python2.7 - --user && \
    ~/.local/bin/pip install --user virtualenv && \
    ~/.local/bin/virtualenv ganga_env && \
    . ~/ganga_env/bin/activate && \
    pip install ganga==$ganga_version && \
    echo -e '[defaults_DiracProxy]\ngroup="gridpp_user"' > ~/.gangarc && \
    echo -e "[DIRAC]\nDiracEnvSource = ~/dirac_ui/bashrc" >> ~/.gangarc && \
    echo -e "[Configuration]\nRUNTIME_PATH=GangaDirac" >> ~/.gangarc && \
    yes | ganga -g && \
    echo $ganga_version > ~/gangadir/.used_versions

ENTRYPOINT (. ~/dirac_ui/bashrc && \
           dirac-proxy-init -x && \
           dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I && \
           dirac-proxy-init -g ${vo}_user -M) && \
           . ~/ganga_env/bin/activate && \
           /bin/bash
