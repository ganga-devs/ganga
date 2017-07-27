FROM centos:6
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"
ARG dirac_version=v6r17p18

RUN yum install -y wget

# Install Python2.7
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


# Add the user UID:1000, GID:1000, home at /home/ganga
RUN groupadd -r ganga -g 1000 && \
    useradd -u 1000 -r -g ganga -m -d /home/ganga -s /sbin/nologin -c "Ganga user" ganga && \
    chmod 755 /home/ganga

# Set the working directory to ganga home directory
WORKDIR /home/ganga


USER ganga

# Install DIRAC UI for Imperial HEP DIRAC Server
RUN mkdir dirac_ui && \
    cd dirac_ui && \
    wget -np -O dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py && \
    chmod u+x dirac-install && \
    ./dirac-install -r $dirac_version -i 27 -g 2017-01-27 && \
    rm -f dirac-install && \
    cd -

RUN curl https://bootstrap.pypa.io/get-pip.py | /usr/local/bin/python2.7 - --user && \
    ~/.local/bin/pip install --user virtualenv && \
    ~/.local/bin/virtualenv ganga_env && \
    (. ganga_env/bin/activate && \
     pip install --upgrade setuptools)

USER root
# Copy in the ganga source from host
COPY ./ /home/ganga/ganga

# when https://github.com/moby/moby/issues/6119 is fixed we can remove this
# and bring copy command below user ganga as it will then respect the user command
RUN chown ganga:ganga /home/ganga/ganga -R

USER ganga

RUN source ganga_env/bin/activate && \
    cd ganga && \
    pip install -r requirements.txt

ENTRYPOINT [". ~/dirac_ui/bashrc && \
           dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I && \
           dirac-proxy-init -g ${vo}_user -M && \ 
	   /home/ganga/ganga_env/bin/py.test"]

CMD ["/home/ganga/ganga/python/GangaDirac/test/Unit", \
     "--cov-report", "xml", "--cov", ".", "--junitxml", "tests.xml"]
