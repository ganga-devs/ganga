FROM centos:6
LABEL maintainer "Alexander Richards <a.richards@imperial.ac.uk>"
ARG ganga_version=6.5.2

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

# Specify the user to execute all commands below
USER ganga

# Install pip, virtualenv and ganga
RUN curl https://bootstrap.pypa.io/get-pip.py | /usr/local/bin/python2.7 - --user && \
    ~/.local/bin/pip install --user virtualenv && \
    ~/.local/bin/virtualenv ganga_env && \
    . ~/ganga_env/bin/activate && \
    pip install ganga==$ganga_version && \
    yes | ganga -g && \
    echo $ganga_version > ~/gangadir/.used_versions

ENTRYPOINT . ~/ganga_env/bin/activate && \
           /bin/bash
