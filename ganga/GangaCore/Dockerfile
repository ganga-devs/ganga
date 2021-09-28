FROM centos:7
LABEL maintainer "Ulrik Egede <ulrik.egede@monash.edu>"
ARG ganga_version=8.4.2

RUN yum -y update && yum install -y wget git python3

# Add the user UID:1000, GID:1000, home at /home/ganga
RUN groupadd -r ganga -g 1000 && \
    useradd -u 1000 -r -g ganga -m -d /home/ganga -s /sbin/nologin -c "Ganga user" ganga && \
    chmod 755 /home/ganga

# Set the working directory to ganga home directory
WORKDIR /home/ganga

# Install pip, virtualenv and ganga
RUN python3 -m pip install --upgrade pip setuptools wheel && \
    python3 -m pip install "PySocks!=1.5.7,>=1.5.6" && \
    python3 -m pip install -e git+https://github.com/ganga-devs/ganga.git@$ganga_version#egg=ganga

# Specify the user to execute all commands below
USER ganga

RUN yes | ganga -g && \
    mkdir -p ~/.cache/Ganga && \
    echo $ganga_version >  ~/.cache/Ganga/.used_versions

ENTRYPOINT /bin/bash
