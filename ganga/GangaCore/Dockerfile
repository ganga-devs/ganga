FROM centos:7
LABEL maintainer "Ulrik Egede <ulrik.egede@monash.edu>"
ARG ganga_version=8.4.2

RUN yum install -y wget python3

# Add the user UID:1000, GID:1000, home at /home/ganga
RUN groupadd -r ganga -g 1000 && \
    useradd -u 1000 -r -g ganga -m -d /home/ganga -s /sbin/nologin -c "Ganga user" ganga && \
    chmod 755 /home/ganga

# Set the working directory to ganga home directory
WORKDIR /home/ganga

# Specify the user to execute all commands below
USER ganga

# Install pip, virtualenv and ganga
RUN python3 -m venv ganga_env && \
    . ~/ganga_env/bin/activate && \
    python3 -m pip install ganga==$ganga_version && \
    yes | ganga -g && \
    echo $ganga_version > ~/gangadir/.used_versions

ENTRYPOINT . ~/ganga_env/bin/activate && \
           /bin/bash
