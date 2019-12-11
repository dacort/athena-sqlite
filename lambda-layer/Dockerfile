FROM lambci/lambda:build-python3.7

ENV PYTHON_VERSION=3.7.5
RUN curl -LO https://github.com/rogerbinns/apsw/releases/download/3.30.1-r1/apsw-3.30.1-r1.zip
RUN unzip apsw-3.30.1-r1.zip 
WORKDIR apsw-3.30.1-r1
RUN python setup.py fetch --all build --enable-all-extensions install
# set workdir back
WORKDIR /var/task
