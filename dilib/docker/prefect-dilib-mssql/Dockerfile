FROM prefecthq/prefect:latest-python3.8


RUN apt-get update \
    && apt-get install -y apt-transport-https ca-certificates curl gnupg2 lsb-release git unzip \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add - 2>/dev/null \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    # Install multiarch support
    && curl http://ftp.de.debian.org/debian/pool/main/g/glibc/multiarch-support_2.28-10+deb10u1_amd64.deb -o /tmp/multiarch-support.deb\
    && apt-get install /tmp/multiarch-support.deb \
    && apt-get update \
    # Install odbc drivers
    && apt-get install -y g++ unixodbc-dev \
    && ACCEPT_EULA=Y apt-get install msodbcsql17

ENV PYTHONPATH "$PYTHONPATH:/flow_base/third_party/python/:/flow_base/"
COPY /mssql_deps_bin.pex /mssql_deps_bin.pex
RUN mkdir flow_base
RUN unzip /mssql_deps_bin.pex -d /flow_base/
RUN rm /mssql_deps_bin.pex

ADD .sgconfig /var/splitgraph/.sgconfig
