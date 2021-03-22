FROM prefecthq/prefect:latest-python3.8



RUN apt-get update \
    && apt-get install -y unzip 


ENV PYTHONPATH "$PYTHONPATH:/flow_base/third_party/python/:/flow_base/"
COPY /standard_deps_bin.pex /standard_deps_bin.pex
RUN mkdir flow_base
RUN unzip /standard_deps_bin.pex -d /flow_base/
RUN rm /standard_deps_bin.pex

ADD .sgconfig /var/splitgraph/.sgconfig