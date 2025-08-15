# Base image: ubuntu:22.04
FROM ubuntu:22.04

# ARGs
# https://docs.docker.com/engine/reference/builder/#understand-how-arg-and-from-interact
ARG TARGETPLATFORM=linux/amd64,linux/arm64
ARG DEBIAN_FRONTEND=noninteractive

# neo4j 5.5.0 installation and some cleanup
RUN apt-get update && \
    apt-get install -y wget gnupg software-properties-common git && \
    wget -O - https://debian.neo4j.com/neotechnology.gpg.key | apt-key add - && \
    echo 'deb https://debian.neo4j.com stable latest' > /etc/apt/sources.list.d/neo4j.list && \
    add-apt-repository universe && \
    apt-get update && \
    apt-get install -y nano unzip neo4j python3-pip && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


# Upgrading pip + install libraries needed: neo4j, pandas, pyarrow
RUN pip3 install --upgrade pip
RUN pip3 install neo4j pandas pyarrow

# Switch to /cse511 as the WORKDIR
RUN mkdir -p /cse511
WORKDIR /cse511

# Configure neo4j for remote access + GDS plugin
RUN echo "server.default_listen_address=0.0.0.0" >> /etc/neo4j/neo4j.conf && \
    echo "server.default_advertised_address=localhost" >> /etc/neo4j/neo4j.conf && \
    echo "dbms.security.auth_enabled=true" >> /etc/neo4j/neo4j.conf

# Downloading & unzip GDS v2.15.0 into /var/lib/neo4j/plugins
RUN wget https://graphdatascience.ninja/neo4j-graph-data-science-2.15.0.zip -O /tmp/gds.zip && \
    unzip /tmp/gds.zip -d /var/lib/neo4j/plugins/ && \
    rm /tmp/gds.zip

# Allow the GDS plugin to run
RUN echo "dbms.security.procedures.unrestricted=gds.*" >> /etc/neo4j/neo4j.conf

# Download the .parquet file INTO /cse511
RUN wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet

# Cloning GitHub repo into /cse511/repo and copying data_loader.py here 
RUN git clone https://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@github.com/SP-2025-CSE511-Data-Processing-at-Scale/Project-1-mchoud26.git /cse511/repo
RUN cp /cse511/repo/data_loader.py /cse511/data_loader.py


# Run the data loader script
RUN chmod +x data_loader.py && \
    neo4j start && \
    sleep 10 && \
    neo4j-admin dbms set-initial-password project1phase1 && \
    neo4j stop

RUN neo4j start && \
    sleep 10 && \
    python3 data_loader.py && \
    neo4j stop

# Expose neo4j ports
EXPOSE 7474 7687

# Copying interface.py and tester.py into the container
COPY interface.py /interface.py
COPY tester.py /tester.py

# Install 'requests' so that tester.py can do HTTP calls
RUN pip3 install requests

# Running tester.py (which in turn calls interface.py).
RUN neo4j start && \
    sleep 10 && \
    python3 /tester.py && \
    neo4j stop


# Start neo4j service and show the logs on container run
CMD ["/bin/bash", "-c", "neo4j start && tail -f /dev/null"]
