FROM neo4j:4.4.8-community

RUN apt-get update && \
    apt-get -y install cron 

COPY ./backup.sh /var/lib/neo4j/
COPY entrypoint.sh /entrypoint.sh

RUN mkdir -p /dumps/neo4j

# Give execution rights on the cron job
RUN chmod 0744 /var/lib/neo4j/backup.sh

# RUN service cron start

ENTRYPOINT [ "/entrypoint.sh" ]