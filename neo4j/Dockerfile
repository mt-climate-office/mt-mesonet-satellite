FROM neo4j:4.4.8-community

RUN apt-get update && \
    apt-get -y install cron && \
    apt-get -y install curl


COPY ./backup.sh /var/lib/neo4j/
COPY entrypoint.sh /entrypoint.sh

RUN mkdir -p /dumps/neo4j
RUN touch /var/log/cron.log
RUN curl https://raw.githubusercontent.com/neo4j/docker-neo4j/master/docker-image-src/4.4/docker-entrypoint.sh -o /neo4j-entrypoint.sh

# Give execution rights on the cron job
RUN chmod +x /var/lib/neo4j/backup.sh
RUN chmod +x /entrypoint.sh
RUN chmod +x /neo4j-entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]