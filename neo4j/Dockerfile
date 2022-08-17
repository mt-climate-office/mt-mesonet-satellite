FROM neo4j:4.4.8-community

RUN apt-get update && \
    apt-get -y install cron 

COPY ./cronjob /etc/cron.d/cronjob
COPY ./backup.sh /var/lib/neo4j/

RUN mkdir -p /dumps/neo4j

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cronjob
RUN chmod 0744 /var/lib/neo4j/backup.sh

# Apply cron job
RUN crontab /etc/cron.d/cronjob

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

CMD cron && tail -f /var/log/cron.log