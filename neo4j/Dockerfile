FROM neo4j:4.4.8-community

RUN apt-get update && \
    apt-get -y install cron 

# COPY ./cronjob /etc/cron.d/cronjob
COPY ./backup.sh /opt/backup.sh
COPY ./test.sh /opt/test.sh

RUN mkdir -p /dumps/neo4j

# Give execution rights on the cron job
# RUN chmod 0644 /etc/cron.d/cronjob
RUN chmod u+x /opt/backup.sh
RUN chmod u+x /opt/test.sh

# Apply cron job
# RUN crontab /etc/cron.d/cronjob
RUN env >> ~/env.log
RUN echo 'PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' > /etc/crontab
RUN echo 'SHELL=/bin/bash' >> /etc/crontab
RUN echo '40 21 * * * root bash -c "source $HOME/env.log; source /opt/backup.sh"' >> /etc/crontab
RUN echo '* * * * * root bash -c "source $HOME/env.log; source /opt/test.sh"' >> /etc/crontab

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

CMD tail -f /var/log/cron.log