FROM python:3.9-slim
RUN apt-get update && \
    apt-get -y install cron && \
    apt-get -y install python3-pip

WORKDIR /setup

COPY ./processing/cronjob /etc/cron.d/cronjob

COPY . /setup/
RUN pip install .

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cronjob

# Make update executable
RUN chmod 0744 /setup/processing/update.py

# Apply cron job
RUN crontab /etc/cron.d/cronjob
 
# Create the log file to be able to run tail
RUN touch /var/log/cron.log
RUN touch /setup/log.txt

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log