FROM python:3.9-slim
RUN apt-get update && \
    apt-get -y install curl && \
    apt-get -y install cron && \
    apt-get -y install python3-pip && \
    apt-get -y install git


WORKDIR /setup
RUN pip install git+https://github.com/mt-climate-office/mt-mesonet-satellite.git@main

COPY ./processing/cronjob /etc/cron.d/cronjob
COPY ./processing/update.py /setup/
 
# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cronjob

# Make update executable
RUN chmod 0744 /setup/update.py

# Apply cron job
RUN crontab /etc/cron.d/cronjob
 
# Create the log file to be able to run tail
RUN touch /var/log/cron.log
RUN touch /setup/log.txt

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log