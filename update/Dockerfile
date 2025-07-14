FROM python:3.9-slim
RUN apt-get update && \
    apt-get -y install curl && \
    apt-get -y install cron && \
    apt-get -y install git

# Set working directory
WORKDIR /setup

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry python3 && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

# Copy necessary files
COPY ./pyproject.toml ./poetry.lock* ./.env /setup/
COPY ./mt_mesonet_satellite /setup/mt_mesonet_satellite

# Install packages.
RUN poetry install --no-root
RUN pip install .

# Copy update scripts
COPY ./update /setup/update

# Copy cronjob to container
RUN cp /setup/update/cronjob /etc/cron.d/cronjob

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cronjob

# Make update executable
RUN chmod 0744 /setup/update/update.py
RUN chmod 0744 /setup/update/backfill.py

# Apply cron job
RUN crontab /etc/cron.d/cronjob

# Create the log file to be able to run tail
RUN touch /var/log/cron.log
RUN touch /setup/info.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log