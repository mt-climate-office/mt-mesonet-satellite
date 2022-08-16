FROM python:3.9-slim
RUN apt-get update && \
    apt-get -y install curl && \
    apt-get -y install cron && \
    && apt-get -y install git

# Set working directory
WORKDIR /setup

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

# Copy necessary files
COPY ../pyproject.toml ../poetry.lock* /setup/
COPY ../mt_mesonet_satellite/ /setup/
COPY ../processing/ /setup/

# Install packages.
RUN poetry install --no-root --no-dev

# Copy cronjob to container
COPY ./cronjob /etc/cron.d/cronjob

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cronjob

# Make update executable
RUN chmod 0744 /setup/processing/update.py

# Apply cron job
RUN crontab /etc/cron.d/cronjob

# Create the log file to be able to run tail
RUN touch /var/log/cron.log
RUN touch /setup/info.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log