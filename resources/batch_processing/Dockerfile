FROM python:3.8-slim

RUN apt-get update && apt-get -y install cron

# Add crontab file in the cron directory
ADD crontab /etc/cron.d/hello-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/hello-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Install your Python dependencies
RUN pip install -r /batch_processing/requirements.txt

# Copy your script into the container
COPY script.py /batch_processing/script.py

CMD ["cron", "-f"]
