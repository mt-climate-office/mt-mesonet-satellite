#!/bin/sh
env >> ~/env.log

echo 'PATH=/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin' > /etc/crontab
echo 'SHELL=/bin/bash' >> /etc/crontab

echo '38 19 * * * root bash -c "source $HOME/env.log; source /var/lib/neo4j/backup.sh"' >> /etc/crontab
echo '* * * * *  echo "Hello $(date)" >> /var/log/cron.log 2>&1' >> /etc/crontab

service cron start
neo4j start
tail -f /dev/null