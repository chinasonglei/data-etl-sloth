#!/usr/bin/env bash
# owner 张建新


PASS=root2758
CP2HOST=172.17.0.195


scp_jar=$(expect -c "
set timeout 10
spawn scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  target/metadata-0.0.1-SNAPSHOT.jar root@${CP2HOST}:/home/application
expect \"*password:\"
send \"${PASS}\r\"
send \"exit \r\"
expect eof
")

echo "$scp_jar"

scp_sh=$(expect -c "
set timeout 10
spawn scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  run.sh root@${CP2HOST}:/home/application
expect \"*password:\"
send \"${PASS}\r\"
send \"exit \r\"
expect eof
")

echo "$scp_sh"
