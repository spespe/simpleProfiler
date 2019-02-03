#!/bin/bash
REMOTE_HOST=${1}
PATTERN=${2}
USER=`whoami`

KEY=`ssh-keygen`
COPY=`ssh-copy-id -i ~/.ssh/id_rsa.pub ${REMOTE_HOST}`

echo "STARTING THE SSH NOW, PLEASE PRESS ENTER, YES AND YOUR PASSWORD WHENEVER REQUIRED"

ssh ${USER}@${REMOTE_HOST} << EOF
  netstat -nl | grep -c ${PATTERN} > pattern_check_${REMOTE_HOST}.txt
  exit
EOF

echo -e "FINISHED CORRECTLY!"
