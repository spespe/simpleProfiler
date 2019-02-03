#!/bin/bash
# CREATED BY: PIETRO SPERI

REMOTE_HOST=${1}
USER=`whoami`

KEY=`ssh-keygen`
COPY=`ssh-copy-id -i ~/.ssh/id_rsa.pub ${REMOTE_HOST}`

echo "STARTING THE SSH NOW, PLEASE PRESS ENTER, YES AND YOUR PASSWORD WHENEVER REQUIRED"

ssh ${USER}@${REMOTE_HOST} << EOF
  ls -altr
  echo "WORKING PERFECTLY"
  exit
EOF

echo -e "FINISHED CORRECTLY!"
