#!/usr/bin/env bash

BACKUP_MONITOR_HOME=${HOME}/backup_monitor
mkdir -p ${BACKUP_MONITOR_HOME}/logs

BACKUP_MONITOR_CONF=${BACKUP_MONITOR_HOME}/conf
mkdir -p ${BACKUP_MONITOR_CONF}
cp conf/backup_monitor.json ${BACKUP_MONITOR_CONF}

sudo cp src/backup_monitor/backup_monitor.py /usr/bin/backup_monitor.py
