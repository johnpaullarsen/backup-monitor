#!/usr/bin/env python3

import json
import os
import logging
import subprocess
import tempfile
import boto3

from datetime import datetime, timezone
from copy import deepcopy
from logging.handlers import RotatingFileHandler

'''
    Test the sync, backup and restore of cycle of a cloud storage on a (possibly remote) computer.
    Creates a canary file on the cloud storage. Expects the computer to sync it down, back it up, restore
    it to a new location, then sync the restored file up to the cloud. Next time this program is invoked it
    looks for the restored file and calculates the time between the present and the time the restored file was
    generated. Puts this value into cloudwatch as a metric named "RestoreLag" so that it can be monitored and
    potentially raise an alarm if the RestoreLag exceeds a value.
    Requires rclone to be installed with a configuration named {storage}-{user} for each storage system
'''


# A directory with this name will be created on the remote cloud storage, canary files used for monitoring will be
# stored under this directory
BACKUP_REMOTE_BASE = "backup-monitor"

CANARY_FILENAME = "canary.json"
CLOUDWATCH_NAMESPACE = "CloudberryBackup"


class Canary:

    def __init__(self) -> None:
        super().__init__()
        self.storage = None
        self.rclone_remote = None
        self.computer = None
        self.timestamp = None
        self.user = None
        self.num_objects = 0
        self.total_bytes = 0


class CanaryEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Canary):
            result = deepcopy(obj.__dict__)
            result['_type'] = 'Canary'
            result['timestamp'] = datetime.isoformat(obj.timestamp)
            return result
        return super(CanaryEncoder, self).default(obj)


class CanaryDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        if '_type' not in obj:
            return obj
        type = obj['_type']
        if type == 'Canary':
            result = Canary()
            result.storage = obj['storage']
            result.rclone_remote = obj['rclone_remote']
            result.computer = obj['computer']
            result.timestamp = datetime.fromisoformat(obj['timestamp'])
            result.user = obj['user']
            result.num_objects = obj['num_objects']
            result.total_bytes = obj['total_bytes']
            return result
        return obj


class CloudwatchMetric:

    def __init__(self, name, value, unit) -> None:
        super().__init__()
        self.name = name
        self.value = value
        self.unit = unit


class BackupMonitor:

    def __init__(self, computer, storage, user) -> None:
        super().__init__()
        self.computer = computer
        self.storage = storage
        self.user = user
        self.rclone_remote = f'{storage}-{user}'

    def get_remote_working_path(self):
        return f"{self.rclone_remote}:/{BACKUP_REMOTE_BASE}/{self.computer}/{self.rclone_remote}"

    def create_canary(self):
        canary = Canary()
        canary.computer = self.computer
        canary.storage = self.storage
        canary.user = self.user
        canary.rclone_remote = self.rclone_remote
        canary.timestamp = datetime.now(timezone.utc)
        canary.num_objects, canary.total_bytes = self.get_remote_size()
        return canary

    def generate_canary_file(self, tempdir):
        """
        Generate a Canary object, and generate a json file from it. Copy the file to the generated directory on the
        remote cloud storage. The monitored computer will sync this file to its disk, then back it up, then restore
        it to the restored directory so we can check it.
        :param tempdir: The tempdir to generate the canary file into befory copying to remote storage
        :return: The Canary object that was generated
        """
        canary = self.create_canary()
        os.mkdir(f"{tempdir}/generated")
        local_temp_file = f"{tempdir}/generated/{CANARY_FILENAME}"
        remote_generated_path = f"{self.get_remote_working_path()}/generated"
        with open(local_temp_file, 'w') as generated_canary_file:
            logging.getLogger().info("Writing generated canary to %s", local_temp_file)
            json.dump(canary, generated_canary_file, cls=CanaryEncoder, indent=2)
            generated_canary_file.flush()
            # Use rclone to put the generated file up on the remote
            logging.getLogger().info("rclone copy generated canary %s to %s", local_temp_file, remote_generated_path)
            process = subprocess.Popen(['rclone', 'copy', local_temp_file, remote_generated_path],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            # TODO better checking of result
            stdout, stderr = process.communicate()
        return canary

    def load_restored_canary_file(self, tempdir):
        """
        The backup restore on the monitored computer should have restored the generated canary file into the
        restored directory of its local storage. Then it should have been synced back to the restored directory of the
        remote cloud storage. Copy it to the temp directory from the remote storage and parse it into a Canary object.
        :param tempdir: The tempdir to put the copy of the restored canary file
        :return: The Canary object parsed from the restored canary file
        """
        local_temp_restored_dir = f"{tempdir}/restored"
        local_temp_restored_file = f"{local_temp_restored_dir}/{CANARY_FILENAME}"
        remote_restored_file = f"{self.get_remote_working_path()}/restored/{CANARY_FILENAME}"
        # Use rclone to copy the restored canary file back from the remote
        logging.getLogger().info("rclone copy restored canary %s to %s", remote_restored_file, local_temp_restored_dir)
        process = subprocess.Popen(['rclone', 'copy', remote_restored_file, local_temp_restored_dir],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        with open(local_temp_restored_file, 'r') as restored_canary_file:
            logging.getLogger().info("Parsing restored canary from %s", local_temp_restored_file)
            restored_canary = json.load(restored_canary_file, cls=CanaryDecoder)
        return restored_canary

    def get_remote_size(self):
        """
        Use rclone to find the number of objects stored on the remote and their total size
        """
        process = subprocess.Popen(['rclone', 'size', f'{self.rclone_remote}:', '--json'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        response = json.loads(stdout)
        return response['count'], response['bytes']

    def put_cloudwatch_metrics(self, metrics):
        """
        Put a list of CloudwatchMetric objects into cloudwatch using dimensions representing this storage
        :param metrics: The list of metrics to put into cloudwatch
        """
        client = boto3.client("cloudwatch")
        for metric in metrics:
            logging.getLogger().info("Putting cloudwatch metric %s %s %s %s %s %s",
                                     self.computer,
                                     self.storage,
                                     self.user,
                                     metric.name,
                                     metric.value,
                                     metric.unit)
            response = client.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=[
                    {
                        "MetricName": metric.name,
                        "Dimensions": [
                            {
                                "Name": "Computer",
                                "Value": self.computer,
                            },
                            {
                                "Name": "Storage",
                                "Value": self.storage,
                            },
                            {
                                "Name": "StorageUser",
                                "Value": self.user,
                            }
                        ],
                        "Value":  metric.value,
                        "Unit": metric.unit
                    }
                ]
            )
            logging.getLogger().debug("Cloudwatch put metric response: %s", response)

    def monitor(self):
        logging.getLogger().info("Monitoring %s %s %s", self.computer, self.storage, self.user)
        with tempfile.TemporaryDirectory() as temp_dir:
            generated_canary = self.generate_canary_file(temp_dir)
            restored_canary = self.load_restored_canary_file(temp_dir)
            restore_lag = generated_canary.timestamp - restored_canary.timestamp
            self.put_cloudwatch_metrics([
                CloudwatchMetric("RestoreLag", restore_lag.total_seconds(), "Seconds"),
                CloudwatchMetric("FileCount", generated_canary.num_objects, "Count"),
                CloudwatchMetric("TotalBytes", generated_canary.total_bytes, "Bytes")
            ])


def create_rotating_log(log_dir, log_file_name):
    """
    Creates a rotating log
    """
    os.makedirs(log_dir, mode=0o700, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # add a rotating handler
    handler = RotatingFileHandler(f"{log_dir}/{log_file_name}", maxBytes=1000000, backupCount=5)
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def monitor(config_file_path):
    try:
        with open(config_file_path, 'r') as config_file:
            configs = json.load(config_file)
            for config in configs:
                backup_monitor = BackupMonitor(config['computer'], config['storage'], config['user'])
                backup_monitor.monitor()

    except Exception as e:
        logging.getLogger().exception("Failed with exception")
        raise e


def main():
    home = os.getenv('HOME')
    backup_monitor_home = os.getenv('BACKUP_MONITOR_HOME', f"{home}/backup_monitor")
    log_dir = f"{backup_monitor_home}/logs"
    conf_file = f"{backup_monitor_home}/conf/backup_monitor.json"
    create_rotating_log(log_dir, "backup_monitor.log")
    monitor(conf_file)


if __name__ == '__main__':
    main()
