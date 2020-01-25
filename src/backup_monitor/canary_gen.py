#!/usr/bin/env python3

import json
import os
import subprocess
import argparse

from datetime import datetime, timezone
from copy import deepcopy

BACKUP_MONITOR_HOME = f"{os.environ['HOME']}/backup_monitor"
CANARY_FILENAME = "canary.json"


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


class BackupMonitor:

    def __init__(self, storage, rclone_remote, computer, user) -> None:
        super().__init__()
        self.storage = storage
        self.rclone_remote = rclone_remote
        self.computer = computer
        self.user = user

    def create_canary(self):
        canary = Canary()
        canary.storage = self.storage
        canary.rclone_remote = self.rclone_remote
        canary.computer = self.computer
        canary.timestamp = datetime.now(timezone.utc)
        canary.user = self.user
        canary.num_objects, canary.total_bytes = self.get_remote_size()
        return canary

    def generate_canary_file(self):
        canary = self.create_canary()
        generate_file_path = f"{BACKUP_MONITOR_HOME}/{self.rclone_remote}/generated/{CANARY_FILENAME}"
        with open(generate_file_path, 'w') as generated_canary_file:
            json.dump(canary, generated_canary_file, cls=CanaryEncoder, indent=2)
        return canary

    def load_restored_canary_file(self):
        restored_file_path = f"{BACKUP_MONITOR_HOME}/{self.rclone_remote}/restored/{CANARY_FILENAME}"
        with open(restored_file_path, 'r') as restored_canary_file:
            restored_canary = json.load(restored_canary_file, cls=CanaryDecoder)
            print(restored_canary)
        return restored_canary

    def get_remote_size(self):
        """ Use rclone to find the number of objects stored on the remote and their total size """
        process = subprocess.Popen(['rclone', 'size', f'{self.rclone_remote}:', '--json'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        response = json.loads(stdout)
        return response['count'], response['bytes']

    def monitor(self):
        generated_canary = self.generate_canary_file()
        restored_canary = self.load_restored_canary_file()
        restore_lag = generated_canary.timestamp - restored_canary.timestamp


def main():
    parser = argparse.ArgumentParser(description='Produce a json \'canary\' file for testing cloud backups')
    parser.add_argument('storage', metavar='storage_provider', help='Cloud storage provider e.g dropbox')
    parser.add_argument('remote', metavar='rclone_remote_name', help='The name of the rclone remote e.g dropbox-johnl')
    parser.add_argument('computer', metavar='computer', help='The name for the computer within the backup system')
    args = parser.parse_args()
    backup_monitor = BackupMonitor(args.storage, args.remote, args.computer, os.environ['USER'])
    backup_monitor.monitor()


if __name__ == '__main__':
    main()

