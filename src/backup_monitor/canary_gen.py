#!/usr/bin/env python3

import json
import os
import subprocess
import argparse
import tempfile

from datetime import datetime, timezone
from copy import deepcopy

# A directory with this name will be created on the remote cloud storage, canary files used for monitoring will be
# stored under this directory
BACKUP_REMOTE_BASE = "backup-monitor"
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

    def get_remote_working_path(self):
        return f"{self.rclone_remote}:/{BACKUP_REMOTE_BASE}/{self.computer}/{self.rclone_remote}"

    def create_canary(self):
        canary = Canary()
        canary.storage = self.storage
        canary.rclone_remote = self.rclone_remote
        canary.computer = self.computer
        canary.timestamp = datetime.now(timezone.utc)
        canary.user = self.user
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
            json.dump(canary, generated_canary_file, cls=CanaryEncoder, indent=2)
            generated_canary_file.flush()
            # Use rclone to put the generated file up on the remote
            process = subprocess.Popen(['rclone', 'copy', local_temp_file, remote_generated_path],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
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
        local_temp_file = f"{tempdir}/restored/{CANARY_FILENAME}"
        remote_restored_file = f"{self.get_remote_working_path()}/restored/{CANARY_FILENAME}"
        # Use rclone to copy the restored canary file back from the remote
        process = subprocess.Popen(['rclone', 'copy', remote_restored_file, local_temp_file],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        with open(local_temp_file, 'r') as restored_canary_file:
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

    def monitor(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            generated_canary = self.generate_canary_file(temp_dir)
            restored_canary = self.load_restored_canary_file(temp_dir)
            restore_lag = generated_canary.timestamp - restored_canary.timestamp
            print(f"Restore lag: {restore_lag}")


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
