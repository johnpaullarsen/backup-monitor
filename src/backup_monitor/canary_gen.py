#!

import json
import os
import subprocess
import argparse

from datetime import datetime, timezone


class Canary:

    def __init__(self, storage, remote, computer) -> None:
        super().__init__()
        self.storage = storage
        self.remote = remote
        self.computer = computer
        self.timestamp = None
        self.user = None
        self.num_objects = 0
        self.total_bytes = 0


class BackupMonitor:

    def __init__(self, storage, rclone_remote, computer) -> None:
        super().__init__()
        self.storage = storage
        self.rclone_remote = rclone_remote
        self.computer = computer
        self.user = os.environ['USER']

    def create_canary(self):
        canary = Canary(self.storage, self.rclone_remote, self.computer)
        canary.timestamp = datetime.isoformat(datetime.now(timezone.utc))
        canary.user = self.user
        canary.num_objects, canary.total_bytes = self.get_remote_size()
        return canary

    def get_remote_size(self):
        """ Use rclone to find the number of objects stored on the remote and their total size """
        process = subprocess.Popen(['rclone', 'size', f'{self.rclone_remote}:', '--json'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        response = json.loads(stdout)
        return response['count'], response['bytes']

    def monitor(self):
        canary = self.create_canary()
        print(json.dumps(canary.__dict__, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Produce a json \'canary\' file for testing cloud backups')
    parser.add_argument('storage', metavar='storage_provider', help='Cloud storage provider e.g dropbox')
    parser.add_argument('remote', metavar='rclone_remote_name', help='The name of the rclone remote e.g dropbox-johnl')
    parser.add_argument('computer', metavar='computer', help='The name for the computer within the backup system')
    args = parser.parse_args()
    backup_monitor = BackupMonitor(args.storage, args.remote, args.computer)
    backup_monitor.monitor()


if __name__ == '__main__':
    main()

