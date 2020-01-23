#!

import json
import os
import subprocess
import argparse

from datetime import datetime, timezone





class Canary:

    def __init__(self, storage, remote, computer) -> None:
        super().__init__()
        self.computer = computer
        self.storage = storage
        self.remote = remote

    def get_remote_size(self, rsync_remote):
        """ Use rclone to find the number of objects stored on the remote and their total size """
        process = subprocess.Popen(['rclone', 'size', f'{rsync_remote}:', '--json'],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return json.loads(stdout)

    def generate(self):
        size_result = self.get_remote_size(self.remote)
        data = dict()
        data['timestamp'] = datetime.isoformat(datetime.now(timezone.utc))
        data['computer'] = self.computer
        data['storage'] = self.storage
        data['user'] = os.environ['USER']
        data['numObjects'] = size_result['count']
        data['totalBytes'] = size_result['bytes']
        print(json.dumps(data, indent=2))


def main():
    parser = argparse.ArgumentParser(description='Produce a json \'canary\' file for testing cloud backups')
    parser.add_argument('storage', metavar='storage_provider', help='Cloud storage provider e.g dropbox')
    parser.add_argument('remote', metavar='rclone_remote_name', help='The name of the rclone remote e.g dropbox-johnl')
    parser.add_argument('computer', metavar='computer', help='The name for the computer within the backup system')
    args = parser.parse_args()
    canary = Canary(args.storage, args.remote, args.computer)
    canary.generate()


if __name__ == '__main__':
    main()

