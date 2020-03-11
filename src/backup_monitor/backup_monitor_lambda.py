import json
import os
import subprocess


def lambda_handler(event, context):
    # TODO implement


    path = '/var/task'

    process = subprocess.run(['/var/task/bin/rclone', 'size', 'onedrive-johnl:/Scans'],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    # TODO better checking of result
    stdout, stderr = process.communicate()
    out = [str(stdout), str(stderr)]

    return {
        'statusCode': 200,
        'body': json.dumps(f'Here is the output: {out}')
    }


def walk_dir(path):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            files.append(os.path.join(r, file))

    file_list = []
    for f in files:
        file_list.append(str(f))
    return file_list
