#     Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#     Licensed under the Apache License, Version 2.0 (the "License").
#     You may not use this file except in compliance with the License.
#     A copy of the License is located at
#
#         https://aws.amazon.com/apache-2-0/
#
#     or in the "license" file accompanying this file. This file is distributed
#     on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#     express or implied. See the License for the specific language governing
#     permissions and limitations under the License.

import getopt
import json
import sys
import time
from datetime import datetime

import boto3
import requests
import urllib3

log_token = None
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

with open('/opt/ml/metadata/resource-metadata.json') as logs:
    nb_name = json.load(logs)['ResourceName']

LOG_GROUP_NAME = '/aws/sagemaker/NotebookInstances'
STREAM_NAME = '{nb_name}/LifecycleConfigOnStart'.format(nb_name=nb_name)
LOGS_CLIENT = None


def get_stream(resp):
    for stream in resp['logStreams']:
        if 'LifecycleConfigOnStart' in stream['logStreamName']:
            return stream
    raise Exception('no stream')


def _get_client():
    global LOGS_CLIENT
    if LOGS_CLIENT is None:
        LOGS_CLIENT = boto3.client('logs')
    return LOGS_CLIENT


def _get_token():
    global log_token
    if log_token is None:
        client = _get_client()
        resp = client.describe_log_streams(logGroupName=LOG_GROUP_NAME)
        stream = get_stream(resp)
        log_token = stream['uploadSequenceToken']
    return log_token


def _set_token(resp):
    global log_token
    log_token = resp['nextSequenceToken']


def log(msg):
    global log_token
    timestamp = int(round(time.time() * 1000))

    token = _get_token()
    client = _get_client()
    resp = client.put_log_events(
        logGroupName=LOG_GROUP_NAME,
        logStreamName=STREAM_NAME,
        logEvents=[
            {'timestamp': timestamp, 'message': msg},
        ],
        sequenceToken=token,
    )
    _set_token(resp)


# Usage
usageInfo = """Usage:
This scripts checks if a notebook is idle for X seconds if it does, it'll stop the notebook:
python autostop.py --time <time_in_seconds> [--port <jupyter_port>] [--ignore-connections]
Type "python autostop.py -h" for available options.
"""
# Help info
helpInfo = """-t, --time
    Auto stop time in seconds
-p, --port
    jupyter port
-c --ignore-connections
    Stop notebook once idle, ignore connected users
-h, --help
    Help information
"""

# Read in command-line parameters
port = '8443'
ignore_connections = False
max_idle_duration = None
try:
    opts, args = getopt.getopt(sys.argv[1:], "ht:p:c", ["help", "time=", "port=", "ignore-connections"])
    if len(opts) == 0:
        raise getopt.GetoptError("No input parameters!")
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            log(helpInfo)
            exit(0)
        if opt in ("-t", "--time"):
            max_idle_duration = int(arg)
        if opt in ("-p", "--port"):
            port = str(arg)
        if opt in ("-c", "--ignore-connections"):
            ignore_connections = True
except getopt.GetoptError:
    log(usageInfo)
    exit(1)

# Missing configuration notification
if not max_idle_duration:
    log("Missing '-t' or '--time'")
    exit(2)


def is_notebook_idle(last_activity):
    last_activity = datetime.strptime(last_activity, "%Y-%m-%dT%H:%M:%S.%fz")
    return (datetime.now() - last_activity).total_seconds() > max_idle_duration


idle_label = {True: 'idle', False: 'active'}


def is_instance_idle(session):
    name = session['path']  # TODO?
    is_idle = False
    if session['kernel']['execution_state'] == 'idle':
        n_connections = session['kernel']['connections']
        if ignore_connections or n_connections == 0:
            is_idle = is_notebook_idle(session['kernel']['last_activity'])
    state = idle_label[is_idle]
    log('Notebook {name} is {state}'.format(name, state))
    return is_idle


def is_server_idle():
    response = requests.get('https://localhost:'+port+'/api/sessions', verify=False)
    sessions = response.json()
    is_idle = True
    n_sessions = len(sessions)
    if n_sessions > 0:
        n_idle = is_idle = sum(is_instance_idle(sess) for sess in sessions)
        log('{n_idle}/{n_nb} notebooks are idle'.format(n_idle=n_idle, n_nb=len(sessions)))
        is_idle = n_idle == len(sessions)
    else:
        # a notebook server with no active notebooks is considered idle
        log('No active notebooks.')
        is_idle = True
    state = idle_label[is_idle]
    log('Server is {state}'.format(state))
    return is_idle


if is_server_idle():
    client = boto3.client('sagemaker')
    client.stop_notebook_instance(
        NotebookInstanceName=nb_name,
    )
