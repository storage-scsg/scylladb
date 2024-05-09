# Copyright 2024-present cmss - ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for synctable operations - Synctable.

import boto3
import pytest
import random
from botocore.exceptions import ClientError, HTTPClientError

import sys
sys.path.insert(1, sys.path[0] + '/../alternator')
sys.path.insert(1, sys.path[0] + '/../rest_api')
from util import random_string, full_scan, full_query, multiset, scylla_inject_error
from rest_util import set_tmp_task_ttl
import urllib3
import traceback
import sys
from conftest import new_dynamodb_session
import requests
import json
import re

def get_current_url(request):
    if request.config.getoption('url') != None:
        url = request.config.getoption('url')
    else:
        url = 'https://localhost:8043' if request.config.getoption('https') else 'http://localhost:8000'
    return url

module_name = "repair"

def list_tasks(rest_api_wrap, module_name, internal=False, keyspace=None, table=None):
    args = { "internal": internal }
    if keyspace:
        args["keyspace"] = keyspace
    if table:
        args["table"] = table
    resp = rest_api_wrap.send("GET", f"task_manager/list_module_tasks/{module_name}", args)
    resp.raise_for_status()
    return resp.json()

def drain_module_tasks(rest_api_wrap, module_name):
    tasks = [task for task in list_tasks(rest_api_wrap, module_name, True)]
    for task in tasks:
        resp = rest_api_wrap.send("GET", f"task_manager/wait_task/{task['task_id']}")
        # The task may be already unregistered.
        assert resp.status_code == requests.codes.ok or resp.status_code == requests.codes.bad_request, "Invalid status code"

def run_repair_and_wait_synctable(rest_api_wrap, keyspace):
    resp = rest_api_wrap.send("POST", f"storage_service/repair_async/{keyspace}", {'primaryRange' : 'true'})
    resp.raise_for_status()
    sequence_number = resp.json()
    resp = rest_api_wrap.send("GET", f"storage_service/repair_status", { "id": sequence_number })
    resp.raise_for_status()
    return sequence_number

def get_task_status(rest_api_wrap, task_id):
    resp = rest_api_wrap.send("GET", f"task_manager/task_status/{task_id}")
    resp.raise_for_status()
    return resp.json()

def test_synctable_repair_task(synctable_test_table1, synctable_test_table2, rest_api_wrap_list, request):
    # embed some data.
    count = 1000
    with synctable_test_table1.batch_writer() as batch:
        for i in range(count):
            batch.put_item(Item={
                'obj': "{}".format(i),
                'bi': "obj_meta_153e6449-ec57-4819-ab20-93879c2dbb74bucket0317%.440505.1.{}".format(i),
                'attribute': str(i),
                'another': 'xyz'
            })

    for rest_api_peer in rest_api_wrap_list:
        drain_module_tasks(rest_api_peer, module_name)
        with set_tmp_task_ttl(rest_api_peer, 1000000000):
            # Insert some data.
            keyspace = 'alternator_' + synctable_test_table1.name

            # config synctable param
            dest_url = get_current_url(request)
            dest_url = dest_url.split('//')[1]

            # {trace -> false}, {jobThreads -> 1}, {incremental -> false}, {parallelism -> parallel}
            resp = rest_api_peer.send("POST", f"storage_service/repair_async/{keyspace}", {'primaryRange' : 'true', 'trace' : 'false', 'jobThreads' : '1', 'incremental' : 'false', 'parallelism' : 'parallel', 'target_table': synctable_test_table2.name, 'ips': dest_url, 'projection': 'obj,bi', 'column_alter': 'bi', 'column_alter_method': 'replace_shard_id_to_zero'})
            resp.raise_for_status()
            sequence_number = resp.json()
            resp = rest_api_peer.send("GET", f"storage_service/repair_status", { "id": sequence_number })
            resp.raise_for_status()

            # Get all repairs.
            statuses = [get_task_status(rest_api_peer, task["task_id"]) for task in list_tasks(rest_api_peer, "repair") if task["sequence_number"] == sequence_number]
            assert len(statuses) == 1, "Wrong number of internal repair tasks"
            status = statuses[0]
            assert status["progress_completed"] == status["progress_total"], "Incorrect task progress"

            assert "children_ids" in status, "Shard tasks weren't created"
            children = [get_task_status(rest_api_peer, child_id) for child_id in status["children_ids"]]
            assert all([child["progress_completed"] == child["progress_total"] for child in children]), "Some shard tasks have incorrect progress"

            assert sum([child["progress_total"] for child in children]) == status["progress_total"], "Total progress of parent is not equal to children total progress sum"
            assert sum([child["progress_completed"] for child in children]) == status["progress_completed"], "Completed progress of parent is not equal to children completed progress sum"
        drain_module_tasks(rest_api_peer, module_name)

    # validate synctable_test_table2 data num.
    pos = None
    got_items = []
    while True:
        response = synctable_test_table2.scan(ExclusiveStartKey=pos, ConsistentRead=True) if pos else synctable_test_table2.scan(ConsistentRead=True)
        pos = response.get('LastEvaluatedKey', None)
        got_items += response['Items']
        if not pos:
            break
    assert count == len(got_items)