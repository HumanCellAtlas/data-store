"""
See Readme.md in this directory for documentation on the dss-sync daemon.
"""

import os, sys, json, logging
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

import domovoi
from cloud_blobstore import BlobNotFoundError

from dcplib.s3_multipart import AWS_MIN_CHUNK_SIZE

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, Replica
from dss.util.aws import resources, clients
from dss.logging import configure_lambda_logging
from dss.events.handlers.sync import (compose_upload, initiate_multipart_upload, complete_multipart_upload, copy_part,
                                      exists, get_part_size, get_sync_work_state, parts_per_worker, dependencies_exist,
                                      do_oneshot_copy, sync_sfn_dep_wait_sleep_seconds, sync_sfn_num_threads)

configure_lambda_logging()
logger = logging.getLogger(__name__)
dss.Config.set_config(dss.BucketConfig.NORMAL)

app = domovoi.Domovoi()

# This entry point is for S3 native events forwarded through SQS.
@app.s3_event_handler(bucket=Config.get_s3_bucket(),
                      events=["s3:ObjectCreated:*"],
                      use_sqs=True,
                      sqs_queue_attributes=dict(VisibilityTimeout="920"))
def launch_from_s3_event(event, context):
    source_replica = Replica.aws
    executions = {}
    if event.get("Event") == "s3:TestEvent":
        logger.info("S3 test event received and processed successfully")
    else:
        for event_record in event["Records"]:
            bucket = resources.s3.Bucket(event_record["s3"]["bucket"]["name"])
            obj = bucket.Object(unquote(event_record["s3"]["object"]["key"]))
            if obj.key.startswith("cache"):
                logger.info("Ignoring cache object")
                continue
            if bucket.name != source_replica.bucket:
                logger.error("Received S3 event for bucket %s with no configured replica", bucket.name)
                continue

            for dest_replica in Config.get_replication_destinations(source_replica):
                if exists(dest_replica, obj.key):
                    # Logging error here causes daemons/invoke_lambda.sh to report failure, for some reason
                    # - Brian Hannafious, 2019-01-31
                    logger.info("Key %s already exists in %s, skipping sync", obj.key, dest_replica)
                    continue
                exec_name = bucket.name + "/" + obj.key + ":" + source_replica.name + ":" + dest_replica.name
                exec_input = dict(source_replica=source_replica.name,
                                  dest_replica=dest_replica.name,
                                  source_key=obj.key,
                                  source_obj_metadata=event_record["s3"]["object"])
                executions[exec_name] = app.state_machine.start_execution(**exec_input)["executionArn"]
    return executions

# This entry point is for external events forwarded by dss-gs-event-relay (or other event sources) through SNS-SQS.
@app.sqs_queue_subscriber("dss-sync-" + os.environ["DSS_DEPLOYMENT_STAGE"],
                          queue_attributes=dict(VisibilityTimeout="920"))
def launch_from_forwarded_event(event, context):
    executions = {}
    for event_record in event["Records"]:
        message = json.loads(json.loads(event_record["body"])["Message"])
        if message['resourceState'] == "not_exists":
            logger.info("Ignoring object deletion event")
        elif message["selfLink"].startswith("https://www.googleapis.com/storage"):
            source_replica = Replica.gcp
            source_key = message["name"]
            bucket = source_replica.bucket
            for dest_replica in Config.get_replication_destinations(source_replica):
                if exists(dest_replica, source_key):
                    logger.info("Key %s already exists in %s, skipping sync", source_key, dest_replica)
                    continue
                exec_name = bucket + "/" + message["name"] + ":" + source_replica.name + ":" + dest_replica.name
                exec_input = dict(source_replica=source_replica.name,
                                  dest_replica=dest_replica.name,
                                  source_key=message["name"],
                                  source_obj_metadata=message)
                executions[exec_name] = app.state_machine.start_execution(**exec_input)["executionArn"]
        else:
            raise NotImplementedError()
    return executions

# This entry point is for operator initiated replication
@app.sqs_queue_subscriber("dss-sync-operation-" + os.environ['DSS_DEPLOYMENT_STAGE'],
                          queue_attributes=dict(VisibilityTimeout="920"))
def launch_from_operator_queue(event, context):
    executions = {}
    for event_record in event['Records']:
        message = json.loads(event_record['body'])
        try:
            source_replica = Replica[message['source_replica']]
            dest_replica = Replica[message['dest_replica']]
            key = message['key']
            assert source_replica != dest_replica
        except (KeyError, AssertionError):
            logger.error("Inoperable operation sync message %s", message)
            continue
        bucket = source_replica.bucket
        if exists(dest_replica, key):
            logger.info("Key %s already exists in %s, skipping sync", key, dest_replica)
            continue
        try:
            size = Config.get_blobstore_handle(source_replica).get_size(bucket, key)
        except BlobNotFoundError:
            logger.error("Key %s does not exist on source replica %s", key, source_replica)
            continue
        exec_name = bucket + "/" + key + ":" + source_replica.name + ":" + dest_replica.name
        exec_input = dict(source_replica=source_replica.name,
                          dest_replica=dest_replica.name,
                          source_key=key,
                          source_obj_metadata=dict(size=size))
        executions[exec_name] = app.state_machine.start_execution(**exec_input)["executionArn"]
    return executions

retry_config = [
    {
        "ErrorEquals": ["States.TaskFailed"],
        "IntervalSeconds": 5,
        "MaxAttempts": 5,
        "BackoffRate": 1.5
    },
    {
        "ErrorEquals": ["States.Timeout"],
        "IntervalSeconds": 30,
        "MaxAttempts": 3,
        "BackoffRate": 1.5
    },
    {
        "ErrorEquals": ["States.Permissions"],
        "MaxAttempts": 0
    }
]

sfn = {
    "Comment": "DSS Sync Daemon state machine",
    "StartAt": "DispatchSync",
    "TimeoutSeconds": 60 * 60 * 2,
    "States": {
        "DispatchSync": {
            "Type": "Task",
            "Resource": None,  # This will be set by Domovoi to the Lambda ARN
            "Next": "WaitOrCopyOrQuit",
            "Retry": retry_config
        },
        "WaitOrCopyOrQuit": {
            "Type": "Choice",
            "Choices": [{
                "Variable": "$.sleep",
                "BooleanEquals": True,
                "Next": "WaitForDeps"
            }, {
                "Variable": "$.do_oneshot_copy",
                "BooleanEquals": True,
                "Next": "OneshotCopy"
            }, {
                "Variable": "$.do_multipart_copy",
                "BooleanEquals": True,
                "Next": "MultipartCopyThreadpool"
            }, {
                "Variable": "$.done",
                "BooleanEquals": True,
                "Next": "Quit"
            }],
            "Default": "CheckDeps"
        },
        "WaitForDeps": {
            "Type": "Wait",
            "SecondsPath": "$.sleep_seconds",
            "Next": "CheckDeps"
        },
        "CheckDeps": {
            "Type": "Task",
            "Resource": None,  # This will be set by Domovoi to the Lambda ARN
            "Next": "WaitOrCopyOrQuit",
            "Retry": retry_config
        },
        "OneshotCopy": {
            "Type": "Task",
            "Resource": None,  # This will be set by Domovoi to the Lambda ARN
            "End": True,
            "Retry": retry_config
        },
        "MultipartCopyThreadpool": {
            "Type": "Parallel",
            "Branches": [],  # This will be filled in with an array of "thread" state machines below
            "Next": "ComposeParts"
        },
        "ComposeParts": {
            "Type": "Task",
            "Resource": None,  # This will be set by Domovoi to the Lambda ARN
            "End": True,
            "Retry": retry_config
        },
        "Quit": {
            "Type": "Pass",
            "End": True
        }
    }
}

sfn_thread = {
    "StartAt": "Worker{t}",
    "States": {
        "Worker{t}": {
            "Type": "Task",
            "Resource": None,  # This will be set by Domovoi to the Lambda ARN
            "Next": "Branch{t}",
            "Retry": retry_config
        },
        "Branch{t}": {
            "Type": "Choice",
            "Choices": [{
                "Variable": "$.finished",
                "BooleanEquals": True,
                "Next": "EndThread{t}"
            }],
            "Default": "Worker{t}"
        },
        "EndThread{t}": {
            "Type": "Pass",
            "End": True
        }
    }
}

@app.step_function_task(state_name="DispatchSync", state_machine_definition=sfn)
def dispatch_sync(event, context):
    """
    Processes the storage event notification and orchestrates the rest of the copying:
    - If the notification is for a file blob:
        - If the blob is under the one-shot threshold, immediately copies it and exits the state machine.
        - Otherwise, configures the state machine to run the threadpool (copy blob parts) and closer (compose the copy).
    - Otherwise, check if all referenced entities are already copied to the destination:
        - For a file manifest, check that the blob is there.
        - For a bundle manifest, check that file manifests for all files in the bundle are there.
        - For a collection manifest, check that all collection contents are there.
        If the checks fail, cause the state machine to sleep for 8 seconds, then try again.
        If the checks succeed, do a one-shot copy of the manifest.
    """
    if event["source_key"].startswith("blobs"):
        if int(event["source_obj_metadata"]["size"]) > AWS_MIN_CHUNK_SIZE:
            new_event = dict(event, sleep=False, do_oneshot_copy=False, do_multipart_copy=True, done=False)
            if Replica[event["dest_replica"]] == Replica.aws:
                mpu_id = initiate_multipart_upload(source_replica=Replica[event["source_replica"]],
                                                   dest_replica=Replica[event["dest_replica"]],
                                                   source_key=event["source_key"])
                new_event.update(mpu_id=mpu_id)
            return new_event
        else:
            return dict(event, sleep=False, do_oneshot_copy=True, do_multipart_copy=False, done=False)
    return dict(event, sleep=False, do_oneshot_copy=False, do_multipart_copy=False, done=False)

def copy_parts(event, context):
    task_name = context.stepfunctions_task_name
    source_replica = Replica[event["source_replica"]]
    dest_replica = Replica[event["dest_replica"]]
    object_size = int(event["source_obj_metadata"]["size"])

    assert task_name.startswith("Worker")
    task_id = int(task_name[len("Worker"):])

    part_size = get_part_size(object_size, dest_replica)

    log_msg = "Copying {source_key}:{part} from {source_replica} to {dest_replica}"
    blobstore_handle = dss.Config.get_blobstore_handle(source_replica)
    source_url = blobstore_handle.generate_presigned_GET_url(bucket=source_replica.bucket, key=event["source_key"])
    futures = []
    gs = dss.Config.get_native_handle(Replica.gcp)
    with ThreadPoolExecutor(max_workers=4) as executor:
        for part_index, part_start in enumerate(range(0, object_size, part_size)):
            if part_index % sync_sfn_num_threads != task_id:
                continue
            if part_index <= event.get("last_completed_part", -1):
                continue

            part = dict(id=part_index + 1, start=part_start, end=min(object_size - 1, part_start + part_size - 1))

            logger.info(log_msg.format(part=part, **event))
            if dest_replica.storage_schema == "s3":
                # TODO: (s3) Check if the part was completed but not marked as last copied.
                upload_url = "{host}/{bucket}/{key}?partNumber={part_num}&uploadId={mpu_id}".format(
                    host=clients.s3.meta.endpoint_url,
                    bucket=dest_replica.bucket,
                    key=event["source_key"],
                    part_num=part["id"],
                    mpu_id=event["mpu_id"]
                )
            elif dest_replica.storage_schema == "gs":
                dest_blob_name = "{}.part{}".format(event["source_key"], part_index + 1)
                dest_blob = gs.get_bucket(dest_replica.bucket).blob(dest_blob_name)
                if dest_blob.exists():
                    continue

                upload_url = dest_blob.create_resumable_upload_session(size=part["end"] - part["start"] + 1)
            futures.append(executor.submit(copy_part, upload_url, source_url, dest_replica.storage_schema, part))
            if len(futures) >= parts_per_worker[dest_replica.storage_schema]:
                break
    for future in futures:
        future.result()

    event.update(last_completed_part=part_index)
    event.setdefault("finished", False)
    if (part_index + 1) * part_size >= object_size:
        event.update(finished=True)
    return event

# Construct the threadpool definition by explicitly mentioning each thread in the state machine definition.
for t in range(sync_sfn_num_threads):
    thread = json.loads(json.dumps(sfn_thread).replace("{t}", str(t)))
    sfn["States"]["MultipartCopyThreadpool"]["Branches"].append(thread)
    app.step_function_task(state_name="Worker{}".format(t), state_machine_definition=sfn)(copy_parts)

@app.step_function_task(state_name="CheckDeps", state_machine_definition=sfn)
def check_deps(event, context):
    source_replica = Replica[event["source_replica"]]
    dest_replica = Replica[event["dest_replica"]]
    if dependencies_exist(source_replica, dest_replica, event["source_key"]):
        return dict(event, sleep=False, do_oneshot_copy=True)
    else:
        return dict(event, sleep=True, sleep_seconds=sync_sfn_dep_wait_sleep_seconds)

@app.step_function_task(state_name="OneshotCopy", state_machine_definition=sfn)
def oneshot_copy(event, context):
    source_replica = Replica[event["source_replica"]]
    dest_replica = Replica[event["dest_replica"]]
    logger.info(f"Begin transfer of {event['source_key']} from {source_replica} to {dest_replica}")
    do_oneshot_copy(source_replica=source_replica, dest_replica=dest_replica, source_key=event["source_key"])
    logger.info(f"Finished transfer of {event['source_key']} from {source_replica} to {dest_replica}")

@app.step_function_task(state_name="ComposeParts", state_machine_definition=sfn)
def compose_parts(events, context):
    # Since this is a SFN task handler that sits downstream of a Parallel task, it receives an array of task outputs.
    # We use just the first task output since the relevant SFN state information in all the outputs is identical.
    dest_replica = Replica[events[0]["dest_replica"]]
    if dest_replica == Replica.gcp:
        compose_upload(get_sync_work_state(events[0]))
    elif dest_replica == Replica.aws:
        complete_multipart_upload(get_sync_work_state(events[0]))
    else:
        raise NotImplementedError()
