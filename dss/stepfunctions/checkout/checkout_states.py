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
    },
    {
        "ErrorEquals": ["States.ALL"],
        "IntervalSeconds": 5,
        "MaxAttempts": 5,
        "BackoffRate": 2.0
    }
]

catch_config = [
    {
        "ErrorEquals": ["States.ALL"],
        "Next": "NotifyFailure"
    }
]

state_machine_def = {
    "Comment": "DSS checkout service state machine that submits a job to S3 copy client"
               " and monitors the Job until it completes.",
    "StartAt": "PreExecutionCheck",
    "TimeoutSeconds": 3600,             # 60 minutes, in seconds.
    "States": {
        "PreExecutionCheck": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.validation",
            "Next": "PreExecutionCheckPassed",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "PreExecutionCheckPassed": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.validation.checkout_status",
                    "StringEquals": "PASSED",
                    "Next": "ScheduleCopy"
                }
            ],
            "Default": "NotifyFailure"
        },
        "ScheduleCopy": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.schedule",
            "Next": "Wait",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.schedule.wait_time_seconds",
            "Next": "GetJobStatus"
        },
        "GetJobStatus": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.status",
            "Next": "JobDone",
            "Retry": retry_config,
            "Catch": catch_config
        },
        "JobDone": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.status.checkout_status",
                    "StringEquals": "FAILURE",
                    "Next": "NotifyFailure"
                },
                {
                    "And": [
                        {
                            "Variable": "$.status.checkout_status",
                            "StringEquals": "IN_PROGRESS",
                        },
                        {
                            "Variable": "$.status.check_count",
                            "NumericGreaterThan": 10,
                        }
                    ],
                    "Next": "NotifyFailure"
                },
                {
                    "Variable": "$.status.checkout_status",
                    "StringEquals": "IN_PROGRESS",
                    "Next": "Wait"
                },
                {
                    "Variable": "$.status.checkout_status",
                    "StringEquals": "SUCCESS",
                    "Next": "Notify"
                }
            ],
            "Default": "Wait"
        },
        "NotifyFailure": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.emailFailure",
            "Next": "JobFailed"
        },
        "JobFailed": {
            "Type": "Fail",
            "Cause": "DSS Job Failed",
            "Error": "DSS Job returned FAILED"
        },
        "Notify": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.email",
            "End": True
        }
    }
}
