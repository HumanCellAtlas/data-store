sentinel_sfn = {
    "Comment": "DSS Re-index service state machine sentinel",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": None,
            "Next": "MusterWalkers",
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "NotifyFailure"
                }
            ]
        },
        "MusterWalkers": {
            "Type": "Task",
            "Resource": None,
            "TimeoutSeconds": 240,
            "Next": "CheckStatus",
            "Retry": [
                {
                    "ErrorEquals": ["States.Timeout"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 3,
                    "BackoffRate": 2
                }
            ],
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": "NotifyFailure"
                }
            ]
        },
        "CheckStatus": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.code",
                    "StringEquals": "RUNNING",
                    "Next": "Wait"
                },
            ],
            "Default": "Succeeded"
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.wait_time",
            "Next": "MusterWalkers"
        },
        "NotifyFailure": {
            "Type": "Task",
            "Resource": None,
            "Next": "Failed"
        },
        "Failed": {
            "Type": "Fail",
        },
        "Succeeded": {
            "Type": "Task",
            "Resource": None,
            "End": True
        }
    }
}

walker_sfn = {
    "Comment": "prefix walker",
    "StartAt": "Initialize",
    "States": {
        "Initialize": {
            "Type": "Task",
            "Resource": None,
            "Next": "CheckStatus",
            "Catch": [
                {
                    "ErrorEquals": [
                        "States.ALL"
                    ],
                    "Next": "Failed"
                }
            ]
        },
        "Walk": {
            "Type": "Task",
            "Resource": None,
            "Next": "CheckStatus",
            "TimeoutSeconds": 240,
            "Retry": [
                {
                    "ErrorEquals": ["States.Timeout"],
                    "IntervalSeconds": 2,
                    "MaxAttempts": 3,
                    "BackoffRate": 2
                },
            ],
            "Catch": [
                {
                    "ErrorEquals": [
                        "States.ALL"
                    ],
                    "Next": "Failed"
                }
            ]
        },
        "CheckStatus": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.code",
                    "StringEquals": "RUNNING",
                    "Next": "Walk"
                }
            ],
            "Default": "Succeeded"
        },
        "Failed": {
            "Type": "Fail",
        },
        "Succeeded": {
            "Type": "Pass",
            "End": True
        }
    }
}
