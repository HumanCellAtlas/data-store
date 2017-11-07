definition = {
    "Comment": "DSS Checkout service state machine that submits a Job to chained copy client and monitors the Job until it completes.",
    "StartAt": "SanityCheck",
    "States": {
        "SanityCheck": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.validation",
            "Next": "SanityCheckPassed"
        },
        "SanityCheckPassed": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.validation.code",
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
            "Next": "Wait"
        },
        "Wait": {
            "Type": "Wait",
            "SecondsPath": "$.schedule.wait_time",
            "Next": "GetJobStatus"
        },
        "GetJobStatus": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.status",
            "Next": "JobDone"
        },
        "JobDone": {
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.status.code",
                    "StringEquals": "FAILURE",
                    "Next": "NotifyFailure"
                },
                {
                    "And": [
                        {
                            "Variable": "$.status.code",
                            "StringEquals": "IN_PROGRESS",
                        },
                        {
                            "Variable": "$.status.check_count",
                            "NumericGreaterThan": 5,
                        }
                    ],
                    "Next": "NotifyFailure"
                },
                {
                    "Variable": "$.status.code",
                    "StringEquals": "IN_PROGRESS",
                    "Next": "Wait"
                },
                {
                    "Variable": "$.status.code",
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
