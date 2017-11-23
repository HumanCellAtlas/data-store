state_machine_def = {
    "Comment": "DSS Checkout service state machine that submits a Job to chained copy client"
               " and monitors the Job until it completes.",
    "StartAt": "PreExecutionCheck",
    "States": {
        "PreExecutionCheck": {
            "Type": "Task",
            "Resource": None,
            "ResultPath": "$.validation",
            "Next": "PreExecutionCheckPassed"
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
            "Next": "Wait"
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
            "Next": "JobDone"
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
