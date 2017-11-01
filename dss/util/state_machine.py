def gen_branch(branch_idx: int):
    return \
        {
            "StartAt": "Branch{}".format(branch_idx),
            "States": {
                "Branch{}".format(branch_idx): {
                    "Type": "Choice",
                    "Choices": [{
                        "Variable": "$.file-count",
                        "NumericGreaterThan": branch_idx,
                        "Next": "Init{}".format(branch_idx)
                    }],
                    "Default": "Pass{}".format(branch_idx)
                },
                "Init{}".format(branch_idx): {
                    "Type": "Pass",
                    "Result": branch_idx,
                    "ResultPath": "$.branch",
                    "OutputPath": "$",
                    "Next": "Copy{}".format(branch_idx)
                },
                "Copy{}".format(branch_idx): {
                    "Type": "Task",
                    "Resource": None,  # This will be set by Domovoi to the Lambda ARN
                    "End": True
                },
                "Pass{}".format(branch_idx): {
                    "Type": "Pass",
                    "End": True
                }
            }
        }

def gen_all(pool_size: int):
    pool = []
    for branch_idx in range(pool_size):
        pool.append(gen_branch(branch_idx))
    return pool


pool_size = 10

sfn = {
    "Comment": "Checkut out service state machine using a parallel state to execute two branches at the same time.",
    "StartAt": "Count",
    "States": {
        "Count": {
            "Type": "Pass",
            "Result": 2,
            "ResultPath": "$.file-count",
            "OutputPath": "$",
            "Next": "Parallel1"
        },
        "Parallel1": {
            "Type": "Parallel",
            "Next": "Final",
            "Branches": gen_all(pool_size)
        },
        "Final": {
            "Type": "Pass",
            "End": True
        }
    }
}

sfn1 = {
"Comment": "DSS Checkout service state machine that submits a Job to chained copy client and monitors the Job until it completes.",
  "StartAt": "ScheduleCopy",
  "States": {
    "ScheduleCopy": {
      "Type": "Task",
      "Resource": None,
      "Next": "Wait"
    },
    "Wait": {
      "Type": "Wait",
      "SecondsPath": "$.wait_time",
      "Next": "GetJobStatus"
    },
    "GetJobStatus": {
      "Type": "Pass",
        "Result": "SUCCEEDED",
        "ResultPath": "$.status",
        "OutputPath": "$",
        "Next": "JobDone"
    },
    "JobDone": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.status",
          "StringEquals": "FAILED",
          "Next": "JobFailed"
        },
        {
          "Variable": "$.status",
          "StringEquals": "IN_PROGRESS",
          "Next": "Wait"
        },
        {
          "Variable": "$.status",
          "StringEquals": "SUCCEEDED",
          "Next": "Notify"
        }
      ],
      "Default": "Wait"
    },
    "JobFailed": {
      "Type": "Fail",
      "Cause": "DSS Job Failed",
      "Error": "DSS Job returned FAILED"
    },
    "Notify": {
      "Type": "Pass",
      "End": True
    }
  }
}