data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_availability_zones" "available" {}

data "aws_subnet" "default" {
  count             = 3
  vpc_id            = "${data.aws_vpc.default.id}"
  availability_zone = "${data.aws_availability_zones.available.names[count.index]}"
  default_for_az    = true
}

data "aws_ecs_cluster" "default"{
  cluster_name = "default"
}

locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-monitor-${var.DSS_INFRA_TAG_SERVICE}",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "${var.DSS_INFRA_TAG_OWNER}"
  )}"
}

resource "aws_iam_role" "task-executor" {
  name = "dss-monitor-${var.DSS_DEPLOYMENT_STAGE}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ecs.amazonaws.com",
          "ecs-tasks.amazonaws.com",
          "events.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "task-executor-ecs" {
  role = "${aws_iam_role.task-executor.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "AWS-Events-Invoke" {
  name = "dss-monitor-cloudwatch-event-invoke-${var.DSS_DEPLOYMENT_STAGE}"
  role = "${aws_iam_role.task-executor.id}"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ecs:RunTask"
            ],
            "Resource": [
                "arn:aws:ecs:*:${data.aws_caller_identity.current.account_id}:task-definition/dss-monitor-${var.DSS_DEPLOYMENT_STAGE}:*"
            ],
            "Condition": {
                "ArnLike": {
                    "ecs:cluster": "arn:aws:ecs:*:${data.aws_caller_identity.current.account_id}:cluster/default"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": [
                "*"
            ],
            "Condition": {
                "StringLike": {
                    "iam:PassedToService": "ecs-tasks.amazonaws.com"
                }
            }
        }
    ]
}
EOF
}

resource "aws_iam_role" "task-performer" {
  name = "dss-monitor-task-performer-${var.DSS_DEPLOYMENT_STAGE}"
  tags = "${local.common_tags}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ecs-tasks.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "task-performer" {
  name = "dss-monitor-task-performer-${var.DSS_DEPLOYMENT_STAGE}"
  role = "${aws_iam_role.task-performer.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
            {
      "Effect": "Allow",
      "Action": "secretsmanager:Get*",
      "Resource": "arn:aws:secretsmanager:*:${data.aws_caller_identity.current.account_id}:secret:${var.DSS_SECRETS_STORE}/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "tag:GetTagKeys",
        "tag:GetResources",
        "tag:GetTagValues",
        "cloudwatch:*"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_ecs_task_definition" "monitor" {
  # Note if changing the family, also change the IAM ROLE POLICY under ecs:RunTask; unable to reference due to dependency cycle.
  family = "dss-monitor-${var.DSS_DEPLOYMENT_STAGE}"
  execution_role_arn = "${aws_iam_role.task-executor.arn}"
  task_role_arn = "${aws_iam_role.task-performer.arn}"
  requires_compatibilities = ["FARGATE"]
  network_mode = "awsvpc"
  cpu = "256"
  memory = "512"
  # TODO add logging below.
  container_definitions = <<DEFINITION
[
  {
    "family": "dss-monitor",
    "name": "dss-monitor-lambda",
    "image": "humancellatlas/dss-monitor-image",
    "essential": true,
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${aws_cloudwatch_log_group.task-performer.name}",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
    },
    "environment" :[
          {"name": "DSS_DEPLOYMENT_STAGE", "value": "${var.DSS_DEPLOYMENT_STAGE}"},
          {"name": "DSS_SECRETS_STORE", "value": "${var.DSS_SECRETS_STORE}"},
          {"name": "DSS_MONITOR_WEBHOOK_SECRET_NAME", "value": "${var.DSS_MONITOR_WEBHOOK_SECRET_NAME}"}
    ]
  }
]
DEFINITION
  tags = "${local.common_tags}"
}


resource "aws_cloudwatch_log_group" "task-performer" {
  name              = "/aws/service/dss-monitor-task-performer-${var.DSS_DEPLOYMENT_STAGE}"
  retention_in_days = 1827
}

resource "aws_cloudwatch_event_rule" "dss-monitor" {
  name = "dss-monitor-trigger-${var.DSS_DEPLOYMENT_STAGE}"
  schedule_expression = "cron(0 17 * * * *)"
  description = "schedule trigger for dss-monitor notifications"
  tags = "${local.common_tags}"

}

resource "aws_cloudwatch_event_target" "scheduled_task" {
  rule      = "${aws_cloudwatch_event_rule.dss-monitor.name}"
  arn       = "${data.aws_ecs_cluster.default.arn}"
  role_arn = "${aws_iam_role.task-executor.arn}"
  ecs_target = {
    task_count          = 1
    task_definition_arn = "${aws_ecs_task_definition.monitor.arn}"
    launch_type         = "FARGATE"
    platform_version    = "LATEST"

    network_configuration {
      assign_public_ip = true
      subnets         = ["${data.aws_subnet.default.*.id}"]
    }
  }
}

