data "aws_caller_identity" "current" {}
locals {
  common_tags = "${map(
    "managedBy" , "terraform",
    "Name"      , "${var.DSS_INFRA_TAG_SERVICE}-monitor-${var.DSS_INFRA_TAG_SERVICE}",
    "project"   , "${var.DSS_INFRA_TAG_PROJECT}",
    "env"       , "${var.DSS_DEPLOYMENT_STAGE}",
    "service"   , "${var.DSS_INFRA_TAG_SERVICE}",
    "owner"     , "${var.DSS_INFRA_TAG_OWNER}"
  )}",
  availability_zones = "${split(" ", "${var.DSS_AVAILABILITY_ZONES}")}"
}
resource "aws_iam_role" "task_executor" {
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
          "ecs-tasks.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "task_executor_ecs" {
  role = "${aws_iam_role.task_executor.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "query_runner" {
  name = "dss-monitor-query-runner-${var.DSS_DEPLOYMENT_STAGE}"
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

resource "aws_iam_role_policy" "query_runner" {
  name = "dss-monitor-query-runner-${var.DSS_DEPLOYMENT_STAGE}"
  role = "${aws_iam_role.query_runner.id}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
            {
      "Effect": "Allow",
      "Action": "secretsmanager:Get*",
      "Resource": "arn:aws:secretsmanager:*:${var.AWS_ACCOUNT_ID}:secret:${var.DSS_SECRETS_STORE}/*"
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
  family = "dss-monitor-${var.DSS_DEPLOYMENT_STAGE}"
  execution_role_arn = "${aws_iam_role.task_executor.arn}"
  task_role_arn = "${aws_iam_role.query_runner.arn}"
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
    "cpu": 256,
    "memory": 512,
    "essential": true,
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${aws_cloudwatch_log_group.query_runner.name}",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
    },
    "portMappings": [
      {
        "containerPort": 80,
        "hostPort": 80
      }
    ]
  }
]
DEFINITION
  # tags = "${local.common_tags}"
}


resource "aws_cloudwatch_log_group" "query_runner" {
  name              = "/aws/service/dss-monitor-query-runner-${var.DSS_DEPLOYMENT_STAGE}"
  retention_in_days = 1827
}


resource "aws_vpc" "dss_fargate" {
  cidr_block = "10.0.0.0/16"
  enable_dns_support = true
  enable_dns_hostnames = true
  tags = "${local.common_tags}"
}

# These subnets need to be associated with a route table providing
# internet access.
resource "aws_subnet" "dss_monitor" {
  count 		    = "${length(local.availability_zones)}"
  vpc_id 			= "${aws_vpc.dss_fargate.id}"
  availability_zone = "${local.availability_zones[count.index]}"
  cidr_block = "${cidrsubnet("10.0.0.0/16", "4", count.index)}"
}

resource "aws_internet_gateway" "gw" {
  vpc_id = "${aws_vpc.dss_fargate.id}"
}

# Route the public subnet trafic through the IGW
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.dss_fargate.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.gw.id}"
}



resource "aws_ecs_cluster" "dss_monitor" {
  name = "dss-monitor-${var.DSS_DEPLOYMENT_STAGE}"
  # tags = "${local.common_tags}"
}

resource "aws_ecs_service" "notification-builder" {
  name            = "dss-monitor-${var.DSS_DEPLOYMENT_STAGE}"
  cluster         = "${aws_ecs_cluster.dss_monitor.id}"
  task_definition = "${aws_ecs_task_definition.monitor.arn}"
  desired_count   = 0
  launch_type = "FARGATE"
  # depends_on      = ["aws_subnet.dss-monitor"]
  # tags = "${local.common_tags}"

  lifecycle {
    ignore_changes = ["desired_count"]
  }

  network_configuration {
    # security_groups = ["${aws_vpc.vpc.default_security_group_id}"]
    subnets         = ["${aws_subnet.dss_monitor.*.id}"]
    assign_public_ip = true
  }
}

resource "aws_cloudwatch_event_rule" "dss-monitor" {
  alarm_name = "dss-monitor-lambda-trigger"
  schedule_expression = "cron(0 0 * * MON-FRI *)"
  description = "daily event trigger for dss-monitor notifications"
  tags = "${local.common_tags}"

}

resource "aws_cloudwatch_event_target" "scheduled_task" {
  rule      = "${aws_cloudwatch_event_rule.scheduled_task.name}"
  arn       = "${aws_ecs_cluster.dss_monitor.arn}"

  ecs_target = {
    task_count          = 1
    task_definition_arn = "${aws_ecs_task_definition.monitor.arn}"
    launch_type         = "FARGATE"
    platform_version    = "LATEST"
    group               = ""

    network_configuration {
      assign_public_ip = false
      security_groups  = ["${aws_security_group.nsg_task.id}"]
      subnets          = ["${split(",", var.private_subnets)}"]
    }
  }

  # allow the task definition to be managed by external ci/cd system
  lifecycle = {
    ignore_changes = ["ecs_target.0.task_definition_arn"]
  }
}
