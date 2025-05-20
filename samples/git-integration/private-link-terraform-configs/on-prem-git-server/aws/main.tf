terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.95.0"
    }
  }
}

provider "aws" {
  shared_credentials_files = ["#credentials-file"] #Replace with with your credentials file
  profile                  = "default"
  region                   = var.b_region
}

variable "a_vpc_id" {
  type        = string
  description = "VPC ID (example value: vpc-0b034fa19c3b5ac93)"
}

variable "b_region" {
  type        = string
  description = "Snowflake region (example value: us-west-2)"
}

variable "c_availability_zone" {
  type        = string
  description = "Availability zone within the region (example value: us-west-2b)"
}

data "aws_subnet" "vpc_subnet" {
  filter {
    name = "availabilityZone"
    values = [var.c_availability_zone]
  }
}

resource "aws_vpc_endpoint_service" "snowflake_pl_es" {
  acceptance_required        = true
  allowed_principals         = ["*"]
  network_load_balancer_arns = [aws_lb.snowflake_pl_lb.arn]

  tags = {
    Name = "snowflake_pl_es"
  }
}

resource "aws_lb" "snowflake_pl_lb" {
  name               = "snowflake-privatelink-lb"
  internal           = true
  load_balancer_type = "network"
  enable_cross_zone_load_balancing = true
  security_groups    = [aws_security_group.snowflake_pl_sg.id]
  subnets            = [data.aws_subnet.vpc_subnet.id]
}

resource "aws_lb_listener" "snowflake_pl_lbl" {
  load_balancer_arn = aws_lb.snowflake_pl_lb.arn
  port              = "443"
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.snowflake_pl_tg.arn
  }
}

resource "aws_lb_target_group" "snowflake_pl_tg" {
  name     = "snowflake-privatelink-tg"
  port     = 443
  protocol = "TCP"
  vpc_id   = var.a_vpc_id
}

resource "aws_lb_target_group_attachment" "snowflake_pl_tg_ec2" {
  target_group_arn = aws_lb_target_group.snowflake_pl_tg.arn
  target_id        = aws_instance.snowflake_pl_git_server.id
  port             = 443
}

resource "aws_security_group" "snowflake_pl_sg" {
  name        = "snowflake_pl_https_443_sg"
  description = "Allow HTTPS traffic from anywhere"

  ingress {
    description = "TCP"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Allow SSH traffic from anywhere
  }

  egress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# create EC2 proxy instance
resource "aws_instance" "snowflake_pl_git_server" {
  ami           = data.aws_ami.amazon_linux.id
  instance_type = "t2.micro"
  subnet_id     = data.aws_subnet.vpc_subnet.id
  key_name      = null # This ensures the instance is created without a key pair

  vpc_security_group_ids = [aws_security_group.snowflake_pl_ssh_sg.id, aws_security_group.snowflake_pl_sg.id]

  tags = {
    Name = "snowflake_pl_git_server"
  }
}


data "aws_ami" "amazon_linux" {
  most_recent = true

  filter {
    name   = "name"
    values = ["amzn2-ami-kernel-5.10-hvm-*-x86_64-gp2"] # Pattern for Amazon Linux 2023 AMI
  }

  filter {
    name   = "owner-id"
    values = ["137112412989"] # Amazon's official owner ID for Amazon Linux
  }
}

resource "aws_security_group" "snowflake_pl_ssh_sg" {
  name        = "snowflake_pl_ssh_sg"
  description = "Allow SSH traffic from anywhere"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Allow SSH traffic from anywhere
  }

  egress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}