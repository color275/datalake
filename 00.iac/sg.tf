resource "aws_security_group" "common_sg" {
  name        = "${local.name}-common-sg"
  description = "${local.name}-common-sg"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Allow all inbound traffic from self"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  ingress {
    description = "Allow HTTP traffic"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}