data "aws_iam_policy" "administrator_access" {
  arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

data "aws_iam_policy" "ssm_managed_instance_core" {
  arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role" "ec2_admin_role" {
  name = "${local.name}-ec2-admin-role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "ec2_admin_attach" {
  role       = aws_iam_role.ec2_admin_role.name
  policy_arn = data.aws_iam_policy.administrator_access.arn
}

resource "aws_iam_role_policy_attachment" "ssm_attach" {
  role       = aws_iam_role.ec2_admin_role.name
  policy_arn = data.aws_iam_policy.ssm_managed_instance_core.arn
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "${local.name}-ec2-instance-profile"
  role = aws_iam_role.ec2_admin_role.name
}