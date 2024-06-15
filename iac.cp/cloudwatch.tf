resource "aws_cloudwatch_log_group" "msk_broker_log_group" {
  name              = "/aws/msk/broker/logs"
  retention_in_days = 30 # 로그 보존 기간을 원하는 대로 변경하세요.
}