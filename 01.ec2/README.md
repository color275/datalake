# EC2 셋업
## KeyPair 생성
1. Name : chiholee-datalake01
1. Key 다운로드 후 chmod 변경 `chmod 400 chiholee-datalake01.pem` 

## EC2 셋업
1. Name : WebServer
1. Amazon Machine Image (AMI) : Amazon Linux 2023 AMI
1. Instance Type : m5.xlarge
1. Key pair : chiohlee-datalake01
1. VPC : chiholee
1. Subnet : chiholee-public-ap-northeast-2a
1. Auto-assign public IP : Enable
1. Common security groups : chiholee-common-sg
1. Configure stroage : 100gib
1. Advanced details
   1. IAM instance profile : chiholee-ec2-instance-profile

   1. User data
    ```bash
    #!/bin/bash
    # Update the package index
    yum update -y

    # Install the tzdata package if not already installed
    yum install -y tzdata

    # Set the timezone to Asia/Seoul
    timedatectl set-timezone Asia/Seoul

    # Verify the timezone is set correctly
    timedatectl
    ```
> [!CAUTION]  
> chiholee-ec2-instance-profile 은 AdministratorAccess 권한을 갖고 있음

## 주문 데이터 생성
1. EC2 SSH 접속
1. git clone
   ```bash
   sudo yum install -y git
   cd $HOME
   git clone https://github.com/color275/datalake.git        
   ```
1. DB Endpoint 환경 변수 저장
   ```bash
   echo 'export RDS_WRITER_ENDPINT=chiholee.cluster-cz2ss0kqkbwh.ap-northeast-2.rds.amazonaws.com' >> ~/.bash_profile
   echo 'alias ss="mysql -uadmin -p -h $RDS_WRITER_ENDPINT"' >> ~/.bash_profile
   . ~/.bash_profile
   ```
1. order data gen
   ```bash
   cd /home/ec2-user/datalake/ec2
   python3 -m venv .venv
   echo 'alias venv="source .venv/bin/activate"' >> ~/.bash_profile
   . ~/.bash_profile
   venv
   pip install -r requirements.txt
   ```
1. create env
   ```bash
   cat <<EOF > .env
   MYSQL_HOST=$RDS_WRITER_ENDPINT
   MYSQL_USER=admin
   MYSQL_PASSWORD=admin1234
   MYSQL_DB=ecommerce
   EOF
   ```
1. mysql client 설치
   ```bash
   sudo dnf -y localinstall https://dev.mysql.com/get/mysql80-community-release-el9-4.noarch.rpm
   sudo dnf -y install mysql mysql-community-client
   ```
1. RDS Primary Password 를 admin1234 로 변경

1. RDS 보안그룹이 common 보안그룹 3306 포트 허용

1. RDS 접속, 데이터베이스/테이블 생성(password : admin1234)
    ```bash
    cd /home/ec2-user/datalake/ec2

    mysql -uadmin -p -h $RDS_WRITER_ENDPINT
    create database ecommerce;
    use ecommerce
    source ecommerce_backup.sql
    exit
    ```
1. AccessLog 디렉토리 생성
   ```bash
   sudo mkdir /var/log/accesslog
   sudo chown ec2-user:ec2-user /var/log/accesslog
   ```
1. execution
   ```bash
   nohup python generate.py &
   ```
