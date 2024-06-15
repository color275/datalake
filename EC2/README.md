# WebServer 셋업
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
> [!CAUTION]  
> chiholee-ec2-instance-profile 은 AdministratorAccess 권한을 갖고 있음
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

### Generate Order Data
- EC2 SSH 접속
- git clone
    ```bash
    sudo yum install -y git
    cd $HOME
    git clone https://github.com/color275/datalake.git        
    ```
- DB Endpoint 환경 변수 저장
    ```bash
    export RDS_WRITER_ENDPINT=chiholee.cluster-cz2ms4a2cmjt.ap-northeast-2.rds.amazonaws.com
    ```
- order data gen
    ```bash
    cd /home/ec2-user/datalake/gen_order_data
    python3 -m venv .venv
    echo 'alias venv="source .venv/bin/activate"' >> ~/.bash_profile
    . ~/.bash_profile
    venv
    pip install -r requirements.txt
    ```
- create env
    ```bash
    cat <<EOF > .env
    MYSQL_HOST=$RDS_WRITER_ENDPINT
    MYSQL_USER=admin
    MYSQL_PASSWORD=admin1234
    MYSQL_DB=ecommerce
    EOF
    ```
    > [!CAUTION]  
    > rds > cluster > modify > Credentials management > Self managed 선택
    > 
    > Master password 를 admin1234 로 변경
    > 
    > RDS 보안그룹이 common 보안그룹 3306 포트 허용
- mysql client 설치
    ```bash
    sudo dnf -y localinstall https://dev.mysql.com/get/mysql80-community-release-el9-4.noarch.rpm
    sudo dnf -y install mysql mysql-community-client
    ```
- RDS 접속, 데이터베이스/테이블 생성(password : admin1234)
    ```bash
    cd /home/ec2-user/datalake/gen_order_data
    mysql -uadmin -p -h $RDS_WRITER_ENDPINT
    create database ecommerce;
    use ecommerce
    source ecommerce_backup.sql
    exit
    ```
- AccessLog 디렉토리 생성
    ```bash
    sudo mkdir /var/log/accesslog
    sudo chown ec2-user:ec2-user /var/log/accesslog
    ```
- execution
    ```bash
    nohup python generate.py &
    ```

### AccessLog -> MSK 파이프라인
#### FluentBit
- FluentBit 설치 (https://docs.fluentbit.io/manual/installation/linux/amazon-linux#amazon-linux-2023)
  ```bash
  cd /etc/yum.repos.d/
  ```
  ```bash
  # sudo vi fluent-bit.repo
  [fluent-bit]
  name = Fluent Bit
  baseurl = https://packages.fluentbit.io/amazonlinux/2023/
  gpgcheck=1
  gpgkey=https://packages.fluentbit.io/fluentbit.key
  enabled=1  
  ```
- FluentBit 시작 및 확인
  ```bash
  sudo yum -y install fluent-bit
  sudo systemctl start fluent-bit
  systemctl status fluent-bit
  ```

- access_log_topic Topic 생성
  ```bash
  kafka-topics.sh  \
  --create  \
  --topic access_log_topic  \
  --bootstrap-server $MSK_BOOTSTRAP_ADDRESS  \
  --replication-factor 2  \
  --partitions 1
  ```
- Topic 생성 확인
  ```bash
  kafka-topics.sh \
  --bootstrap-server $MSK_BOOTSTRAP_ADDRESS \
  --list
  ```

- FluentBit Config 수정
  ```bash
  # sudo vi /etc/fluent-bit/fluent-bit.conf
  [SERVICE]
    flush        1
    daemon       Off
    log_level    info
  [INPUT]
    name tail
    path /var/log/accesslog/access.log.*
  [OUTPUT]
    name kafka
    match *
    brokers $MSK_BOOTSTRAP_ADDRESS
    topics access_log_topic
  ```
- FluentBit 재시작
  ```bash
  systemctl restart fluent-bit
  systemctl status fluent-bit
  ```

- MSK의 access_log_topic에 메시지가 Publish 되는지 확인
  ```bash
  kafka-console-consumer.sh \
  --bootstrap-server $MSK_BOOTSTRAP_ADDRESS \
  --topic access_log_topic
  ```