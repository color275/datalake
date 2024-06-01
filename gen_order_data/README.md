### WebServer
- KeyPair 
  - Name : chiholee-datalake01
  - Key 다운로드 후 chmod 변경 `chmod 400 chiholee-datalake01.pem` 

- EC2 생성
  - Name : WebServer
  - Amazon Machine Image (AMI) : Amazon Linux 2023 AMI
  - Instance Type : m5.xlarge
  - Key pair : chiohlee-datalake01
  - VPC : chiholee
  - Subnet : chiholee-public-ap-northeast-2a
  - Auto-assign public IP : Enable
  - Common security groups : chiholee-common-sg
  - Configure stroage : 100gib
  - Advanced details
    - IAM instance profile : chiholee-ec2-instance-profile
        > [!CAUTION]  
        > chiholee-ec2-instance-profile 은 AdministratorAccess 권한을 갖고 있음

  - User data
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

  - EC2 SSH 접속
    - git clone
        ```bash
        sudo yum install -y git
        cd $HOME
        git clone https://github.com/color275/datalake.git        
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
        MYSQL_HOST=chiholee.cluster-cz2ms4a2cmjt.ap-northeast-2.rds.amazonaws.com
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
        mysql -uadmin -p -h chiholee.cluster-cz2ms4a2cmjt.ap-northeast-2.rds.amazonaws.com
        create database ecommerce;
        use ecommerce
        source ecommerce_backup.sql
        exit
        ```



### execution
```bash
nohup python generate.py &
```

