# FluentBit ( to MSK)
1. FluentBit 설치 (https://docs.fluentbit.io/manual/installation/linux/amazon-linux#amazon-linux-2023)
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
2. FluentBit 시작 및 확인
   ```bash
   sudo yum -y install fluent-bit
   sudo systemctl start fluent-bit
   systemctl status fluent-bit
   ```

3. access_log_topic Topic 생성
   ```bash
   kafka-topics.sh  \
   --create  \
   --topic access_log_topic  \
   --bootstrap-server $MSK_BOOTSTRAP_ADDRESS  \
   --replication-factor 2  \
   --partitions 1
   ```

4. Topic 생성 확인
   ```bash
   kafka-topics.sh \
   --bootstrap-server $MSK_BOOTSTRAP_ADDRESS \
   --list
   ```

5. FluentBit Config 수정 ($MSK_BOOTSTRAP_ADDRESS 에 실제 값을 저장)
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
6. FluentBit 재시작
   ```bash
   sudo systemctl restart fluent-bit
   sudo systemctl status fluent-bit
   ```

7. MSK의 access_log_topic에 메시지가 Publish 되는지 확인
   ```bash
   kafka-console-consumer.sh \
   --bootstrap-server $MSK_BOOTSTRAP_ADDRESS \
   --topic access_log_topic
   ```