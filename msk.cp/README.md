# Create MSK

![](./img/2024-06-02-19-46-42.png)

![](./img/2024-06-02-19-47-06.png)

![](./img/2024-06-02-19-46-21.png)

![](./img/2024-06-02-19-47-34.png)

Next

![](./img/2024-06-02-19-50-06.png)

Next

![](./img/2024-06-02-19-50-51.png)

![](./img/2024-06-02-19-51-03.png)

![](./img/2024-06-02-19-51-41.png)

![](./img/2024-06-02-19-56-28.png)

Next

Create cluster

# MSK, Kafka Cient Setup
- WebServer SSH 접속
- Java Install
    ```bash
    cd ~
    mkdir media
    cd media
    wget https://corretto.aws/downloads/latest/amazon-corretto-21-x64-al2023-jdk.rpm
    wget https://corretto.aws/downloads/latest/amazon-corretto-21-x64-al2023-jre.rpm
    sudo yum localinstall -y amazon-corretto-21-x64-al2023-jdk.rpm amazon-corretto-21-x64-al2023-jre.rpm
    which java
    java -version
    ```
- Kafka client 설치
    ```bash
    cd ~
    cd media
    wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
    tar xvf kafka_2.13-3.5.1.tgz
    cd ..
    ln -s ./media/kafka_2.13-3.5.1 kafka
    ```
- kafka bootstrap 환경설정 (MSK_BOOTSTRAP_ADDRESS는 생성한 MSK 클러스터의 View client information 에서 확인)
    ![](./img/2024-06-02-22-59-08.png)
    ![](./img/2024-06-02-23-00-10.png)
    ```bash
    echo 'export MSK_BOOTSTRAP_ADDRESS=b-2.chiholeemsk.ie9kyn.c3.kafka.ap-northeast-2.amazonaws.com:9092,b-1.chiholeemsk.ie9kyn.c3.kafka.ap-northeast-2.amazonaws.com:9092' >> ~/.bash_profile
    echo 'export PATH=$PATH:$HOME/kafka/bin' >> ~/.bash_profile
    . ~/.bash_profile
    ```
- MSK Default Topic 확인
  - Unauthenticated access 로 접근, IAM 통한 접근은 별도 설정 필요
  - Common SG 가 SELF로 모든 포트에 대해 inbound rule 이 허용되어 있으므로 방화벽 설정은 필요없음
    ```bash
    kafka-topics.sh \
    --bootstrap-server $MSK_BOOTSTRAP_ADDRESS \
    --list
    ````
    ![](./img/2024-06-02-23-12-46.png)