# EMR

## EMR 클러스터 생성
![](./img/2024-06-16-11-57-04.png)

![](./img/2024-06-16-11-59-11.png)

![](./img/2024-06-16-11-59-45.png)

### Bootstrap 설정
아래 파일을 s3에 업로드 후 등록
![](./img/2024-06-16-12-02-45.png)
```bash
#!/bin/bash
sudo pip3 install -U boto3
sudo timedatectl set-timezone Asia/Seoul
```

### Iceberg 사용을 위한 설정
```bash
[
  {
    "Classification": "iceberg-defaults",
    "Properties": {
      "iceberg.enabled": "true"
    }
  }
]
```
![](./img/2024-06-16-11-57-57.png)

![](./img/2024-06-16-12-03-24.png)

![](./img/2024-06-16-12-04-48.png)

이 후 Iceberg의 bookmark 를 dynamoDB를 사용하므로 AmazonDynamoDBFullAccess 정책을 생성된 프로파일에 추가
![](./img/2024-06-16-12-05-01.png)


