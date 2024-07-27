
### Replication Subnet Group 생성
![](./img/2024-07-13-11-26-19.png)

### Replication Instance 생성
![](./img/2024-07-13-11-28-28.png)
![](./img/2024-07-13-11-28-38.png)
![](./img/2024-07-13-11-29-05.png)

### Source Endpoint (from mysql)
![](./img/2024-07-13-11-31-40.png)
![](./img/2024-07-13-11-31-57.png)

### Target Endpoint (to redshift)

[참고](https://docs.aws.amazon.com/ko_kr/dms/latest/userguide/CHAP_Target.Redshift.html)

> [!TIP] S3 Bucket이 필요한 이유
> AWS DMS Amazon S3 버킷을 사용하여 Amazon Redshift 데이터베이스로 데이터를 전송합니다. AWS DMS에서 버킷을 생성하는 경우, 콘솔은 IAM 역할인 dms-access-for-endpoint를 사용합니다. AWS CLI 또는 DMS API를 사용하여 대상 데이터베이스인 Amazon Redshift를 통해 데이터베이스 마이그레이션을 생성하는 경우 이 IAM 역할을 생성해야 합니다. 이 역할 생성에 관한 자세한 내용은 AWS CLI 및 AWS DMS API와 함께 사용할 IAM 역할 생성 섹션을 참조하십시오.

> [!WARNING] 
> AWS DMS는 대상 Amazon Redshift 인스턴스에서 BLOB, CLOB 및 NCLOB를 lVARCHAR로 변환합니다. Amazon Redshift는 64KB보다 큰 VARCHAR 데이터 형식을 지원하지 않으므로 Amazon Redshift에는 기존 LOB를 저장할 수 없습니다.

ServiceAccessRoleArn
![](./img/2024-07-13-11-52-50.png)

![](./img/2024-07-13-11-53-33.png)
![](./img/2024-07-13-12-54-22.png)

> [!TIP]
> dms-access-for-endpoint 는 Trust Relationships 에 dms, redshift 를 포함함

### Endpoint Test
![](./img/2024-07-13-11-55-45.png)
![](./img/2024-07-13-11-57-57.png)

### S3 Endpoint 생성
s3 endpoint 필요... 왜 필요하지? public subnet 인데....
![](./img/2024-07-13-15-38-40.png)
![](./img/2024-07-13-15-38-55.png)

### Intial Task 생성
![](./img/2024-07-13-12-02-06.png)
![](./img/2024-07-13-12-02-22.png)
![](./img/2024-07-13-12-02-40.png)
![](./img/2024-07-13-12-03-00.png)




### Intial Task 동작
![](./img/2024-07-13-12-05-46.png)
![](./img/2024-07-13-12-06-46.png)
![](./img/2024-07-13-12-12-04.png)
![](./img/2024-07-13-12-12-29.png)
![](./img/2024-07-13-12-12-54.png)

### CDD Task 생성
![](./img/2024-07-13-12-16-38.png)
![](./img/2024-07-13-12-16-53.png)
![](./img/2024-07-13-12-17-14.png)
![](./img/2024-07-13-12-17-37.png)



### 음...
initial load 후 cdc 를 수행할 때 기준이 될 seq number 를 찾기가 힘들다.
차라리 처음부터 initial load + cdc를 하는것이 맞겠다.

Cloudwatch Logging 옵션 선택 할것.. (캡쳐에는 없음)
![](./img/2024-07-13-16-10-06.png)
`Stop task after full load completes` 헷갈리는 옵션....
![](./img/2024-07-13-16-10-31.png)
![](./img/2024-07-13-16-10-57.png)
![](./img/2024-07-13-16-11-12.png)

#### inial load 시작
![](./img/2024-07-13-16-13-33.png)
full load 과정에서 발생된 insert, update 가 적용된건가... 
![](./img/2024-07-13-16-17-16.png)
#### CDC 시작
![](./img/2024-07-13-16-18-27.png)
정상 복제 진행 중
![](./img/2024-07-13-16-20-36.png)


### Source Table 변경 테스트

#### 테이블 생성
```sql
CREATE TABLE sales (
    sale_id INT NOT NULL,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    sale_date DATETIME NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, sale_date)
)
PARTITION BY RANGE (YEAR(sale_date)) (
    PARTITION p0 VALUES LESS THAN (2010),
    PARTITION p1 VALUES LESS THAN (2015),
    PARTITION p2 VALUES LESS THAN (2020),
    PARTITION p3 VALUES LESS THAN MAXVALUE
);
```

#### 데이터 입력
```sql
INSERT INTO sales (sale_id, customer_id, product_id, quantity, price, sale_date, status)
VALUES
(1, 1, 101, 2, 20.00, '2009-01-01 10:00:00', 'completed'),
(2, 2, 102, 1, 15.00, '2011-02-02 11:00:00', 'completed'),
(3, 3, 103, 3, 45.00, '2014-03-03 12:00:00', 'pending'),
(4, 4, 104, 5, 75.00, '2016-04-04 13:00:00', 'shipped'),
(5, 5, 105, 2, 30.00, '2018-05-05 14:00:00', 'completed'),
(6, 6, 106, 4, 60.00, '2019-06-06 15:00:00', 'completed'),
(7, 7, 107, 1, 10.00, '2020-07-07 16:00:00', 'returned'),
(8, 8, 108, 3, 90.00, '2021-08-08 17:00:00', 'completed'),
(9, 9, 109, 2, 40.00, '2022-09-09 18:00:00', 'completed'),
(10, 10, 110, 5, 100.00, '2023-10-10 19:00:00', 'pending'),
(11, 11, 111, 3, 45.00, '2008-11-11 20:00:00', 'completed'),
(12, 12, 112, 4, 55.00, '2009-12-12 21:00:00', 'shipped'),
(13, 13, 113, 2, 25.00, '2010-01-13 22:00:00', 'completed'),
(14, 14, 114, 1, 35.00, '2012-02-14 23:00:00', 'completed'),
(15, 15, 115, 5, 125.00, '2013-03-15 09:00:00', 'pending'),
(16, 16, 116, 3, 65.00, '2015-04-16 08:00:00', 'completed'),
(17, 17, 117, 4, 80.00, '2017-05-17 07:00:00', 'completed'),
(18, 18, 118, 2, 50.00, '2021-06-18 06:00:00', 'shipped'),
(19, 19, 119, 1, 20.00, '2022-07-19 05:00:00', 'returned'),
(20, 20, 120, 5, 150.00, '2023-08-20 04:00:00', 'completed');
```

#### 컬럼 추가 - 정상
```sql
ALTER TABLE sales ADD COLUMN shipping_address VARCHAR(255) AFTER status;
```

#### 컬럼 삭제 - 정상
```sql
ALTER TABLE sales DROP COLUMN shipping_address;
```

#### 파티션 삭제 - 오류
target 에서 파티션이 삭제되지 않으므로 데이터도 그대로 존재함
```sql
ALTER TABLE sales
DROP PARTITION p1;
```
![](./img/2024-07-13-17-31-18.png)
![](./img/2024-07-13-17-30-11.png)
![](./img/2024-07-13-17-38-19.png)
![](./img/2024-07-13-17-39-23.png)
 Table 'ecommerce'.'sales' was errored/suspended (subtask 0 thread 0)
 sales 테이블은 더 이상 복제 되지 않음.

트러블 슈팅 [참고](https://repost.aws/knowledge-center/dms-task-error-status)
오류 해결 후 오류 테이블 reload [참고](https://docs.aws.amazon.com/ko_kr/dms/latest/userguide/CHAP_Tasks.ReloadTables.html)

Reload 클릭
에러 복구 과정에서 전체 Reload 를 하네......... (redshift 의 sales 테이블 create time 확인하니..)
```sql
SELECT
	*
FROM SVV_TABLE_INFO
WHERE database = 'prod'
  AND schema = 'ecommerce'
ORDER BY tbl_rows;
```
![](./img/2024-07-13-17-45-59.png)
![](./img/2024-07-13-17-49-00.png)