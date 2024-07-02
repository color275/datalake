DBT Overview
![](2024-07-01-09-24-10.png)

Elastic IP
![](2024-06-30-22-19-24.png)
![](2024-06-30-22-19-52.png)

Redshift Cluster 생성
![](2024-06-30-22-20-22.png)
![](2024-06-30-22-20-38.png)
![](2024-06-30-22-20-50.png)
![](2024-06-30-22-21-25.png)
![](2024-06-30-22-29-12.png)
![](2024-06-30-22-29-31.png)

endpoint : dbt-redshift.cncx5aab3wic.ap-northeast-2.redshift.amazonaws.com

> [!TIP]
> ssh -i /Users/chiholee/Desktop/Project/keys/chiholee-datalake02.pem -L 15439:chiholee-redshift-cluster-1.cncx5aab3wic.ap-northeast-2.redshift.amazonaws.com:5439 ec2-user@43.203.213.41


데이터베이스 생성
```sql
create database first_dbt;
````


dbt 환경 셋업
```bash
mkdir chiholee_dbt
python3.10 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install dbt-core
pip install dbt-redshift
```

```bash
dbt init first_dbt
Which database would you like to use? 1
[1] redshift
[2] postgres

host (hostname.region.redshift.amazonaws.com): dbt-redshift.cncx5aab3wic.ap-northeast-2.redshift.amazonaws.com

port [5439]: 5439

user (dev username): admin

[1] password
[2] iam
Desired authentication method option (enter a number): 1

password (dev password): Admin1234

dbname (default database that dbt will build objects in): first_dbt

schema (default schema that dbt will build objects in): dev

threads (1 or more) [1]: 1

14:00:43  Profile first_dbt written to /Users/chiholee/.dbt/profiles.yml using target's profile_template.yml and your supplied values. Run 'dbt debug' to validate the connection.
```

설치 확인
```bash
cd first_dbt
dbt debug
```
![](2024-06-30-23-04-51.png)


DBT 디렉토리 구조
![](2024-06-30-23-06-25.png)

raw_layer 데이터 생성
```sql
create schema raw_layer;

CREATE TABLE raw_layer.raw_listings
                    (id integer,
                     listing_url varchar(256),
                     name varchar(256),
                     room_type varchar(256),
                     minimum_nights integer,
                     host_id integer,
                     price varchar(256),
                     created_at timestamp,
                     updated_at timestamp);



COPY raw_layer.raw_listings (id,
                        listing_url,
                        name,
                        room_type,
                        minimum_nights,
                        host_id,
                        price,
                        created_at,
                        updated_at)
                   from 's3://dbtlearn/listings.csv'                   
                   DELIMITER ','
                   REGION 'us-east-2'
                   TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS' 
                   IAM_ROLE default
                   format as CSV
                   IGNOREHEADER as 1;
                    

CREATE TABLE raw_layer.raw_reviews
                    (listing_id integer,
                     date timestamp,
                     reviewer_name varchar(256),
                     comments varchar(65535),
                     sentiment varchar(256));
                    
COPY raw_layer.raw_reviews (listing_id, date, reviewer_name, comments, sentiment)
                   from 's3://dbtlearn/reviews.csv'
                   DELIMITER ','
                   REGION 'us-east-2'
                   TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS' 
                   IAM_ROLE default
                   format as CSV
                   IGNOREHEADER as 1;
                    

CREATE TABLE raw_layer.raw_hosts
                    (id integer,
                     name varchar(256),
                     is_superhost varchar(256),
                     created_at timestamp,
                     updated_at timestamp);
                    
COPY raw_layer.raw_hosts (id, name, is_superhost, created_at, updated_at)
                   from 's3://dbtlearn/hosts.csv'
                   DELIMITER ','
                   REGION 'us-east-2'
                   IAM_ROLE default
                   format as CSV
                   IGNOREHEADER as 1;
```
![](2024-07-01-09-58-14.png)
![](2024-07-01-09-57-56.png)


stg model 구성
![](2024-07-01-10-00-50.png)
models/stg/stg_hosts.sql
```sql
WITH raw_hosts AS (
    SELECT
        *
    FROM
       first_dbt.raw_layer.raw_hosts
)
SELECT
    id AS host_id,
    NAME AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts
```

models/stg/stg_listings.sql
```sql
WITH raw_listings AS (
    SELECT
        *
    FROM
        first_dbt.raw_layer.raw_listings
)
SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
FROM
    raw_listings
```

models/stg/stg_reviews.sql
```sql
WITH raw_reviews AS (
    SELECT
        *
    FROM
        first_dbt.raw_layer.raw_reviews
)
SELECT
    listing_id,
    date AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment
FROM
    raw_reviews
```











































- ref
https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero/blob/main/_course_resources/course-resources.md

ssh -i /Users/chiholee/Desktop/Project/keys/chiholee-datalake02.pem -L 15439:chiholee-redshift-cluster-1.cncx5aab3wic.ap-northeast-2.redshift.amazonaws.com:5439 ec2-user@43.203.213.41


```sql
-- Create our database and schemas
CREATE DATABASE AIRBNB;
CREATE SCHEMA AIRBNB.RAW_DATA;


-- Create our three tables
CREATE TABLE raw_data.raw_listings
                    (id integer,
                     listing_url varchar(256),
                     name varchar(256),
                     room_type varchar(256),
                     minimum_nights integer,
                     host_id integer,
                     price varchar(256),
                     created_at timestamp,
                     updated_at timestamp);



COPY raw_data.raw_listings (id,
                        listing_url,
                        name,
                        room_type,
                        minimum_nights,
                        host_id,
                        price,
                        created_at,
                        updated_at)
                   from 's3://dbtlearn/listings.csv'                   
                   DELIMITER ','
                   REGION 'us-east-2'
                   TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS' 
                   IAM_ROLE default
                   format as CSV
                   IGNOREHEADER as 1;
                    

CREATE TABLE raw_data.raw_reviews
                    (listing_id integer,
                     date timestamp,
                     reviewer_name varchar(256),
                     comments varchar(65535),
                     sentiment varchar(256));
                    
COPY raw_data.raw_reviews (listing_id, date, reviewer_name, comments, sentiment)
                   from 's3://dbtlearn/reviews.csv'
                   DELIMITER ','
                   REGION 'us-east-2'
                   TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS' 
                   IAM_ROLE default
                   format as CSV
                   IGNOREHEADER as 1;
                    

CREATE TABLE raw_data.raw_hosts
                    (id integer,
                     name varchar(256),
                     is_superhost varchar(256),
                     created_at timestamp,
                     updated_at timestamp);
                    
COPY raw_data.raw_hosts (id, name, is_superhost, created_at, updated_at)
                   from 's3://dbtlearn/hosts.csv'
                   DELIMITER ','
                   REGION 'us-east-2'
                   IAM_ROLE default
                   format as CSV
                   IGNOREHEADER as 1;

select *
from raw_data.raw_listings;

select *
from raw_data.raw_reviews;

select *
from raw_data.raw_hosts;
```

```bash
python3.10 -m venv .venv
pip install --upgrade pip
pip install dbt-core
pip install dbt-redshift
```

```bash
dbt init dbtlearn
````
```bash
(.venv) ➜  12.dbt git:(main) ✗ cat ~/.dbt/profiles.yml
dbtlearn:
  outputs:
    dev:
      cluster_id: chiholee-redshift-cluster-1
      dbname: airbnb
      host: 0.0.0.0
      password: Admin1234
      port: 15439
      schema: dev
      threads: 1
      type: redshift
      user: admin
  target: dev
```

```bash
dbt debug
15:01:28  Running with dbt=1.8.3
15:01:28  dbt version: 1.8.3
15:01:28  python version: 3.10.11
15:01:28  python path: /Users/chiholee/Desktop/Project/datalake/12.dbt/.venv/bin/python3.10
15:01:28  os info: macOS-14.5-arm64-arm-64bit
15:01:28  Using profiles dir at /Users/chiholee/.dbt
15:01:28  Using profiles.yml file at /Users/chiholee/.dbt/profiles.yml
15:01:28  Using dbt_project.yml file at /Users/chiholee/Desktop/Project/datalake/12.dbt/dbtlearn/dbt_project.yml
15:01:28  adapter type: redshift
15:01:28  adapter version: 1.8.1
15:01:28  Configuration:
15:01:28    profiles.yml file [OK found and valid]
15:01:28    dbt_project.yml file [OK found and valid]
15:01:28  Required dependencies:
15:01:28   - git [OK found]

15:01:28  Connection:
15:01:28    host: 0.0.0.0
15:01:28    user: admin
15:01:28    port: 15439
15:01:28    database: airbnb
15:01:28    method: database
15:01:28    cluster_id: chiholee-redshift-cluster-1
15:01:28    iam_profile: None
15:01:28    schema: dev
15:01:28    sslmode: prefer
15:01:28    region: None
15:01:28    sslmode: prefer
15:01:28    region: None
15:01:28    iam_profile: None
15:01:28    autocreate: False
15:01:28    db_groups: []
15:01:28    ra3_node: False
15:01:28    connect_timeout: None
15:01:28    role: None
15:01:28    retries: 1
15:01:28    autocommit: True
15:01:28  Registered adapter: redshift=1.8.1
15:01:29    Connection test: [OK connection ok]

15:01:29  All checks passed!
````




![](2024-06-29-00-08-15.png)