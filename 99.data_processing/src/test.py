import pyarrow.parquet as pq

def print_parquet_schema(file_path):
    # Parquet 파일 읽기
    parquet_file = pq.ParquetFile(file_path)
     
    # 스키마 출력
    print(parquet_file.schema)

# 예시 사용법
file_path = 'test.parquet'  # Parquet 파일 경로
print_parquet_schema(file_path)