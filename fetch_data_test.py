# %%
import requests
import pandas as pd
import sqlite3
import pickle
from datetime import datetime
import time
import os
import json
from urllib.parse import urlencode
import ssl
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import PoolManager

# Import configuration
from config import config

# %%
# SSL Custom Adapter for TLS 1.2 
class CustomHTTPAdapter(HTTPAdapter):
    def __init__(self, ssl_context=None, *args, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=self.ssl_context
        )

# Create SSL context from config
ssl_context = config.create_ssl_context()

# Create session with custom SSL adapter
session = Session()
adapter = CustomHTTPAdapter(ssl_context=ssl_context)
session.mount('https://', adapter)

# 수집할 날짜 설정 (예: 2024년 1월 1일)
exec_date = datetime(2024, 1, 1)

# 페이지네이션 초기화
page = 1
all_data = []

# %%
# 데이터 수집 루프
while True:
    # 요청 파라미터 생성
    params = config.get_request_params(exec_date.year, exec_date, page)
    
    # URL 생성
    query_string = urlencode(params)
    full_url = f"{config.api_base_url}?{query_string}"
    
    print(f"Requesting: {full_url}")
    
    # 요청 보내기 (인증 방식 유지: verify=False)
    response = session.post(full_url, headers=config.headers, verify=False)
    
    # 응답 코드 확인
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        break
    
    # JSON 데이터 파싱
    data = response.json()
    
    # 데이터가 없으면 종료
    if not data or isinstance(data, dict) and len(data) == 0:
        print("Empty response received, ending collection")
        break
    
    # Extract data from the nested structure
    row_data = []
    if 'QWGJK' in data:
        for item in data['QWGJK']:
            if 'row' in item:
                row_data.extend(item['row'])
        
        if row_data:
            print(f"Successfully retrieved {len(row_data)} records")
            all_data.extend(row_data)
        else:
            print(f"No data found in response")
            break
    else:
        print("Unexpected JSON structure. 'QWGJK' key not found.")
        break
    
    # 받아온 데이터가 MAX_RECORDS_PER_REQUEST보다 적으면 마지막 페이지
    if len(row_data) < config.api_max_records_per_request:
        print(f"Retrieved {len(row_data)} records (less than max {config.api_max_records_per_request}), likely at end")
        break
    
    # 다음 페이지 요청
    page += 1
    time.sleep(1)  # API 요청 간 딜레이

# %%
print(f"Total records collected: {len(all_data)}")

# %%
# 데이터 저장: pickle 파일
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs('data', exist_ok=True)
pickle_filename = f'data/crawled_data_{timestamp}.pkl'
with open(pickle_filename, 'wb') as f:
    pickle.dump(all_data, f)
print(f"Data saved to {pickle_filename}")

# 데이터 저장: SQLite 데이터베이스
conn = sqlite3.connect(config.db_name)
df = pd.DataFrame(all_data)
if 'exe_ymd' in df.columns:
    df['exe_ymd'] = pd.to_datetime(df['exe_ymd'])
df['crawled_at'] = datetime.now()
df.to_sql(config.db_table, conn, if_exists='append', index=False)
conn.close()
print(f"Data saved to SQLite database: {config.db_name}, table: {config.db_table}")