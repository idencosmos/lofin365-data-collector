# %% 필요한 라이브러리 import
import os
import glob
import json
import pickle
import pandas as pd
import sqlite3
from datetime import datetime
from config import config

# %% 파일 처리 함수
def load_pickle_file(file_path):
    """피클 파일 로드"""
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    print(f"파일 로드 완료: {file_path}, 레코드 수: {len(data)}")
    return data

def find_monthly_data_files(data_dir, year=None, month=None, start_year=None, end_year=None):
    """월별 데이터 파일 찾기"""
    pattern = "monthly_*.pkl"
    
    if year is not None and month is not None:
        pattern = f"monthly_{year}_{month:02d}_*.pkl"
    elif year is not None:
        pattern = f"monthly_{year}_*.pkl"
    
    files = glob.glob(os.path.join(data_dir, pattern))
    
    # 연도 범위로 필터링
    if start_year is not None and end_year is not None:
        filtered_files = []
        for file in files:
            filename = os.path.basename(file)
            parts = filename.split('_')
            if len(parts) >= 3:
                file_year = int(parts[1])
                if start_year <= file_year <= end_year:
                    filtered_files.append(file)
        files = filtered_files
    
    return sorted(files)

# %% 데이터 결합 및 저장 함수
def combine_monthly_data(files):
    """월별 데이터 파일 합치기"""
    all_data = []
    
    for file in files:
        data = load_pickle_file(file)
        if data:
            all_data.extend(data)
    
    print(f"총 {len(files)}개 파일, {len(all_data)}개 레코드 결합 완료")
    return all_data

def save_to_sqlite(data, table_name):
    """데이터를 SQLite에 저장"""
    if not data:
        print("저장할 데이터가 없습니다.")
        return False
    
    try:
        conn = sqlite3.connect(config.db_name)
        df = pd.DataFrame(data)
        
        # 날짜 컬럼 변환
        if 'exe_ymd' in df.columns:
            df['exe_ymd'] = pd.to_datetime(df['exe_ymd'], format='%Y%m%d')
        
        # 숫자 컬럼 변환
        numeric_columns = ['bdg_cash_amt', 'bdg_ntep', 'capep', 'sggep', 'etc_amt', 'ep_amt', 'cpl_amt']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 처리 타임스탬프 추가
        df['processed_at'] = datetime.now()
        
        # SQLite에 저장
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"{len(df)}개 레코드를 {table_name} 테이블에 저장했습니다.")
        
        conn.close()
        return df
    except Exception as e:
        print(f"SQLite 저장 오류: {e}")
        return None

# %% 예시 사용법
"""
# 특정 연도 데이터 처리
data_dir = 'data'
year = 2023
files = find_monthly_data_files(data_dir, year=year)
combined_data = combine_monthly_data(files)
df = save_to_sqlite(combined_data, f"yearly_data_{year}")
summary = generate_summary_from_data(df, f"yearly_{year}")

# 연도 범위 데이터 처리
start_year = 2020
end_year = 2023
files = find_monthly_data_files(data_dir, start_year=start_year, end_year=end_year)
combined_data = combine_monthly_data(files)
df = save_to_sqlite(combined_data, f"range_data_{start_year}_{end_year}")
summary = generate_summary_from_data(df, f"range_{start_year}-{end_year}")
"""

data_dir = 'data'
for year in range(2016, 2025):
    files = find_monthly_data_files(data_dir, year=year)
    combined_data = combine_monthly_data(files)
    df = save_to_sqlite(combined_data, f"yearly_data_{year}")
