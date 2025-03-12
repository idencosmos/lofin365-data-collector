import requests
import pandas as pd
import sqlite3
import pickle
from datetime import datetime, timedelta
import time
import os
import logging
import json
from urllib.parse import urlencode
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import PoolManager
import sys
import argparse
import calendar

# Import configuration
from config import config

# 로깅 설정
def setup_logging():
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # 일반 로그 파일
    today = datetime.now().strftime("%Y%m%d")
    log_file = f"{log_dir}/fetch_log_{today}.log"
    
    # 수집 비교 로그 파일 (간결한 형식)
    count_log_file = f"{log_dir}/collection_count_{today}.log"
    
    # 일반 로거 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    # 간결한 카운트 비교용 로거 설정
    count_logger = logging.getLogger('count_logger')
    count_logger.setLevel(logging.INFO)
    count_formatter = logging.Formatter('%(message)s')
    count_handler = logging.FileHandler(count_log_file)
    count_handler.setFormatter(count_formatter)
    count_logger.addHandler(count_handler)
    count_logger.propagate = False  # 부모 로거로 전달하지 않음
    
    return logging.getLogger(__name__), count_logger

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

# Create session with custom SSL settings
def create_session():
    ssl_context = config.create_ssl_context()
    
    session = Session()
    adapter = CustomHTTPAdapter(ssl_context=ssl_context)
    session.mount('https://', adapter)
    return session

def is_empty_response(response_json):
    """Check if response is empty (just '{}'). Returns True if empty."""
    if not response_json:
        return True
    
    if isinstance(response_json, dict) and len(response_json) == 0:
        return True
    
    # Check for empty QWGJK array or missing 'row' data
    if 'QWGJK' in response_json and (
        not response_json['QWGJK'] or 
        all('row' not in item or not item['row'] for item in response_json['QWGJK'])
    ):
        return True
        
    return False

def crawl_data(year, exec_date, logger, retry_count=0):
    """Crawl data for a given year and execution date."""
    all_data = []
    page = 1
    session = create_session()
    total_count = None
    empty_response_count = 0
    
    while True:
        try:
            params = config.get_request_params(year, exec_date, page)
            
            # Print complete URL with parameters
            logger.info("="*100)
            query_string = urlencode(params)
            full_url = f"{config.api_base_url}?{query_string}"
            logger.info(f"Complete URL: {full_url}")
            logger.info("="*100)
            
            # Use session with verify=False and full URL
            response = session.post(full_url, headers=config.headers, verify=False)
            
            # API 응답 코드 처리
            if response.status_code == 300:
                logger.error(f"Error: Required values missing for year {year}, date {exec_date}")
                break
            elif response.status_code == 500:
                logger.error(f"Error: Server internal error for year {year}, date {exec_date}")
                if retry_count < config.api_max_retries:
                    logger.info(f"Retrying in {config.api_retry_delay} seconds... (Attempt {retry_count + 1}/{config.api_max_retries})")
                    time.sleep(config.api_retry_delay)
                    return crawl_data(year, exec_date, logger, retry_count + 1)
                break
            elif response.status_code != 200:
                logger.error(f"Error: Received status code {response.status_code} for year {year}, date {exec_date}")
                if retry_count < config.api_max_retries:
                    logger.info(f"Retrying in {config.api_retry_delay} seconds... (Attempt {retry_count + 1}/{config.api_max_retries})")
                    time.sleep(config.api_retry_delay)
                    return crawl_data(year, exec_date, logger, retry_count + 1)
                break
            
            # Check for empty response
            response_text = response.text.strip()
            if response_text == '{}' or len(response_text) < 5:
                logger.info(f"Page {page}: Empty response received ({{}})")
                empty_response_count += 1
                
                # If we've received 2 consecutive empty responses, assume we're done
                if empty_response_count >= 2:
                    logger.info("Multiple empty responses received, assuming all data collected")
                    break
                    
                page += 1
                time.sleep(1)
                continue
                
            # Reset empty response counter if we got a valid response
            empty_response_count = 0
            
            # Parse the JSON response
            try:
                response_json = response.json()
            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON response: {response.text[:200]}...")
                if retry_count < config.api_max_retries:
                    logger.info(f"Retrying in {config.api_retry_delay} seconds... (Attempt {retry_count + 1}/{config.api_max_retries})")
                    time.sleep(config.api_retry_delay)
                    return crawl_data(year, exec_date, logger, retry_count + 1)
                break
                
            # Check for empty JSON response
            if is_empty_response(response_json):
                logger.info(f"Page {page}: Empty JSON response received")
                empty_response_count += 1
                
                # If we've already collected data and now getting empty responses, we're done
                if all_data and empty_response_count >= 1:
                    logger.info("Empty JSON response after collecting data, assuming all data collected")
                    break
                
                page += 1
                time.sleep(1)
                continue
            
            # Extract data from the nested structure
            if 'QWGJK' in response_json:
                # 첫 요청에서 전체 데이터 개수 확인
                if page == 1 and response_json['QWGJK'][0].get('head'):
                    metadata = response_json['QWGJK'][0]['head']
                    for meta_item in metadata:
                        if 'list_total_count' in meta_item:
                            total_count = meta_item['list_total_count']
                            logger.info(f"Total records available: {total_count}")
                
                # 데이터 추출
                row_data = []
                for item in response_json['QWGJK']:
                    if 'row' in item:
                        row_data.extend(item['row'])
                
                if row_data:
                    logger.info(f"Page {page}: Successfully retrieved {len(row_data)} records")
                    all_data.extend(row_data)
                else:
                    logger.info(f"Page {page}: No data found in response")
                    empty_response_count += 1
                
                # 데이터를 모두 수집했는지 확인
                if total_count is not None:
                    logger.info(f"Collected {len(all_data)}/{total_count} records so far")
                    
                    # 데이터 수집이 거의 완료되었는지 확인 (99.5% 이상 수집)
                    collection_percentage = len(all_data) / total_count * 100
                    logger.info(f"Collection progress: {collection_percentage:.2f}%")
                    
                    if len(all_data) >= total_count:
                        logger.info(f"All records collected for {year}, {exec_date}")
                        
                        # 데이터가 모두 수집되었는지 확인하기 위해 다음 페이지 요청해보고 비어있는지 확인
                        next_page = page + 1
                        next_params = config.get_request_params(year, exec_date, next_page)
                        query_string = urlencode(next_params)
                        next_url = f"{config.api_base_url}?{query_string}"
                        
                        logger.info(f"Verifying collection completion by checking page {next_page}...")
                        next_response = session.post(next_url, headers=config.headers, verify=False)
                        
                        if next_response.status_code == 200:
                            next_text = next_response.text.strip()
                            if next_text == '{}' or len(next_text) < 5:
                                logger.info(f"Verification successful: Page {next_page} returns empty response")
                                break
                            
                            try:
                                next_json = next_response.json()
                                if is_empty_response(next_json):
                                    logger.info(f"Verification successful: Page {next_page} returns empty JSON")
                                    break
                                else:
                                    logger.warning(f"Verification failed: Page {next_page} contains more data!")
                                    # Continue to next page to collect remaining data
                                    page = next_page
                                    continue
                            except json.JSONDecodeError:
                                logger.info("Verification successful: Next page returns invalid JSON (likely empty)")
                                break
                        else:
                            logger.info(f"Verification: Next page returns status code {next_response.status_code}")
                            break
                    
                    # If collection is close but not exact, and no data on current page, we can end
                    if collection_percentage > 99.5 and not row_data:
                        logger.info(f"Collection at {collection_percentage:.2f}% with no more data on current page. Ending collection.")
                        break
                
                # 현재 페이지에 데이터가 없거나 MAX_RECORDS_PER_REQUEST보다 적으면 거의 수집 완료
                if not row_data:
                    empty_response_count += 1
                    if empty_response_count >= 2:  # Two consecutive empty pages
                        logger.info("Multiple empty pages received, ending collection")
                        break
                elif len(row_data) < config.api_max_records_per_request:
                    logger.info(f"Page {page} has {len(row_data)} records (less than max {config.api_max_records_per_request}), likely at end")
                    
                    # Verify by checking next page
                    next_page = page + 1
                    next_params = config.get_request_params(year, exec_date, next_page)
                    query_string = urlencode(next_params)
                    next_url = f"{config.api_base_url}?{query_string}"
                    
                    logger.info(f"Checking if page {next_page} is empty...")
                    next_response = session.post(next_url, headers=config.headers, verify=False)
                    
                    if next_response.status_code == 200:
                        next_text = next_response.text.strip()
                        if next_text == '{}' or len(next_text) < 5:
                            logger.info(f"Confirmed: Page {next_page} is empty")
                            break
                        
                        try:
                            next_json = next_response.json()
                            if is_empty_response(next_json):
                                logger.info(f"Confirmed: Page {next_page} has empty JSON structure")
                                break
                            else:
                                logger.warning(f"Unexpected: Page {next_page} contains more data!")
                                # Continue to next page
                                page = next_page
                                continue
                        except json.JSONDecodeError:
                            logger.info("Next page returns invalid JSON (likely empty)")
                            break
                    else:
                        logger.info(f"Next page returns status code {next_response.status_code}")
                        break
                
                page += 1
                time.sleep(1)  # Avoid hitting rate limits
            else:
                logger.error("Unexpected JSON structure. 'QWGJK' key not found.")
                logger.error(f"Response content: {response_json}")
                break
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error occurred: {e}")
            if retry_count < config.api_max_retries:
                logger.info(f"Retrying in {config.api_retry_delay} seconds... (Attempt {retry_count + 1}/{config.api_max_retries})")
                time.sleep(config.api_retry_delay)
                return crawl_data(year, exec_date, logger, retry_count + 1)
            break
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}")
            import traceback
            logger.error(traceback.format_exc())
            if retry_count < config.api_max_retries:
                logger.info(f"Retrying in {config.api_retry_delay} seconds... (Attempt {retry_count + 1}/{config.api_max_retries})")
                time.sleep(config.api_retry_delay)
                return crawl_data(year, exec_date, logger, retry_count + 1)
            break
    
    # 검증: 데이터 수집 완료 확인
    if total_count is not None and len(all_data) < total_count:
        completion_rate = len(all_data) / total_count * 100
        logger.warning(f"⚠️ Incomplete data collection: {len(all_data)}/{total_count} records ({completion_rate:.2f}%)")
        
        # 추가적인 재시도가 필요하다고 표시
        if completion_rate < 99.5 and retry_count < config.api_max_retries:
            logger.info(f"Will attempt to retry this date later (completion rate: {completion_rate:.2f}%)")
            # 불완전 수집을 기록
            return all_data, total_count, False
    
    return all_data, total_count, True if not total_count or len(all_data) >= total_count * 0.995 else False

def save_to_pickle(data, filename, logger):
    """Save data to pickle file with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_filename = f'data/{filename}_{timestamp}.pkl'
    with open(full_filename, 'wb') as f:
        pickle.dump(data, f)
    logger.info(f"Data saved to {full_filename}")
    return full_filename

def save_to_sqlite(data, logger):
    """Save data to SQLite database with proper schema."""
    if not data:
        logger.warning("No data to save to database")
        return
        
    conn = sqlite3.connect(config.db_name)
    df = pd.DataFrame(data)
    
    # Convert date columns if they exist
    if 'exe_ymd' in df.columns:
        df['exe_ymd'] = pd.to_datetime(df['exe_ymd'], format='%Y%m%d')
    
    # Convert numeric columns
    numeric_columns = ['bdg_cash_amt', 'bdg_ntep', 'capep', 'sggep', 'etc_amt', 'ep_amt', 'cpl_amt']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Add crawling timestamp
    df['crawled_at'] = datetime.now()
    
    df.to_sql(config.db_table, conn, if_exists='append', index=False)
    logger.info(f"Saved {len(df)} records to database {config.db_name}")
    conn.close()

def load_incomplete_dates():
    """불완전 수집 날짜 데이터를 로드합니다."""
    incomplete_file = 'data/incomplete_dates.json'
    if os.path.exists(incomplete_file):
        with open(incomplete_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_incomplete_dates(incomplete_dates):
    """불완전 수집 날짜 데이터를 저장합니다."""
    incomplete_file = 'data/incomplete_dates.json'
    with open(incomplete_file, 'w', encoding='utf-8') as f:
        json.dump(incomplete_dates, f, ensure_ascii=False, indent=2)

def get_last_day_of_month(year, month):
    """Return the last day of the specified month for the given year."""
    last_day = calendar.monthrange(year, month)[1]
    return datetime(year, month, last_day)

def main():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Local Finance Data Collector')
    parser.add_argument('--start-year', type=int, help='Start year for data collection')
    parser.add_argument('--end-year', type=int, help='End year for data collection')
    parser.add_argument('--start-month', type=int, choices=range(1, 13), help='Start month (1-12) for data collection')
    parser.add_argument('--end-month', type=int, choices=range(1, 13), help='End month (1-12) for data collection')
    parser.add_argument('--date', type=str, help='Specific date to collect (YYYY-MM-DD)')
    parser.add_argument('--retry-incomplete', action='store_true', help='Retry collecting data for incomplete dates')
    parser.add_argument('--all-days', action='store_true', help='Collect data for all days instead of just month-end')
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode with prompts')
    args = parser.parse_args()
    
    # Interactive mode - prompt for inputs
    if args.interactive:
        print("\n===== 지방재정 데이터 수집기 =====")
        
        # 데이터 수집 방식 선택
        print("\n데이터 수집 방식을 선택하세요:")
        print("1) 월별 마지막 날짜만 수집 (기본)")
        print("2) 모든 날짜 수집")
        collection_choice = input("선택 (1 또는 2): ").strip()
        args.all_days = True if collection_choice == "2" else False
        
        # 연도 및 월 범위 입력
        default_start_year = config.data_start_year
        default_end_year = config.data_end_year
        
        print(f"\n데이터 수집 기간을 입력하세요:")
        start_year_input = input(f"시작 연도 ({default_start_year}): ").strip()
        start_month_input = input("시작 월 (1-12, 기본: 1): ").strip()
        end_year_input = input(f"종료 연도 ({default_end_year}): ").strip()
        end_month_input = input("종료 월 (1-12, 기본: 12): ").strip()
        
        args.start_year = int(start_year_input) if start_year_input else default_start_year
        args.start_month = int(start_month_input) if start_month_input and 1 <= int(start_month_input) <= 12 else 1
        args.end_year = int(end_year_input) if end_year_input else default_end_year
        args.end_month = int(end_month_input) if end_month_input and 1 <= int(end_month_input) <= 12 else 12
        
        print(f"\n설정 정보:")
        print(f"- 수집 방식: {'모든 날짜' if args.all_days else '월별 마지막 날짜만'}")
        print(f"- 데이터 기간: {args.start_year}년 {args.start_month}월부터 {args.end_year}년 {args.end_month}월까지")
        
        confirm = input("\n위 설정으로 데이터를 수집하시겠습니까? (y/n): ").lower().strip()
        if confirm != 'y':
            print("데이터 수집이 취소되었습니다.")
            sys.exit(0)
    
    start_year = args.start_year if args.start_year else config.data_start_year
    end_year = args.end_year if args.end_year else config.data_end_year
    start_month = args.start_month if args.start_month else 1
    end_month = args.end_month if args.end_month else 12
    
    # 로깅 설정
    logger, count_logger = setup_logging()
    logger.info("Starting data collection process")
    logger.info(f"Configuration: START_YEAR={start_year}, START_MONTH={start_month}, END_YEAR={end_year}, END_MONTH={end_month}, ALL_DAYS={'Yes' if args.all_days else 'No'}")
    
    # 불완전 수집 날짜 목록 로드
    incomplete_dates = load_incomplete_dates()
    
    # 헤더 라인 추가
    count_logger.info(f"{'Date':10} | {'Year':4} | {'Expected':8} | {'Collected':8} | {'Match':5} | {'Success Rate':11} | {'Status'}")
    count_logger.info("-" * 80)
    
    if not config.api_key:
        logger.error("API key not found in environment variables")
        raise ValueError("API key not found in environment variables")
        
    all_crawled_data = []
    collection_summary = []
    new_incomplete_dates = []
    
    # Create directory for data if it doesn't exist
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    # 특정 날짜만 처리
    if args.date:
        try:
            specific_date = datetime.strptime(args.date, "%Y-%m-%d")
            year = specific_date.year
            
            logger.info(f"Collecting data for specific date: {args.date}")
            date_str = specific_date.strftime("%Y-%m-%d")
            
            data, total_count, is_complete = crawl_data(year, specific_date, logger)
            
            # 로그 출력 및 요약
            match_status = "✓" if total_count == len(data) else "✗"
            success_rate = f"{(len(data) / total_count * 100):.2f}%" if total_count else "N/A"
            status = "SUCCESS" if is_complete else "INCOMPLETE" if total_count else "NO DATA"
            
            count_logger.info(f"{date_str:10} | {year:4} | {total_count or 0:8} | {len(data):8} | {match_status:5} | {success_rate:11} | {status}")
            
            # 수집 결과 저장
            if data:
                daily_filename = save_to_pickle(data, f'daily_{year}_{date_str}', logger)
                save_to_sqlite(data, logger)
                
            # 불완전 수집 기록
            if not is_complete:
                new_incomplete_dates.append({
                    'date': date_str,
                    'year': year,
                    'expected': total_count,
                    'collected': len(data),
                    'last_attempt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        finally:
            # 현재 함수 종료
            sys.exit(0)
    
    # 불완전 수집 데이터 재시도
    if args.retry_incomplete:
        if not incomplete_dates:
            logger.info("No incomplete dates to retry.")
        else:
            logger.info(f"Retrying {len(incomplete_dates)} incomplete dates...")
            
            for item in incomplete_dates:
                date_str = item['date']
                year = item['year']
                expected = item['expected']
                
                logger.info(f"Retrying data collection for {date_str} (previous: {item['collected']}/{expected})")
                
                specific_date = datetime.strptime(date_str, "%Y-%m-%d")
                data, total_count, is_complete = crawl_data(year, specific_date, logger)
                
                # 로그 출력 및 요약
                match_status = "✓" if total_count == len(data) else "✗"
                success_rate = f"{(len(data) / total_count * 100):.2f}%" if total_count else "N/A"
                status = "SUCCESS" if is_complete else "INCOMPLETE" if total_count else "NO DATA"
                
                count_logger.info(f"{date_str:10} | {year:4} | {total_count or 0:8} | {len(data):8} | {match_status:5} | {success_rate:11} | {status}")
                
                # 수집 결과 저장
                if data:
                    daily_filename = save_to_pickle(data, f'retry_daily_{year}_{date_str}', logger)
                    save_to_sqlite(data, logger)
                    
                    # 연속된 데이터에도 추가
                    all_crawled_data.extend(data)
                
                # 여전히 불완전하면 목록에 유지
                if not is_complete:
                    new_incomplete_dates.append({
                        'date': date_str,
                        'year': year,
                        'expected': total_count,
                        'collected': len(data),
                        'last_attempt': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'previous_attempts': item.get('previous_attempts', 0) + 1
                    })
            
            # 재시도 결과 저장
            save_incomplete_dates(new_incomplete_dates)
            sys.exit(0)
    
    # Iterate over years
    for year in range(start_year, end_year + 1):
        year_data = []
        year_summary = []
        
        # Set month range based on start_year and end_year
        month_start = start_month if year == start_year else 1
        month_end = end_month if year == end_year else 12
        
        logger.info(f"Processing year {year}, months {month_start}-{month_end}")
        
        # If --all-days flag is used, collect all days as before
        if args.all_days:
            # Iterate over execution dates with month constraints
            start_date = datetime(year, month_start, 1)
            # Calculate end date based on the last day of end_month
            last_day = calendar.monthrange(year, month_end)[1]
            end_date = datetime(year, month_end, last_day)
            
            current_date = start_date
            
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                logger.info(f"Crawling data for year {year}, date {date_str}")
                
                data, total_count, is_complete = crawl_data(year, current_date, logger)
                
                # 간결한 로그 출력
                match_status = "✓" if is_complete else "✗"
                success_rate = f"{(len(data) / total_count * 100):.2f}%" if total_count else "N/A"
                status = "SUCCESS" if is_complete else "INCOMPLETE" if total_count else "NO DATA"
                
                # 한 줄로 결과 로그 기록 
                count_logger.info(f"{date_str:10} | {year:4} | {total_count or 0:8} | {len(data):8} | {match_status:5} | {success_rate:11} | {status}")
                
                # 수집 결과 요약 저장
                summary_item = {
                    'year': year,
                    'date': date_str,
                    'total_expected': total_count,
                    'total_collected': len(data),
                    'success_rate': (len(data) / total_count * 100) if total_count else None,
                    'is_complete': is_complete,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                year_summary.append(summary_item)
                collection_summary.append(summary_item)
                
                # 불완전 수집 기록
                if not is_complete and total_count is not None and len(data) > 0:
                    new_incomplete_dates.append({
                        'date': date_str,
                        'year': year,
                        'expected': total_count,
                        'collected': len(data),
                        'last_attempt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                
                if data:
                    year_data.extend(data)
                    
                # 일일 데이터 백업
                if data:
                    daily_filename = save_to_pickle(data, f'daily_{year}_{date_str}', logger)
                    logger.info(f"Daily data for {date_str} saved to {daily_filename}")
                
                current_date += timedelta(days=1)
        else:
            # Collect data only for the last day of each month
            logger.info(f"Collecting data for year {year}, months {month_start}-{month_end}, last day of each month only")
            
            for month in range(month_start, month_end + 1):
                # Get the last day of the current month
                current_date = get_last_day_of_month(year, month)
                date_str = current_date.strftime("%Y-%m-%d")
                
                logger.info(f"Crawling data for end of month: {date_str}")
                
                data, total_count, is_complete = crawl_data(year, current_date, logger)
                
                # 간결한 로그 출력
                match_status = "✓" if is_complete else "✗"
                success_rate = f"{(len(data) / total_count * 100):.2f}%" if total_count else "N/A"
                status = "SUCCESS" if is_complete else "INCOMPLETE" if total_count else "NO DATA"
                
                # 한 줄로 결과 로그 기록 
                count_logger.info(f"{date_str:10} | {year:4} | {total_count or 0:8} | {len(data):8} | {match_status:5} | {success_rate:11} | {status}")
                
                # 수집 결과 요약 저장
                summary_item = {
                    'year': year,
                    'date': date_str,
                    'total_expected': total_count,
                    'total_collected': len(data),
                    'success_rate': (len(data) / total_count * 100) if total_count else None,
                    'is_complete': is_complete,
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                year_summary.append(summary_item)
                collection_summary.append(summary_item)
                
                # 불완전 수집 기록
                if not is_complete and total_count is not None and len(data) > 0:
                    new_incomplete_dates.append({
                        'date': date_str,
                        'year': year,
                        'expected': total_count,
                        'collected': len(data),
                        'last_attempt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                
                if data:
                    year_data.extend(data)
                    
                # 월말 데이터 백업
                if data:
                    monthly_filename = save_to_pickle(data, f'monthly_{year}_{month:02d}', logger)
                    logger.info(f"Monthly data for {year}-{month:02d} saved to {monthly_filename}")
        
        # 연간 데이터 백업 (부분적일 수 있음)
        if year_data:
            # 일부 월만 수집한 경우 파일 이름 변경
            if year == start_year and month_start > 1 or year == end_year and month_end < 12:
                if year == start_year and year == end_year and month_start > 1 and month_end < 12:
                    yearly_filename = save_to_pickle(year_data, f'yearly_{year}_{month_start:02d}-{month_end:02d}', logger)
                elif year == start_year and month_start > 1:
                    yearly_filename = save_to_pickle(year_data, f'yearly_{year}_{month_start:02d}-12', logger)
                elif year == end_year and month_end < 12:
                    yearly_filename = save_to_pickle(year_data, f'yearly_{year}_01-{month_end:02d}', logger)
            else:
                yearly_filename = save_to_pickle(year_data, f'yearly_{year}', logger)
                
            logger.info(f"Data for year {year} saved to {yearly_filename}")
            all_crawled_data.extend(year_data)
        
        # 연간 수집 요약 저장
        if year == start_year and year == end_year and (month_start > 1 or month_end < 12):
            summary_filename = f'data/collection_summary_{year}_{month_start:02d}-{month_end:02d}.json'
        elif year == start_year and month_start > 1:
            summary_filename = f'data/collection_summary_{year}_{month_start:02d}-12.json'
        elif year == end_year and month_end < 12:
            summary_filename = f'data/collection_summary_{year}_01-{month_end:02d}.json'
        else:
            summary_filename = f'data/collection_summary_{year}.json'
            
        with open(summary_filename, 'w', encoding='utf-8') as f:
            json.dump(year_summary, f, ensure_ascii=False, indent=2)
        logger.info(f"Collection summary for year {year} saved to {summary_filename}")
    
    # 수집 범위 정보 문자열
    if start_year == end_year:
        if month_start == 1 and month_end == 12:
            collection_range = f"{start_year}"
        else:
            collection_range = f"{start_year}_{month_start:02d}-{month_end:02d}"
    else:
        if month_start == 1 and month_end == 12:
            collection_range = f"{start_year}-{end_year}"
        else:
            collection_range = f"{start_year}_{month_start:02d}-{end_year}_{month_end:02d}"

    # 전체 데이터 SQLite에 저장
    if all_crawled_data:
        save_to_sqlite(all_crawled_data, logger)
        logger.info("All data saved to SQLite database.")
    else:
        logger.warning("No data was collected.")
    
    # 전체 수집 요약 저장
    summary_filename = f'data/collection_summary_{collection_range}.json'
    with open(summary_filename, 'w', encoding='utf-8') as f:
        json.dump(collection_summary, f, ensure_ascii=False, indent=2)
    logger.info(f"Overall collection summary saved to {summary_filename}")
    
    # 불완전 수집 날짜 저장
    if new_incomplete_dates:
        save_incomplete_dates(new_incomplete_dates)
        logger.warning(f"⚠️ Detected {len(new_incomplete_dates)} dates with incomplete data collection")
        logger.info("To retry these dates later, run with --retry-incomplete")
    
    logger.info("Data collection process completed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        # 불완전 데이터 저장 처리
        logger = logging.getLogger(__name__)
        logger.error("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"An error occurred: {e}")
        import traceback
        logger.error(traceback.format_exc())