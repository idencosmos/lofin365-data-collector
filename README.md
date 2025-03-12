# 한국 지방재정 세부사업별 세출현황 데이터 수집기 (2016-2024)

이 프로젝트는 한국 지방재정 데이터 포털([lofin365.go.kr](https://www.lofin365.go.kr))에서 제공하는 API를 활용하여 세부사업별 세출현황 데이터를 수집하는 도구입니다. 2016년부터 2024년까지의 지방자치단체 재정 데이터를 일별 또는 월별로 수집하고 저장합니다.

# Korean Local Finance Data Collector (2016-2024)

This project is a tool for collecting detailed expenditure data by sub-projects from the Korean Local Finance Data Portal ([lofin365.go.kr](https://www.lofin365.go.kr)) API. It collects and stores financial data of local governments from 2016 to 2024 on a daily or monthly basis.

## 기능 개요 (Features)

### 한국어
- 연도별, 월별, 또는 특정 날짜별 데이터 수집
- 월말 데이터만 수집하거나 모든 일자 데이터 수집 옵션
- 페이지네이션 처리로 대용량 데이터 완전 수집
- 데이터 수집 결과를 Pickle 파일 및 SQLite 데이터베이스에 저장
- 재시도 메커니즘을 통한 안정적인 데이터 수집
- 불완전 수집 데이터 관리 및 재수집 기능
- 상세한 로깅 시스템

### English
- Data collection by year, month, or specific date
- Option to collect only month-end data or all dates
- Complete collection of large data sets through pagination
- Store data collection results in Pickle files and SQLite database
- Stable data collection through retry mechanism
- Management of incomplete collection data and re-collection feature
- Detailed logging system

## 설치 방법 (Installation)

### 한국어
이 프로젝트는 Poetry를 사용하여 의존성을 관리합니다.

#### 요구사항
- Python 3.12 이상
- Poetry (의존성 관리)

#### 설치 단계
1. 이 저장소를 복제합니다:
   ```bash
   git clone https://github.com/idencosmos/lofin365-data-collector.git
   cd lofin365-data-collector
   ```

2. Poetry를 사용하여 의존성을 설치합니다:
   ```bash
   poetry install
   ```

3. `.env.example` 파일을 `.env`로 복사하고 필요한 환경 변수를 설정합니다:
   ```bash
   cp .env.example .env
   # 편집기를 사용하여 .env 파일을 열고 API 키 등 필요한 정보를 입력합니다
   ```

### English
This project uses Poetry for dependency management.

#### Requirements
- Python 3.12 or higher
- Poetry (dependency management)

#### Installation Steps
1. Clone this repository:
   ```bash
   git clone https://github.com/idencosmos/lofin365-data-collector.git
   cd lofin365-data-collector
   ```

2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```

3. Copy the `.env.example` file to `.env` and set the required environment variables:
   ```bash
   cp .env.example .env
   # Open the .env file with your editor and enter the API key and other required information
   ```

## 사용 방법 (Usage)

### 한국어

기본적인 데이터 수집을 위한 명령어:
```bash
python fetch_local_finance_data.py
```

특정 연도 및 월 범위를 지정한 데이터 수집:
```bash
python fetch_local_finance_data.py --start-year 2022 --start-month 1 --end-year 2023 --end-month 12
```

모든 날짜의 데이터 수집 (월말 데이터만이 아님):
```bash
python fetch_local_finance_data.py --all-days
```

특정 날짜의 데이터만 수집:
```bash
python fetch_local_finance_data.py --date 2023-12-31
```

인터랙티브 모드로 실행 (대화형 입력):
```bash
python fetch_local_finance_data.py --interactive
```

불완전하게 수집된 데이터 재수집:
```bash
python fetch_local_finance_data.py --retry-incomplete
```

### English

Basic command for data collection:
```bash
python fetch_local_finance_data.py
```

Data collection with specific year and month range:
```bash
python fetch_local_finance_data.py --start-year 2022 --start-month 1 --end-year 2023 --end-month 12
```

Collect data for all dates (not just month-end):
```bash
python fetch_local_finance_data.py --all-days
```

Collect data for a specific date:
```bash
python fetch_local_finance_data.py --date 2023-12-31
```

Run in interactive mode:
```bash
python fetch_local_finance_data.py --interactive
```

Re-collect incompletely collected data:
```bash
python fetch_local_finance_data.py --retry-incomplete
```

## 데이터 구조 (Data Structure)

### 한국어
수집된 데이터는 다음과 같은 형식으로 저장됩니다:

- **Pickle 파일**: `data/` 디렉토리에 저장되는 Python 객체 직렬화 파일입니다. 일별, 월별, 연간 단위로 저장됩니다.
- **SQLite 데이터베이스**: `local_finance_data.db` 파일에 저장되는 관계형 데이터베이스입니다.

### English
The collected data is stored in the following formats:

- **Pickle Files**: Python object serialization files stored in the `data/` directory. They are saved by daily, monthly, and yearly units.
- **SQLite Database**: Relational database stored in the `local_finance_data.db` file.

## 로깅 (Logging)

### 한국어
프로그램 실행 중의 로그는 `logs/` 디렉토리에 저장됩니다.

- **일반 로그**: `logs/fetch_log_YYYYMMDD.log`
- **수집 요약 로그**: `logs/collection_count_YYYYMMDD.log`

### English
Logs during program execution are saved in the `logs/` directory.

- **General Logs**: `logs/fetch_log_YYYYMMDD.log`
- **Collection Summary Logs**: `logs/collection_count_YYYYMMDD.log`

## 라이선스 (License)

### 한국어
이 프로젝트는 MIT 라이선스로 배포됩니다.

### English
This project is distributed under the MIT license.

## 기여 (Contributing)

### 한국어
이슈와 풀 리퀘스트는 환영합니다. 주요 변경사항에 대해서는 먼저 이슈를 열어 논의해주세요.

### English
Issues and pull requests are welcome. Please open an issue first to discuss major changes.