# Local Finance Data Collector (lofin365-data-collector)

지방재정 데이터 수집 및 분석 도구입니다. 세부사업별 세출현황(2016-2024) 데이터를 수집하고 분석합니다.

## 기능

### 데이터 수집
- 지방재정 365 포털에서 세부사업별 세출현황 데이터 수집
- 일별/월별 데이터 수집 지원
- 데이터 수집 이력 관리 및 불완전 수집 데이터 재시도 기능
- SQLite 데이터베이스와 피클 파일로 데이터 저장

### 데이터 분석
- 연도별/월별 세부사업 분석
- 다년도 통합 분석
- 지역별/분야별/부문별 분석
- 계절성 분석
- 이상치 탐지
- 전년 대비 증감률 분석

### 보고서 생성
- 엑셀 보고서 자동 생성
- 차트 및 그래프 시각화
- 데이터 검증 결과 포함
- 다양한 분석 시트 제공

## 설치 방법

이 프로젝트는 Poetry를 사용하여 의존성을 관리합니다.

```bash
# Poetry 설치 (필요한 경우)
curl -sSL https://install.python-poetry.org | python3 -

# 프로젝트 의존성 설치
poetry install
```

## 사용 방법

### 1. 데이터 수집
```bash
# 기본 데이터 수집 (월별 마지막 날짜)
poetry run python fetch_local_finance_data.py

# 특정 기간 데이터 수집
poetry run python fetch_local_finance_data.py --start-year 2023 --end-year 2024

# 모든 날짜 데이터 수집
poetry run python fetch_local_finance_data.py --all-days

# 불완전 수집 데이터 재시도
poetry run python fetch_local_finance_data.py --retry-incomplete
```

### 2. 데이터 처리
```bash
poetry run python process_local_finance_data.py
```

### 3. 데이터 분석
```bash
poetry run python analyze_local_finance.py
```

## 주요 분석 기능

- 월별/분기별 예산 집행 현황
- 분야별/부문별 예산 집행 추이
- 지역별 예산 집행 현황
- 계절성 분석
- 이상치 탐지
- 전년 대비 증감률 분석
- 3차원 분석 (시간/지역/분야)

## 프로젝트 구조

- `fetch_local_finance_data.py`: 데이터 수집 모듈
- `process_local_finance_data.py`: 데이터 처리 모듈
- `analyze_local_finance.py`: 데이터 분석 모듈
- `config.py`: 설정 파일
- `data/`: 수집된 데이터 저장 디렉토리
- `logs/`: 로그 파일 저장 디렉토리

## 의존성

- Python 3.12+
- pandas
- numpy
- sqlite3
- openpyxl
- requests

## 로깅

- 일반 로그: `logs/fetch_log_YYYYMMDD.log`
- 수집 비교 로그: `logs/collection_count_YYYYMMDD.log`

## 개발자

2kwonhee (id@senterie.com)