# 2019년 영화 데이터 수집을 위한 Airflow DAGs

이 레포지토리는 영화진흥위원회 API를 이용해 2019년 영화 데이터를 수집(Extract), 처리(Transform), 저장(Load)하는 것을 목표로 합니다. 이 레포지토리는 Apache Airflow를 사용하여 세 가지 단계로 나누어진 DAGs를 포함하고 있습니다: `extract`, `transform`, `load`.

## 목차
- [소개](#소개)
- [패키지 개요](#패키지-개요)
- [의존성 및 환경 설정](#의존성-및-환경-설정)
- [Contribution](#Contribution)

## 소개

본 레포지토리는 영화진흥위원회에서 제공하는 API를 통해 2019년의 영화 데이터를 추출하여, 이를 통해 활용 가능한 데이터 뱅크를 만드는 것을 목표로 합니다. 이를 위해 크게 세 가지 단계의 DAGs를 포함하고 있습니다.

## 패키지 개요

### Extract
2019년의 영화 목록을 API를 통해 추출

### Transform
추출한 박스오피스 영화 데이터 가공

### Load
변환된 영화 목록 데이터를 데이터베이스에 저장

##의존성 및 환경 설정

### 주요 의존성
- requests (API 호출용)
- 설치 방법:
```
$ pip install requests
```

- pandas (데이터 변환용)
- 설치 방법:
```
$ pip install pandas
```

- pyarrow(데이터 변환 및 저장)
- 설치 방법:
```
$ pip install pyarrow
```

### 환경 설정
1. Python 3.8 이상 설치
2. 가상 환경 설정 (optional):
```
source venv/bin/activate
```
해당 레포지토리를 실행하기 위해 아래와 같이 airflow dag 위치를 이동합니다. 
```
vi ~/.zshrc

[AIRFLOW]
export AIRFLOW_HOME=~/airflow_team
export AIRFLOW__CORE__DAGS_FOLDER=<YOUR_INSTALL_DIR>/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

## Contribution

### 1. 레포지토리 포크

먼저, 이 레포지토리를 포크합니다. GitHub에서 **Fork** 버튼을 클릭하여 포크된 레포지토리를 만듭니다.

### 2. 클론

포크된 레포지토리를 로컬에 클론

```
git clone git@github.com:5Zigo-Gri-jo/airflow_dags.git 
cd airflow_dags
```

### 새로운 기능 추가
- 새로운 기능을 추가하고자 한다면 아래와 같이 진행합니다.
1. 새로운 브랜치 생성합니다.
```
$ git checkout -b dx.0.0/새로운-기능-브랜치
```
2. 변경 사항 적용합니다.
```
$ git add .
$ git commit -m "새로운 기능 또는 버그 수정 내용"
```
3. 원격 저장소에 푸시합니다.
```
$ git push
```
4. 풀 리퀘스트를  생성합니다.
5. 리뷰 및 병합을 진행합니다.

