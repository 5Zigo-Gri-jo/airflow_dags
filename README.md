# 플레이 데이터 DE 32기 첫번째 프로젝트

## 영화 데이터로 ETL 프로젝트 

### How to Install
```
# main
$ pip install git+https://github.com/5Zigo-Gri-jo/airflow_dags.git

# branch
$ pip install git+https://github.com/5Zigo-Gri-jo/airflow_dags.git@<BRANCH_NAME>
```

### 터미널에서 프로그램 실행하는 법 
```
$ git clone <URL>              # 깃허브에서 패키지 클론해오기
$ cd <DIR>                     # 패키지 설치할  폴더 만들기
$ source .venv/bin/activate    # 가상환경 시작하는 코드
$ pdm install                  # 배포위해 설치
$ pytest                       # 코드 테스트를 위해 설치해야함

# option
$ pdm venv create              # 가상환경 설치?
https://pdm-project.org/latest/usage/venv/
```

### 환경 설치 확인(api key 서버에 숨기기)
```
$ cat ~/.zshrc | tail -n 3

# MY_ENV
export MOVIE_API_KEY="<KEY>"
