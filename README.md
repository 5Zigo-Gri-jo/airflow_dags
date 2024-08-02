# 플레이 데이터 DE 32기 5조 첫번째 프로젝트

## 영화 데이터로 ETL 프로젝트
TEAM: 5Zigo-Gri-jo ('오지고 지리' 조 라는 뜻)
MEMBER: 박수진, 최우현, 김도현, 김동욱

해당 레포지토리는 저희 팀의 데이터 ETL(Extract, Transform, Load)을 수행할 airflow 전용 DAG 레포지토리입니다.                                                                                                                        현 레포지토리에 있는 DAG는 kobis에서 제공하는 영화 박스오피스 데이터를 수집,처리하며 저장하는 행위를 통해 활용 가능한 데이터 뱅크를 만드는 것을 목표로 >하며, 이를 위해 크게 extract DAG 3개, transform DAG 3개, load DAG 3개를 가지고 있습니다.

저희 5조의 경우, 활용할 데이터로서 2019년을 할당받았습니다.
                                                                            ### dev/1.0.0 (current version)  
본 브랜치는 현재 load패키지를 import하여 해당 패키지 안에 있는 ice_breaking 함수를 각 task마다 실행될 수 있도록 하여 각 task들이 기본적인 기능을 구현 할 수 있을지 확인하기 위해서만 개발 되었습니다.

다른 기능들은 추후 개발 및 릴리스 때 구현될 예정입니다.

### How to Install
```
# main
$ pip install git+https://github.com/5Zigo-Gri-jo/airflow_dags.git
# extract
$ pip install git+https://github.com/5Zigo-Gri-jo/Extract.git
# transform
$ pip install git+https://github.com/5Zigo-Gri-jo/transform.git
# load
$ pip install git+https://github.com/5Zigo-Gri-jo/load.git

# 특정 branch의 경우 아래와 같이 install받을 수 있습니다.
$ pip install git+https://github.com/5Zigo-Gri-jo/airflow_dags.git@<BRANCH_NAME>
```

### 터미널에서 프로그램 실행하는 법
```
$ git clone <URL>              # 깃허브에서 패키지 클론해오기
$ cd <DIR>                     # 패키지 설치할 폴더 만들기
$ source .venv/bin/activate    # 가상환경 시작하는 코드
$ pdm install                  # 배포위해 설치
$ pytest                       # 코드 테스트를 위해 설치해야함

# option
$ pdm venv create              # 가상환경 설치?
https://pdm-project.org/latest/usage/venv/
