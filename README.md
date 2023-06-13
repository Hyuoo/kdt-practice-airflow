# kdt_airflow_practice

.

## hw1_1 - airflow.cfg 파일   
설정값이 저장되어 있으며 아래와 같이 많은 섹션이 있다.
```
[core]
    dags_folder = /opt/airflow/dags
    executor = SequentialExecutor
    max_active_tasks_per_dag = 16
    max_active_runs_per_dag = 16
[database]  # SqlAlchemy connection, metadata database
[logging]
[metrics]
[secrets]
[cli]
[debug]
[api]   # https://airflow.apache.org/docs/apache-airflow/2.5.1/administration-and-deployment/security/api.html
[lineage]
[atlas]
[operators]
[hive]
[webserver]
[email]
[smtp]
[sentry]
[local_kubernetes_executor]
[celery_kubernetes_executor]
[celery]
[celery_broker_transport_options]
[dask]
[scheduler]
[triggerer]
[kerberos]
[elasticsearch]
[elasticsearch_configs]
[kubernetes_executor]
[sensors]
```


1. DAGs 폴더는 어디에 지정되는가?
    - [core] 섹션에 dags_folder라는 값으로 저장되어있다.
    - 설정값은 /opt/airflow/dags


2. DAGs 폴더에 새로운 Dag를 만들면 언제 실제로 Airflow 시스템에서 이를 알게
되나? 이 스캔 주기를 결정해주는 키의 이름이 무엇인가?
    - [scheduler] 섹션의 dag_dir_list_interval = 300
    - 기본값은 300초(5분)


3. 이 파일에서 Airflow를 API 형태로 외부에서 조작하고 싶다면 어느 섹션을
변경해야하는가?
    - [api] 섹션을 변경하면 된다.
    - 위 [api]에 달린 주석링크에 문서가 있는데 봐도 모르겠다.   
    ---
    ```
    다음날 정답공개
    먼저 api로 외부에서 상태 모니터링, dag실행, connections import/export, variables get/set 가능
    [api]섹션에서
      auth_backends = airflow.api.auth.backend.session
    를
      auth_backends = airflow.api.auth.backend.basic_auth
    로 바꾸면 ID/PW로 인증을 변경
    ```

4. Variable에서 변수의 값이 encrypted가 되려면 변수의 이름에 어떤 단어들이
들어가야 하는데 이 단어들은 무엇일까? :)
    - [core] 섹션의 sensitive_var_conn_names =
    - (,)로 구분되는 문자목록
    - 아마 바로 위의 hide_sensitive_var_conn_fields = True 가 False로 설정되어야 동작하는 듯 하다.
    ---
    ```
    다음날 정답공개
    password, secret, passwd, authorization, api_key, apikey, access_token
    키워드라고 한다.
    ```

5. 이 환경 설정 파일이 수정되었다면 이를 실제로 반영하기 위해서 해야 하는 일은?
    - .
    ---
    ```
    다음날 정답공개
    sudo systemctl restart airflow-webserver
    sudo systemctl restart airflow-scheduler
    ```

6. Metadata DB의 내용을 암호화하는데 사용되는 키는 무엇인가?
    - . 
    ---
    ```
    다음날 정답공개
    # Secret key to save connection passwords in the db
    fernet_key = 
    ```

## hw1_2
API정보 : https://github.com/apilayer/restcountries.git

dags/kdt_homework_country.py
