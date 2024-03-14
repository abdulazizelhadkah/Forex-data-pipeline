from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import csv,requests,json

default_args = {
    "owner":"airflow",
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"Ze.elhadkah@gmail.com",
    "retries":2,
    "retry_delay": timedelta(minutes=5)
}

def _get_message() -> str:
    return "Hello Forex"

with DAG("Forex_data_pipeline",start_date= datetime(2023,12,3), 
schedule_interval='@daily',default_args=default_args,catchup=False) as dag:

 is_forex_rates_available = HttpSensor(
    task_id="is_forex_rates_available",
    http_conn_id="forex_api",
    endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
    response_check=lambda response: "rates" in response.text,
    timeout=20,
    poke_interval = 5 
 )
#1- Run start script to run all containers and docker composer
#2- open airflow and log in as airflow
#3- Add new http connection fill (conn id, conn type, host)
#4- run "docker exec -it 16a60af2b503 /bin/bash" to open airflow terminal for testing
#5- run "airflow tasks test forex_data_pipeline is_forex_rates_available 2023-12-03" to test in airflow cli



is_forex_currencies_file_available = FileSensor(
        task_id = "is_forex_currencies_file_available",
        fs_conn_id= "forex_path",
        filepath= "forex_currencies.csv",
        poke_interval= 5,
        timeout= 20
)
#1- Create new file connection fill (conn id, conn type,path = {"path":"/opt/airflow/dags/files"}) 
#2- run "docker exec -it 16a60af2b503 /bin/bash" to open airflow terminal for testing
#3- run "airflow tasks test forex_data_pipeline is_forex_currencies_file_available  2023-12-03" to test task on airflow cli



def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

downloading_rates = PythonOperator(
    task_id= "downloading_rates",
    python_callable= download_rates
)
#1- run "docker exec -it 16a60af2b503 /bin/bash" to open airflow terminal for testing
#2- Run "airflow tasks test forex_data_pipeline downloading_rates  2023-12-03" to test python task on airflow cli 



saving_rates = BashOperator(
    task_id = "saving_rates",
    bash_command= """
    hdfs dfs -mkdir -p /forex && \
    hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
    """
)
#1- open hue on localhost:32762 with password root 
#2- run "docker exec -it 16a60af2b503 /bin/bash" to open airflow terminal for testing
#3- Run "airflow tasks test forex_data_pipeline Saving_rates  2023-12-03" to test Bash task on airflow cli 


creating_forex_rates_table = HiveOperator(
    task_id = "creating_forex_rates_table",
    hive_cli_conn_id= "hive_conn",
    hql="""
    CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
        base STRING,
        last_update DATE,
        eur DOUBLE,
        usd DOUBLE,
        nzd DOUBLE,
        gbp DOUBLE,
        jpy DOUBLE,
        cad DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    STORED AS TEXTFILE
    """
)
#1- open hue on localhost:32762 with password root 
#2- Add new hive  connection (conn id = hive_conn, conn type = Hive server 2 thrift,host =  hive server,login&password = hive,port= 1000)
#3- run "docker exec -it 16a60af2b503 /bin/bash" to open airflow terminal for testing
#4- Run "airflow tasks test forex_data_pipeline Saving_rates  2023-12-03" to test Bash task on airflow cli 



forex_processing = SparkSubmitOperator(
    task_id = "forex_processing",
    application="/opt/airflow/dags/scripts/forex_processing.py",
    conn_id="spark_conn",
    verbose=False
)
#1- add new connection (conn id = spark_conn, conn type = spark, host = spark://spark-master, port=70777)
#2- run "docker exec -it 16a60af2b503 /bin/bash" to open airflow terminal for testing
#3- Run "airflow tasks test forex_data_pipeline Saving_rates  2023-12-03" to test Bash task on airflow cli 


"""
send_email_notification= EmailOperator(
    task_id = "send_email_notification",
    to="ze.elhadkah@gamil.com",
    subject="forex_data_pipeline",
    html_content="<h3>forex_data_pipeline</h3>"
)
#1- Sign in "https://security.google.com/settings/security/apppasswords" and activate using email
#2-configuere airflow in airflow.cfg to send smtp protocol
"""
"""
[smtp]
# If you want airflow to send emails on retries, failure, and you want to use
# the airflow.utils.email.send_email_smtp function, you have to configure an
smtp server here
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
# Example: smtp_user = airflow
smtp_user = marclamberti.ml@gmail.com
#Example: smtp_password = airflow
smtp_password = bczorlbxywmojogg
smtp_port = 587
smtp_mail_from = marclamberti.ml@gmail.com
smtp_timeout = 30
smtp_retry_limit = 5
"""
#4- test



send_slack_notification = SlackWebhookOperator(
    task_id= "send_slack_notification",
    http_conn_id = "slack_conn",
    message= _get_message(),
    channel="#forex-data-pipeline-monitoring"
)
#1- customize slack channel tha you will send notification for 
#2- open "api.slack.com/app" create new manual app and create new web hook
#3- add new connection (conn type = http,
# if the link is https://hooks.slack.com/services/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
# So host = https://hooks.slack.com/services and password = T069NH1C4MP/B069NK10W3T/s6aweaUtNbOq3KqlfPoXJr0d)
#4-test





#first method set
is_forex_rates_available.set_downstream(is_forex_currencies_file_available)
is_forex_currencies_file_available.set_upstream(is_forex_rates_available)

#second method >> <<
is_forex_rates_available>>is_forex_currencies_file_available>>downloading_rates>>saving_rates
saving_rates>>creating_forex_rates_table>>forex_processing>>send_slack_notification
