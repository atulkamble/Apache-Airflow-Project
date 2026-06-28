### start airflow standalone 
```
airflow standalone
```
### note down username and password
username: admin
```
cat /home/azureuser/airflow/simple_auth_manager_passwords.json.generated
```
### create service file
``` 
sudo nano /etc/systemd/system/airflow-standalone.service
```
## copy content
```
[Unit]
Description=Apache Airflow Standalone
After=network.target

[Service]
Type=simple
User=azureuser
Group=azureuser

Environment="AIRFLOW_HOME=/home/azureuser/airflow"
Environment="PATH=/home/azureuser/airflow-project/airflow-env/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin"

WorkingDirectory=/home/azureuser/airflow-project

ExecStart=/home/azureuser/airflow-project/airflow-env/bin/airflow standalone

Restart=always
RestartSec=10

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### start service and enable and check status
```
sudo systemctl daemon-reload
sudo systemctl start airflow-standalone
sudo systemctl enable airflow-standalone
sudo systemctl status airflow-standalone
```




# 🚀 Apache Airflow 3.x Hands-On Practice Guide

> **Version:** Apache Airflow 3.x
> **Platform:** Ubuntu / Linux / WSL / Virtual Machine
> **Difficulty:** Beginner → Intermediate

---

# 📚 What is Apache Airflow?

Apache Airflow is an **open-source workflow orchestration platform** used to **schedule, automate, and monitor workflows**.

Instead of manually running scripts every day, Airflow allows you to define workflows as **DAGs (Directed Acyclic Graphs)** and execute them automatically.

---

# 🎯 Real-World Use Cases

* ETL Pipelines
* Data Engineering
* Data Warehousing
* Machine Learning Pipelines
* Database Backup
* Report Generation
* Cloud Automation
* File Processing
* Website Monitoring
* DevOps Automation
* Kubernetes Jobs
* Email Notifications

---

# 🏗 Airflow Architecture

```
                  +----------------+
                  |    Web UI      |
                  | (API Server)   |
                  +--------+-------+
                           |
                           |
               +-----------+-----------+
               |                       |
        +------+-----+          +------+------+
        | Scheduler  |          | Metadata DB |
        +------+-----+          +-------------+
               |
               |
        +------+------+
        |    Workers  |
        +------+------+
               |
               |
        +------+------+
        |     DAGs    |
        +-------------+
```

---

# 📦 Prerequisites

* Ubuntu / Linux
* Python 3.10+
* pip
* Virtual Environment
* Internet Connection

---

# 1️⃣ Install Apache Airflow

Update packages

```bash
sudo apt update -y
```

Install Python packages

```bash
sudo apt install python3-full python3-venv python3-pip -y
```

Create project

```bash
mkdir airflow-project
cd airflow-project
```

Create virtual environment

```bash
python3 -m venv airflow-env
```

Activate virtual environment

```bash
source airflow-env/bin/activate
```

Upgrade pip

```bash
pip install --upgrade pip
```

Install Airflow

```bash
pip install apache-airflow
```

Verify installation

```bash
airflow version
```

---

# 2️⃣ Configure Airflow Home

Check Airflow home

```bash
echo $AIRFLOW_HOME
```

Default

```
~/airflow
```

(Optional) Use custom location

```bash
export AIRFLOW_HOME=~/airflow-project
```

---

# 3️⃣ Initialize Airflow Database

Apache Airflow 3.x uses

```bash
airflow db migrate
```

Older command

```bash
airflow db init
```

> ⚠️ `airflow db init` is deprecated in Airflow 3.x.

---

# 4️⃣ Create Admin User

```bash
airflow users create \
  --username admin \
  --firstname Atul \
  --lastname Kamble \
  --role Admin \
  --email admin@example.com \
  --password Admin@123
```

Verify users

```bash
airflow users list
```

---

# 5️⃣ Start Airflow

## Option 1 (Recommended for Beginners)

```bash
airflow standalone
```

This command automatically:

* Initializes the database
* Creates an admin user
* Starts the API Server
* Starts the Scheduler
* Displays login credentials

---

## Option 2 (Manual)

Start API Server

```bash
airflow api-server --port 8080
```

Open browser

```
http://localhost:8080
```

Start Scheduler

```bash
airflow scheduler
```

---

# 6️⃣ Airflow Folder Structure

```
$AIRFLOW_HOME

├── dags/
├── logs/
├── plugins/
├── config/
├── airflow.cfg
├── airflow.db
└── webserver_config.py
```

Important folder

```
dags/
```

All workflows are stored here.

---

# 7️⃣ Create First DAG

Create

```
$AIRFLOW_HOME/dags/hello_airflow.py
```

```python
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Hello from Apache Airflow!")

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
):
    task1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )
```

---

# 8️⃣ DAG Example – Task Dependency

```
Task1
   |
Task2
   |
Task3
```

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dependency_example",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
):

    task1 = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    task2 = BashOperator(
        task_id="sleep_task",
        bash_command="sleep 5"
    )

    task3 = BashOperator(
        task_id="final_task",
        bash_command="echo Workflow Complete"
    )

task1 >> task2 >> task3
```

---

# 9️⃣ DAG Example – ETL Pipeline

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extracting Data")

def transform():
    print("Transforming Data")

def load():
    print("Loading Data")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
):

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

extract_task >> transform_task >> load_task
```

---

# 🔟 DAG Example – System Monitoring

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="system_monitor",
    start_date=datetime(2024,1,1),
    schedule="@hourly",
    catchup=False,
):

    cpu = BashOperator(
        task_id="cpu_usage",
        bash_command="top -b -n1 | head -5"
    )

    disk = BashOperator(
        task_id="disk_usage",
        bash_command="df -h"
    )

cpu >> disk
```

---

# 1️⃣1️⃣ Run Python Script

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="python_script",
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
):

    run_script = BashOperator(
        task_id="run_python_script",
        bash_command="python3 /home/ubuntu/script.py"
    )
```

---

# 1️⃣2️⃣ Website Monitoring Project

```python
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_website():

    response = requests.get("https://google.com")

    if response.status_code == 200:
        print("Website is UP")

    else:
        print("Website DOWN")

with DAG(
    dag_id="website_monitor",
    start_date=datetime(2024,1,1),
    schedule="*/10 * * * *",
    catchup=False,
):

    task = PythonOperator(
        task_id="website_check",
        python_callable=check_website
    )
```

---

# 1️⃣3️⃣ Airflow Commands

## Version

```bash
airflow version
```

---

## List DAGs

```bash
airflow dags list
```

---

## Show DAG

```bash
airflow dags show hello_airflow
```

---

## Trigger DAG

```bash
airflow dags trigger hello_airflow
```

---

## Pause DAG

```bash
airflow dags pause hello_airflow
```

---

## Unpause DAG

```bash
airflow dags unpause hello_airflow
```

---

## Delete DAG Metadata

```bash
airflow dags delete hello_airflow
```

---

## DAG Test

```bash
airflow dags test hello_airflow 2026-01-01
```

---

## List Tasks

```bash
airflow tasks list hello_airflow
```

---

## Test Single Task

```bash
airflow tasks test hello_airflow hello_task 2026-01-01
```

---

## List Users

```bash
airflow users list
```

---

## Delete User

```bash
airflow users delete admin
```

---

## Database Check

```bash
airflow db check
```

---

## Database Migration

```bash
airflow db migrate
```

---

## Reset Database

```bash
airflow db reset
```

---

# 1️⃣4️⃣ Useful Linux Commands

Current directory

```bash
pwd
```

List files

```bash
ls -la
```

Activate virtual environment

```bash
source airflow-env/bin/activate
```

Deactivate

```bash
deactivate
```

Installed packages

```bash
pip list
```

---

# 1️⃣5️⃣ Troubleshooting

List DAG import errors

```bash
airflow dags list-import-errors
```

Check Airflow processes

```bash
ps -ef | grep airflow
```

Stop Airflow

```bash
pkill -f airflow
```

Restart API Server

```bash
airflow api-server --port 8080
```

Restart Scheduler

```bash
airflow scheduler
```

Check logs

```bash
ls $AIRFLOW_HOME/logs
```

---

# 1️⃣6️⃣ Airflow CLI Comparison

| Airflow 2.x          | Airflow 3.x          |
| -------------------- | -------------------- |
| airflow db init      | ✅ airflow db migrate |
| airflow webserver    | ✅ airflow api-server |
| schedule_interval    | ✅ schedule           |
| airflow scheduler    | Same                 |
| airflow dags list    | Same                 |
| airflow users create | Same                 |
| airflow standalone   | ✅ Recommended        |

---

# 1️⃣7️⃣ Airflow Best Practices

* Always use a Python virtual environment.
* Store all DAGs in the `dags/` directory.
* Use the `schedule` parameter instead of `schedule_interval`.
* Set `catchup=False` unless historical runs are required.
* Give tasks meaningful `task_id` values.
* Test DAGs locally before deployment.
* Organize reusable code inside the `plugins/` directory.
* Monitor scheduler and task logs regularly.
* Use Variables and Connections for configuration instead of hardcoding credentials.
* Keep DAG files lightweight and move business logic into external Python modules when possible.

---

# 1️⃣8️⃣ Practice Exercises

## Beginner

* Create a Hello World DAG.
* Print the current date and time.
* Execute Linux commands using `BashOperator`.
* Run a Python script using `BashOperator`.

## Intermediate

* Build an ETL pipeline (Extract → Transform → Load).
* Monitor disk usage every hour.
* Check website availability every 10 minutes.
* Send an email notification on task success or failure.
* Use Airflow Variables and Connections.

## Advanced

* Trigger one DAG from another.
* Use branching with `BranchPythonOperator`.
* Create dynamic task mapping.
* Integrate with cloud services (AWS, Azure, or GCP).
* Build a complete data pipeline using a database and object storage.

---

# 1️⃣9️⃣ Real-World Airflow Projects

| Project                    | Skills Covered                 |
| -------------------------- | ------------------------------ |
| Website Monitoring         | PythonOperator, Scheduling     |
| ETL Pipeline               | Data Engineering               |
| Database Backup            | BashOperator, Cron Scheduling  |
| File Processing            | Sensors, PythonOperator        |
| AWS S3 File Sync           | Cloud Integration              |
| Azure Blob Automation      | Azure Integration              |
| Email Notification System  | SMTP, Alerting                 |
| Log Monitoring             | BashOperator, PythonOperator   |
| Kubernetes Job Automation  | KubernetesPodOperator          |
| ML Model Training Pipeline | Machine Learning Orchestration |

---

# 🎯 Learning Roadmap

```
Python Basics
      ↓
Linux Commands
      ↓
Virtual Environment
      ↓
Install Airflow
      ↓
CLI Commands
      ↓
DAG Basics
      ↓
Operators
      ↓
Scheduling
      ↓
Dependencies
      ↓
Variables & Connections
      ↓
ETL Pipelines
      ↓
Monitoring
      ↓
Cloud Integration
      ↓
Production Airflow
```

---

# ✅ Outcome

After completing this hands-on guide, you will be able to:

* Install and configure Apache Airflow 3.x.
* Create and manage DAGs.
* Schedule and monitor workflows.
* Use Python and Bash operators.
* Build ETL pipelines.
* Automate infrastructure and cloud tasks.
* Troubleshoot common Airflow issues.
* Develop real-world workflow automation projects for DevOps and Data Engineering.
