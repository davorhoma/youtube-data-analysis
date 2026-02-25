"""
youtube_streaming_stop.py
=========================
DAG koji:
  1. Zaustavlja Spark aplikaciju "YouTubeTrendingStreaming" na Spark clusteru
  2. Zaustavlja Airflow task "start_youtube_streaming" i DAG run "youtube_streaming_start"
     i označava ga kao SUCCESS (jer je graceful shutdown očekivano ponašanje)

Potrebne Airflow varijable:
  - SPARK_MASTER_URL   : npr. http://spark-master:8080
  - AIRFLOW_BASE_URL   : npr. http://airflow-airflow-apiserver-1:8080
"""

from datetime import datetime

import requests
from airflow.sdk import dag, Variable
from airflow.providers.standard.operators.python import PythonOperator

SPARK_APP_NAME = "YouTubeTrendingStreaming"
TARGET_DAG_ID = "youtube_streaming_start"
TARGET_TASK_ID = "start_youtube_streaming"
AIRFLOW_USER = "airflow"
AIRFLOW_PASS = "airflow"


def get_airflow_token(airflow_url: str) -> str:
    resp = requests.post(
        f"{airflow_url}/auth/token",
        json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS},
        timeout=10,
    )
    print(f"   Token endpoint status: {resp.status_code}")
    resp.raise_for_status()

    data = resp.json()
    token = data.get("access_token") or data.get("token")
    if not token:
        raise ValueError(f"JWT token nije pronađen u odgovoru: {data}")

    print("JWT token dohvaćen uspješno.")
    return token


def kill_spark_app():
    spark_master_url = Variable.get("SPARK_MASTER_URL")

    print(f"Tražim Spark aplikaciju '{SPARK_APP_NAME}' na {spark_master_url}...")

    resp = requests.get(f"{spark_master_url}/json/", timeout=10)
    resp.raise_for_status()

    data = resp.json()
    active_apps = data.get("activeapps", [])
    print(f"   Pronađeno {len(active_apps)} aktivnih aplikacija.")

    targets = [
        app
        for app in active_apps
        if SPARK_APP_NAME.lower() in app.get("name", "").lower()
    ]

    if not targets:
        print(f"Nema aktivnih Spark aplikacija sa imenom '{SPARK_APP_NAME}'.")
        print(f"   Aktivne aplikacije: {[a.get('name') for a in active_apps]}")
        return

    for app in targets:
        app_id = app["id"]
        print(f"Zaustavljam Spark app: {app['name']} (ID: {app_id})")
        kill_resp = requests.post(
            f"{spark_master_url}/app/kill/",
            data={"id": app_id},
            timeout=10,
        )
        print(f"   HTTP status: {kill_resp.status_code}")

    print(f"✅ Zaustavljeno {len(targets)} Spark aplikacija.")


def kill_airflow_dag_run():
    airflow_url = Variable.get("AIRFLOW_BASE_URL")

    print(f"Dohvaćam JWT token sa {airflow_url}...")
    token = get_airflow_token(airflow_url)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    print(f"🔍 Tražim aktivne DAG runove za '{TARGET_DAG_ID}'...")
    resp = requests.get(
        f"{airflow_url}/api/v2/dags/{TARGET_DAG_ID}/dagRuns",
        params={"state": "running"},
        headers=headers,
        timeout=10,
    )
    resp.raise_for_status()
    dag_runs = resp.json().get("dag_runs", [])

    if not dag_runs:
        print(f"Nema aktivnih DAG runova za '{TARGET_DAG_ID}'. Preskačem.")
        return

    for run in dag_runs:
        dag_run_id = run["dag_run_id"]
        print(f"Obrađujem DAG run: {dag_run_id}")

        tasks_resp = requests.get(
            f"{airflow_url}/api/v2/dags/{TARGET_DAG_ID}/dagRuns/{dag_run_id}/taskInstances",
            headers=headers,
            timeout=10,
        )
        tasks_resp.raise_for_status()
        tasks = tasks_resp.json().get("task_instances", [])

        for task in tasks:
            if task["task_id"] == TARGET_TASK_ID and task["state"] in (
                "running",
                "queued",
                "scheduled",
                "failed",
            ):
                current_state = task["state"]
                print(
                    f"   Postavljam task '{task['task_id']}' (trenutno: {current_state}) → success"
                )
                patch_resp = requests.patch(
                    f"{airflow_url}/api/v2/dags/{TARGET_DAG_ID}"
                    f"/dagRuns/{dag_run_id}/taskInstances/{TARGET_TASK_ID}",
                    json={
                        "dry_run": False,
                        "new_state": "success",
                    },  # success umesto failed
                    headers=headers,
                    timeout=10,
                )
                print(f"   HTTP status: {patch_resp.status_code}")

        print(f"   Označavam DAG run '{dag_run_id}' kao success...")
        patch_run_resp = requests.patch(
            f"{airflow_url}/api/v2/dags/{TARGET_DAG_ID}/dagRuns/{dag_run_id}",
            json={"state": "success"},  # success umesto failed
            headers=headers,
            timeout=10,
        )
        print(f"   HTTP status: {patch_run_resp.status_code}")

    print(f"✅ Obrađeno {len(dag_runs)} DAG runova.")


@dag(
    dag_id="youtube_streaming_stop",
    description="Zaustavlja Spark streaming job i Airflow DAG run za youtube_streaming_start",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    max_active_tasks=1,
)
def stop_streaming():
    t1 = PythonOperator(
        task_id="kill_spark_app",
        python_callable=kill_spark_app,
    )

    t2 = PythonOperator(
        task_id="kill_airflow_dag_run",
        python_callable=kill_airflow_dag_run,
    )

    t1 >> t2


stop_streaming()
