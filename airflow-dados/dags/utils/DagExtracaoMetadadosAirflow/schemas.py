SCHEMA_AIRFLOW_TASK_INSTANCES = {
    "dag_id": {"nomes_origem": ["dag_id"], "data_type": "string"},
    "dag_run_id": {"nomes_origem": ["dag_run_id"], "data_type": "string"},
    "duration": {"nomes_origem": ["duration"], "data_type": "double"},
    "end_date": {"nomes_origem": ["end_date"], "data_type": "timestamp[ns]"},
    "execution_date": {
        "nomes_origem": ["execution_date"],
        "data_type": "timestamp[ns]",
    },
    "executor_config": {"nomes_origem": ["executor_config"], "data_type": "string"},
    "hostname": {"nomes_origem": ["hostname"], "data_type": "string"},
    "map_index": {"nomes_origem": ["map_index"], "data_type": "int64"},
    "max_tries": {"nomes_origem": ["max_tries"], "data_type": "int64"},
    "note": {"nomes_origem": ["note"], "data_type": "string"},
    "operator": {"nomes_origem": ["operator"], "data_type": "string"},
    "pid": {"nomes_origem": ["pid"], "data_type": "double"},
    "pool": {"nomes_origem": ["pool"], "data_type": "string"},
    "pool_slots": {"nomes_origem": ["pool_slots"], "data_type": "int64"},
    "priority_weight": {"nomes_origem": ["priority_weight"], "data_type": "int64"},
    "queue": {"nomes_origem": ["queue"], "data_type": "string"},
    "queued_when": {"nomes_origem": ["queued_when"], "data_type": "timestamp[ns]"},
    "rendered_fields": {"nomes_origem": ["rendered_fields"], "data_type": "string"},
    "sla_miss": {"nomes_origem": ["sla_miss"], "data_type": "string"},
    "start_date": {"nomes_origem": ["start_date"], "data_type": "timestamp[ns]"},
    "state": {"nomes_origem": ["state"], "data_type": "string"},
    "task_id": {"nomes_origem": ["task_id"], "data_type": "string"},
    "trigger": {"nomes_origem": ["trigger"], "data_type": "string"},
    "triggerer_job": {"nomes_origem": ["triggerer_job"], "data_type": "string"},
    "try_number": {"nomes_origem": ["try_number"], "data_type": "int64"},
    "unixname": {"nomes_origem": ["unixname"], "data_type": "string"},
}

SCHEMA_AIRFLOW_DAG_RUNS = {
    "conf": {"nomes_origem": ["conf"], "data_type": "string"},
    "dag_id": {"nomes_origem": ["dag_id"], "data_type": "string"},
    "dag_run_id": {"nomes_origem": ["dag_run_id"], "data_type": "string"},
    "data_interval_end": {
        "nomes_origem": ["data_interval_end"],
        "data_type": "timestamp[ns]",
    },
    "data_interval_start": {
        "nomes_origem": ["data_interval_start"],
        "data_type": "timestamp[ns]",
    },
    "end_date": {"nomes_origem": ["end_date"], "data_type": "timestamp[ns]"},
    "execution_date": {
        "nomes_origem": ["execution_date"],
        "data_type": "timestamp[ns]",
    },
    "external_trigger": {"nomes_origem": ["external_trigger"], "data_type": "bool"},
    "last_scheduling_decision": {
        "nomes_origem": ["last_scheduling_decision"],
        "data_type": "timestamp[ns]",
    },
    "logical_date": {
        "nomes_origem": ["logical_date"],
        "data_type": "timestamp[ns]",
    },
    "note": {"nomes_origem": ["note"], "data_type": "string"},
    "run_type": {"nomes_origem": ["run_type"], "data_type": "string"},
    "start_date": {"nomes_origem": ["start_date"], "data_type": "timestamp[ns]"},
    "state": {"nomes_origem": ["state"], "data_type": "string"},
}

SCHEMA_AIRFLOW_DAGS = {
    "dag_id": {"nomes_origem": ["dag_id"], "data_type": "string"},
    "default_view": {"nomes_origem": ["default_view"], "data_type": "string"},
    "description": {"nomes_origem": ["description"], "data_type": "string"},
    "file_token": {"nomes_origem": ["file_token"], "data_type": "string"},
    "fileloc": {"nomes_origem": ["fileloc"], "data_type": "string"},
    "has_import_errors": {"nomes_origem": ["has_import_errors"], "data_type": "bool"},
    "has_task_concurrency_limits": {
        "nomes_origem": ["has_task_concurrency_limits"],
        "data_type": "bool",
    },
    "is_active": {"nomes_origem": ["is_active"], "data_type": "bool"},
    "is_paused": {"nomes_origem": ["is_paused"], "data_type": "bool"},
    "is_subdag": {"nomes_origem": ["is_subdag"], "data_type": "bool"},
    "last_expired": {
        "nomes_origem": ["last_expired"],
        "data_type": "timestamp[ns]",
    },
    "last_parsed_time": {
        "nomes_origem": ["last_parsed_time"],
        "data_type": "timestamp[ns]",
    },
    "last_pickled": {"nomes_origem": ["last_pickled"], "data_type": "timestamp[ns]"},
    "max_active_runs": {
        "nomes_origem": ["max_active_runs"],
        "data_type": "int8",
    },
    "max_active_tasks": {"nomes_origem": ["max_active_tasks"], "data_type": "int8"},
    "next_dagrun": {
        "nomes_origem": ["next_dagrun"],
        "data_type": "timestamp[ns]",
    },
    "next_dagrun_create_after": {
        "nomes_origem": ["next_dagrun_create_after"],
        "data_type": "timestamp[ns]",
    },
    "next_dagrun_data_interval_end": {
        "nomes_origem": ["next_dagrun_data_interval_end"],
        "data_type": "timestamp[ns]",
    },
    "next_dagrun_data_interval_start": {
        "nomes_origem": ["next_dagrun_data_interval_start"],
        "data_type": "timestamp[ns]",
    },
    "owners": {"nomes_origem": ["owners"], "data_type": "string"},
    "pickle_id": {"nomes_origem": ["pickle_id"], "data_type": "string"},
    "root_dag_id": {"nomes_origem": ["root_dag_id"], "data_type": "string"},
    "schedule_interval": {"nomes_origem": ["schedule_interval"], "data_type": "string"},
    "scheduler_lock": {"nomes_origem": ["scheduler_lock"], "data_type": "bool"},
    "tags": {"nomes_origem": ["tags"], "data_type": "string"},
    "timetable_description": {
        "nomes_origem": ["timetable_description"],
        "data_type": "string",
    },
}

SCHEMA_AIRFLOW_EVENT_LOGS = {
    "event_log_id": {"nomes_origem": ["event_log_id"], "data_type": "int64"},
    "when": {"nomes_origem": ["when"], "data_type": "timestamp[us]"},
    "dag_id": {"nomes_origem": ["dag_id"], "data_type": "string"},
    "task_id": {"nomes_origem": ["task_id"], "data_type": "string"},
    "event": {"nomes_origem": ["event"], "data_type": "string"},
    "execution_date": {
        "nomes_origem": ["execution_date"],
        "data_type": "timestamp[us]",
    },
    "owner": {"nomes_origem": ["owner"], "data_type": "string"},
    "extra": {"nomes_origem": ["extra"], "data_type": "string"},
}

SCHEMA_AIRFLOW_TASK_EXECUTIONS = {
    "task_run_status": {"nomes_origem": ["task_run_status"], "data_type": "string"},
    "dag_id": {"nomes_origem": ["dag_id"], "data_type": "string"},
    "task_id": {"nomes_origem": ["task_id"], "data_type": "string"},
    "run_id": {"nomes_origem": ["run_id"], "data_type": "string"},
    "start_date": {"nomes_origem": ["start_date"], "data_type": "timestamp[us]"},
    "end_date": {"nomes_origem": ["end_date"], "data_type": "timestamp[us]"},
    "attempt": {"nomes_origem": ["attempt"], "data_type": "int16"},
}
