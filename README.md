# Useful Composer / Airflow commands 

## Create a composer cluster 
    gcloud composer environments create simple-pipeline --location europe-west1 --zone europe-west1-c --machine-type n1-standard-2 --disk-size 20

## Delete Dag 
    gcloud composer environments storage dags delete --environment simple-pipeline --location europe-west1 main_dag.py

## Test a task 
    gcloud composer environments run simple-pipeline --location europe-west1 test -- main_dag add 2019-01-01

## Run a backfill 
    gcloud composer environments run simple-pipeline --location europe-west1 backfill -- main_dag_17 -s 2019-01-01 -e 2019-01-02