# Operator

### Trigger

- Explicit Trigger - ```TriggerDagRunOperator```
  - DAG (A -> B) 
- Reacive Trigger - ```ExternalTaskSensor```
  - DAG (A wait B)

### Task

- Branch
- LatestOnly
- TriggerRule

### DB
```
docker exec -it [CONTAINER] sh
psql -h postgres
\dt
SELECT * FROM dag_run;
DELETE FROM dag_run WHERE dag_id='';
```
