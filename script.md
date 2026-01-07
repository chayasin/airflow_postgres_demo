
## Setup the env

```bash
cp .env.example .env
```

## Get Airflow Token

loging airflow

```bash
curl -X POST "http://localhost:8080/auth/token" -H "Content-Type: application/json" -d '{"username":"airflow","password":"airflow"}'
```

## 

to airflow worker shell

```bash
docker exec -it airflow_postgres_demo-airflow-worker-1 bash
```
