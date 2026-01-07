```bash
curl -X POST "http://localhost:8080/api/v2/security/login" -H "Content-Type: application/json" -d '{
    "username": "airflow",
    "password": "airflow",
    "provider": "db",
    "refresh": true
  }'
```