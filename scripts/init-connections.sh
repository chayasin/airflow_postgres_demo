#!/bin/bash
airflow connections add 'postgres-local' \
    --conn-type 'postgres' \
    --conn-host 'postgres_demo-postgres_chinook-1' \
    --conn-schema 'chinook' \
    --conn-login 'chinook' \
    --conn-password 'chinook'