set -e

until pg_isready -h postgres -U airflow &> /dev/null; do
    echo "Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "PostgreSQL is ready!"

echo "Initializing Airflow database..."
airflow db init || echo "Database already initialized"

until airflow db check &> /dev/null; do
    echo "Waiting for Airflow database to be ready..."
    sleep 2
done

echo "Database is ready, ensuring admin user exists..."

airflow users delete --username admin 2>/dev/null || true

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Admin user created successfully!"
echo "Login: admin"
echo "Password: admin"