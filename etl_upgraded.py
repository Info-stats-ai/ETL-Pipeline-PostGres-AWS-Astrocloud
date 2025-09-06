from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pendulum

# Define DAG
with DAG(
    dag_id="nasa_apod_postgres",
    schedule="@daily",
    start_date=pendulum.now("UTC").subtract(days=1),
    catchup=False,
    tags=["nasa", "api", "postgres"]
) as dag:

    # Step 1: Create table if it doesn't exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    # Step 2: Fetch NASA APOD API data
    @task
    def fetch_apod():
        from airflow.models import Connection
        conn = Connection.get_connection_from_secrets("nasa_api")
        api_key = conn.extra_dejson.get("api_key")
        url = "https://api.nasa.gov/planetary/apod"
        response = requests.get(url, params={"api_key": api_key})
        response.raise_for_status()
        return response.json()

    # Step 3: Transform API response
    @task
    def transform_apod_data(response):
        return {
            "title": response["title"],
            "explanation": response["explanation"],
            "url": response["url"],
            "date": response["date"],
            "media_type": response["media_type"]
        }

    # Step 4: Load data into Postgres
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data["title"],
                apod_data["explanation"],
                apod_data["url"],
                apod_data["date"],
                apod_data["media_type"]
            )
        )

    # Define task dependencies
    table = create_table()
    api_response = fetch_apod()
    transformed = transform_apod_data(api_response)
    table >> api_response >> transformed >> load_data_to_postgres(transformed)

