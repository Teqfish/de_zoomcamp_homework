import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.resource(name="ny_taxi_data", write_disposition="replace")
def ny_taxi_data(page_size: int = 1000):
    """
    Fetch NYC taxi trips from the custom Zoomcamp API.

    API contract (homework):
    - paginated JSON
    - 1000 records per page
    - request pages until an empty page is returned
    """
    page = 1
    while True:
        resp = requests.get(BASE_URL, params={"page": page}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        # Stop condition per homework: empty page means no more data
        if not data:
            break

        # Yield records to dlt
        yield data

        page += 1


def run():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
        progress="log",
    )
    load_info = pipeline.run(ny_taxi_data())
    print(load_info)


if __name__ == "__main__":
    run()
