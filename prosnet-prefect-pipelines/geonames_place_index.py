import csv
import io
import json
import os
import zipfile
from prefect import flow, get_run_logger, task
from pydantic import BaseModel, Field, HttpUrl
from pydantic.types import PathType
import requests
from push_to_typesense import push_data_to_typesense_flow, Params as PushParams


@task()
def get_geonames_tsv_file(url: HttpUrl, column_names: list) -> dict:
    """Download the geonames tsv file and create a dictionary from it."""
    logger = get_run_logger()
    logger.info(f"Downloading {url}")
    response = requests.get(url)
    response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX/5XX
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Assuming there's only one file in the zip
        with thezip.open(thezip.namelist()[0]) as thefile:
            lines = (line.decode("utf-8") for line in thefile)
            reader = csv.DictReader(lines, fieldnames=column_names, delimiter="\t")
            return [row for row in reader]


# @task()
def create_typesense_data(data: list) -> str:
    """Create typesense data from the geonames data."""
    logger = get_run_logger()
    logger.info(f"Creating typesense data from {len(data)} rows.")
    data = [
        {
            "id": f"https://sws.geonames.org/{row['id']}/",
            "label": f"{row['name']} ({row['country_code']}) - {row['feature_code']}",
            "name": row["asciiname"],
            "country": row["country_code"],
            "feature_code": row["feature_code"],
            "coordinates": [float(row["latitude"]), float(row["longitude"])],
        }
        for row in data
    ]

    output_dir = "/data/typesense/data"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "geonames.json")
    logger.info(f"Saving data to {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Successfully saved {len(data)} records to {output_file}")
    return output_file


class Params(BaseModel):
    tsv_location: HttpUrl = Field(
        ...,
        description="URL to the geonames tsv file.",
    )
    typesense_collection_name: str = Field(
        default="prosnet-geonames-place-index",
        description="Name of the typesense collection to use.",
    )
    typesense_definition: dict = Field(
        default={
            "name": "prosnet-geonames-place-index",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "label", "type": "string"},
                {"name": "name", "type": "string", "optional": True},
                {"name": "country", "type": "string", "optional": True},
                {"name": "feature_code", "type": "string", "optional": True},
                {"name": "coordinates", "type": "geopoint", "optional": True},
            ],
        },
        description="Typesense definition to use, if None, existing collection is used.",
    )


@flow(version="0.1.0")
def create_typesense_place_index_from_geonames(params: Params):
    geonames_file = get_geonames_tsv_file(
        params.tsv_location,
        [
            "id",
            "name",
            "asciiname",
            "alternatenames",
            "latitude",
            "longitude",
            "feature_class",
            "feature_code",
            "country_code",
            "cc2",
            "admin1_code",
            "admin2_code",
            "admin3_code",
            "admin4_code",
            "population",
            "elevation",
            "dem",
            "timezone",
            "modification_date",
        ],
    )
    typesense_data = create_typesense_data(geonames_file)

    push_data_to_typesense_flow(
        PushParams(
            typesense_collection_name=params.typesense_collection_name,
            typesense_definition=params.typesense_definition,
            data=typesense_data,
        )
    )


if __name__ == "__main__":
    create_typesense_place_index_from_geonames(Params())
