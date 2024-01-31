from prefect import flow
from pydantic import BaseModel, Field, HttpUrl
from wikidata_index import create_typesense_index_from_sparql_query, Params as BaseParams

class Params(BaseModel):
    path_sparql_query: str = Field(default="prosnet-prefect-pipelines/sparql/wikidata-city.sparql", description="Relativ path to SPARQL query.")
    sparql_endpoint: HttpUrl = Field(default="https://query.wikidata.org/sparql", description="SPARQL endpoint to use, defaults to wikidata.")
    limit: int = Field(default=100, description="Limit to use for the SPARQL queries")
    typesense_definition: dict = Field(default={
        "name": "prosnet-wikidata-place-index",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "label", "type": "string"},
            {"name": "name", "type": "string", "optional": True},
            {"name": "country", "type": "string", "optional": True},
        ]
    },  description="Typesense definition to use, if None, incremental backup needs to be set.")
    incremental_update: bool = Field(default=True, description="If True, only objects changed since last run will be updated.")
    incremental_date: int | None = Field(default=2, description="Number of days to retrieve update for (today - days).")
    typesense_collection_name: str = Field(default="prosnet-wikidata-place-index", description="Name of the typesense collection to use.")
    typesense_api_key: str = Field(default="typesense-api-key", description="Name of the Prefect secrets block that holds the API key to use for typesense.")
    typesense_host: str = Field(default="typesense.acdh-dev.oeaw.ac.at", description="Host to use for typesense.")
    field_mapping: dict = Field(default={
        "itemLabel": "name",
        "countryLabel": "country",
        }, description="List of tuples to map SPARQL fields to typesense fieldnames.")
    data_postprocessing_functions: dict | None = Field(default=None, description="Dict of functions to apply to values before pushing them to typesense.")
    label_creator_function: str = Field(default="label_creator_place", description="Function to create the label field.")
    

@flow(version="0.1.1")
def create_typesense_place_index_from_wikidata(params: Params):
    create_typesense_index_from_sparql_query(BaseParams(**params.model_dump()))


if __name__ == "__main__":
    create_typesense_place_index_from_wikidata(Params(incremental_update=False, incremental_date=None))