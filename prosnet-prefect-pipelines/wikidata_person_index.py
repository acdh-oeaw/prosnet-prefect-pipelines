
from prefect import flow
from pydantic import BaseModel, Field, HttpUrl
from wikidata_index import create_typesense_index_from_sparql_query
from wikidata_index import Params as BaseParams

    
class Params(BaseModel):
    path_sparql_query: str = Field(default="prosnet-prefect-pipelines/sparql/wikidata-person.sparql", description="Relativ path to SPARQL query.")
    sparql_endpoint: HttpUrl = Field(default="https://query.wikidata.org/sparql", description="SPARQL endpoint to use, defaults to wikidata.")
    limit: int = Field(default=200, description="Limit to use for the SPARQL queries")
    typesense_definition: dict = Field(default={
        "name": "prosnet-wikidata-person-index",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "description", "type": "string", "optional": True},
            {"name": "label", "type": "string"},
            {"name": "name", "type": "string", "optional": True},
            {"name": "date_of_birth", "type": "string", "optional": True},
            {"name": "date_of_death", "type": "string", "optional": True},
            {"name": "place_of_birth", "type": "string", "optional": True},
            {"name": "place_of_death", "type": "string", "optional": True},
        ]
    },  description="Typesense definition to use, if None, incremental backup needs to be set.")
    incremental_update: bool = Field(default=True, description="If True, only objects changed since last run will be updated.")
    incremental_date: int = Field(default=2, description="Number of days to retrieve update for (today - days).")
    typesense_collection_name: str = Field(default="prosnet-wikidata-person-index", description="Name of the typesense collection to use.")
    typesense_api_key: str = Field(default="typesense-api-key", description="Name of the Prefect secrets block that holds the API key to use for typesense.")
    typesense_host: str = Field(default="typesense.acdh-dev.oeaw.ac.at", description="Host to use for typesense.")
    field_mapping: dict = Field(default={
        "itemLabel": "name",
        "place_of_birthLabel": "place_of_birth",
        "place_of_deathLabel": "place_of_death",
        }, description="List of tuples to map SPARQL fields to typesense fieldnames.")
    data_postprocessing_functions: dict = Field(default={
        "date_of_birth": "date_postprocessing",
        "date_of_death": "date_postprocessing",
        }, description="Dict of functions to apply to values before pushing them to typesense.")
    label_creator_function: str = Field(default="label_creator_person", description="Function to create the label field.")

@flow(version="0.1.1")
def create_typesense_person_index_from_wikidata(params: Params):
    create_typesense_index_from_sparql_query(BaseParams(**params.model_dump()))