from string import Template
from typing import List, Optional
from SPARQLWrapper import JSON, SPARQLWrapper
from pydantic import BaseModel, Field, HttpUrl
from prefect import get_run_logger, task, flow
from prefect.concurrency.sync import rate_limit
import typesense
from .push_to_typesense import push_data_to_typesense_flow, Params as PushParams



@task()
def retrieve_data_from_sparql_query(sparql_query, sparql_con, offset=None, limit=None, incremental_date=False, count_query=False):
    """Retrieve data from a SPARQL query."""
    logger = get_run_logger()
    logger.info(f"Retrieving data from SPARQL query: {sparql_query}")
    if not count_query:
        rate_limit("wikidata-sparql-limit")
        query = Template(sparql_query).substitute(offset=offset, limit=limit)
    else:
        query = sparql_query
    sparql_con.setQuery(query)
    if count_query:
        return sparql_con.query().convert()["results"]["bindings"][0]["count"]["value"]
    return sparql_con.query().convert()

@task()
def create_sparql_queries(path_sparql_query, incremental_update, incremental_date):
    with open(path_sparql_query) as f:
        sparql_query = f.readlines()
        if incremental_update and incremental_date:
            for ix, line in enumerate(sparql_query):
                if line.startswith("#REMOVE_INCREMENTAL "):
                    sparql_query[ix] = line.replace("#REMOVE_INCREMENTAL ", "").replace("{{INCREMENTAL_DATE}}", incremental_date)
        sparql_query_count = "".join(["SELECT (COUNT(DISTINCT(?item)) AS ?count)\n", *sparql_query[1:-2]])
        sparql_query = "".join(sparql_query)
    return sparql_query_count, sparql_query



@task()
def create_typesense_data_from_sparql_data(sparql_data, field_mapping):
    """Create typesense data from SPARQL data."""
    res = []
    for item in sparql_data["results"]["bindings"]:
        res2 = {}
        for key, value in item.items():
            if key in field_mapping:
                key = field_mapping[key]
            if key == "id":
                res2["id"] = value["value"].split("/")[-1]
            else:
                res2[key] = value["value"]
        res.append(res2)
    return res

@task()
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    return sparql

class Params(BaseModel):
    path_sparql_query: str = Field(..., description="Relativ path to SPARQL query.")
    sparql_endpoint: HttpUrl = Field("https://query.wikidata.org/", description="SPARQL endpoint to use, defaults to wikidata.")
    limit: int = Field(500, description="Limit to use for the SPARQL queries")
    typesense_definition: dict = Field({
        "name": "prosnet-wikidata-person-index",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "label", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "date_of_birth", "type": "string"},
            {"name": "date_of_death", "type": "string"},
            {"name": "place_of_birth", "type": "string"},
            {"name": "place_of_death", "type": "string"}
        ]
    },  description="Typesense definition to use, if None, incremental backup needs to be set.")
    incremental_update: bool = Field(False, description="If True, only objects changed since last run will be updated.")
    incremental_date: Optional[str] = Field(None, description="Date to use for incremental update, if None, last run of flow will be used.")
    typesense_collection_name: str = Field("prosnet-wikidata-person-index", description="Name of the typesense collection to use.")
    typesense_api_key: str = Field("typesense-api-key", description="Name of the Prefect secrets block that holds the API key to use for typesense.")
    typesense_host: str = Field("typesense.acdh-dev.oeaw.ac.at", description="Host to use for typesense.")
    field_mapping: dict = Field({"itemLabel": "label"}, description="List of tuples to map SPARQL fields to typesense fieldnames.")



@flow(version="0.1.8")
def create_typesense_index_from_sparql_query(params: Params):
    """Create a typesense index from a SPARQL data."""
    sparql_con = setup_sparql_connection(params.sparql_endpoint)
    sparql_count_query, sparql_query = create_sparql_queries(params.path_sparql_query, params.incremental_update, params.incremental_date)
    counts = retrieve_data_from_sparql_query(sparql_count_query, sparql_con, incremental_date=params.incremental_date, count_query=True)
    for offset in range(0, int(counts), params.limit):
        sparql_data = retrieve_data_from_sparql_query.submit(sparql_query, sparql_con, offset, params.limit, incremental_date=params.incremental_date)
        typesense_data = create_typesense_data_from_sparql_data(sparql_data, params.field_mapping)
        push_data_to_typesense_flow(PushParams(
            typesense_collection_name=params.typesense_collection_name,
            typesense_api_key=params.typesense_api_key,
            typesense_host=params.typesense_host,
            typesense_definition=params.typesense_definition,
            data=typesense_data
        ))

