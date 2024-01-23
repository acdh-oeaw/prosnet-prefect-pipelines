from SPARQLWrapper import JSON, SPARQLWrapper
from pydantic import BaseModel, Field, HttpUrl
from prefect import task, flow
from prefect.concurrency.sync import rate_limit

@task()
def create_typesense_definition_from_sparql_data(sparql_data):
    """Create a typesense definition from data retrieved from SPARQL."""
    pass 

@task()
def get_item_counts_from_sparql_query(sparql_query, sparql_con):
"""Get item counts from a SPARQL query."""
    pass

@task()
def retrieve_data_from_sparql_query(sparql_query, offset, limit, sparql_con):
    """Retrieve data from a SPARQL query."""
    rate_limit("wikidata-sparql-limit")
    pass


@task()
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    return sparql

class Params(BaseModel):
    path_sparql_query: str = Field(..., description="Relativ path to SPARQL query.")
    sparql_endpoint: HttpUrl = Field("https://query.wikidata.org/", description="SPARQL endpoint to use, defaults to wikidata.")
    limit: int = Field(500, description="Limit to use for the SPARQL queries")



@flow()
def create_typesense_definition_from_sparql_query(params: Params):
    """Create a typesense definition from a SPARQL query."""
    sparql_con = setup_sparql_connection(params.sparql_endpoint)
    with open(params.path_sparql_query) as f:
        sparql_query = f.readlines()
        sparql_query_count = "".join(["SELECT (COUNT(DISTINCT(?item)) AS ?count)\n", *sparql_query[1:-2]])
        counts = get_item_counts_from_sparql_query(sparql_query_count, sparql_con)
    for offset in range(0, counts, params.limit):
        sparql_data = retrieve_data_from_sparql_query(sparql_query, offset, params.limit)
        if offset == 0:
            typesense_definition = create_typesense_definition_from_sparql_data(sparql_data)
        retrieve_data_from_sparql_query.submit("".join(sparql_query), offset, params.limit, sparql_con)

