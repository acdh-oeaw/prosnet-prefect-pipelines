from datetime import time
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from pydantic import BaseModel, Field, HttpUrl
import requests
from rdflib import Graph, URIRef, Dataset
from typing import Iterator, Literal
import time


@task
def get_data_from_route(
    route: str, headers: dict, limit: int, max_objects: int | None = None
) -> Iterator[str]:
    """Fetch data from API route using pagination."""
    logger = get_run_logger()
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(route, headers=headers, params=params)
        response.raise_for_status()
        time.sleep(5)
        if not response.text.strip():
            break
        if max_objects is not None:
            if offset > max_objects:
                break

        logger.info(f"Retrieved data from {route} with offset {offset}")
        yield response.text

        # if len(response.text.splitlines()) < limit:
        #    break

        offset += limit


@task
def combine_ttl_data(
    ttl_chunks: list[str], named_graph_uri: str | None = None
) -> Dataset | Graph:
    """Combine TTL chunks into a single named graph."""
    logger = get_run_logger()
    if named_graph_uri is not None:
        combined_graph = Dataset()
        context = URIRef(named_graph_uri)
        g = combined_graph.graph(context)
    else:
        g = Graph()
        combined_graph = g
    for chunk in ttl_chunks:
        g.parse(data=chunk, format="turtle")
    logger.info(f"Combined {len(combined_graph)} triples into graph")
    return combined_graph


@task
def serialize_graph(graph: Graph, output_path: str, format: str = "ttl"):
    """Serialize the graph to."""
    logger = get_run_logger()
    graph.serialize(destination=output_path, format=format)
    logger.info(f"Serialized graph to {output_path}")


class Params(BaseModel):
    accept_header: str = Field(
        "text/ttl", description="Value to use for the accept header."
    )
    secret_token: str | None = Field(
        None, description="Secret to use for Token if needed."
    )
    limit: int = Field(200, description="Limit to use in API calls.")
    max_objects: int | None = Field(
        None, description="Max objects to retrieve. Set to None for all."
    )
    routes: list = Field(
        None, description="Routes to use. If nothing is set all will be used."
    )
    api_url: HttpUrl = Field(..., description="Base url of the API endpoint to use.")
    named_graph_uri: str | None = Field(None, description="URI for the named graph")
    output_path: str = Field(..., description="Path where to save the NQuads file")
    graph_format: Literal["ttl", "nq"] = Field(
        "ttl", description="Graph format to use for serialization."
    )


@flow()
def get_data_from_apis_instance(params: Params):
    """Main flow to fetch data from APIs and create a named graph NQuads file."""
    logger = get_run_logger()

    # Setup headers
    headers = {"accept": params.accept_header}
    if params.secret_token is not None:
        headers["Authorization"] = f"Token {Secret.load(params.secret_token).get()}"

    # Determine which routes to use
    # routes = params.routes if params.routes is not None else params.api_routes
    routes = [f"{params.api_url}{r}/" for r in params.routes]
    # Collect all TTL data
    all_ttl_chunks = []
    for route in routes:
        logger.info(f"Processing route: {route}")
        chunks = get_data_from_route(route, headers, params.limit, params.max_objects)
        all_ttl_chunks.extend(chunks)

    # Combine data into named graph
    combined_graph = combine_ttl_data(all_ttl_chunks, params.named_graph_uri)

    # Serialize to NQuads
    serialize_graph(combined_graph, params.output_path, params.graph_format)

    return params.output_path


if __name__ == "__main__":
    result = get_data_from_apis_instance(
        Params(
            max_objects=500,
            api_url="http://localhost:8000/apis/api/apis_ontology.",
            routes=["person", "graduiertean"],
            output_path="test",
            # named_graph_uri="http://test.at",
        )
    )
