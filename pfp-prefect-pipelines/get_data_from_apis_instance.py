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


@task
def process_routes(
    routes: list | None,
    swagger_tags: list | None,
    api_url: HttpUrl,
    swagger: HttpUrl | None,
) -> list:
    """Process routes list and swagger tags to create complete API URLs.

    Args:
        routes: List of route suffixes to append to api_url
        swagger_tags: List of swagger tags to look up in swagger definition
        api_url: Base URL to append routes to
        swagger: URL to the swagger definition

    Returns:
        list: List of complete URLs to query
    """
    logger = get_run_logger()
    processed_routes = []

    # Process direct routes if provided
    if routes:
        for route in routes:
            # Remove leading/trailing slashes for consistent joining
            clean_route = route.strip("/")
            full_url = f"{str(api_url).rstrip('/')}/{clean_route}/"
            processed_routes.append(full_url)
            logger.info(f"Added direct route: {full_url}")

    # Process swagger tags if both swagger URL and tags are provided
    if swagger and swagger_tags:
        try:
            params = {"format": "json"}
            response = requests.get(swagger, params=params)
            # response.raise_for_status()
            swagger_def = response.json()

            # Process each swagger tag
            for tag in swagger_tags:
                tag = tag.lower()
                found = False
                for path, methods in swagger_def.get("paths", {}).items():
                    for method in methods.values():
                        if "tags" in method and tag in [
                            t.lower() for t in method["tags"]
                        ]:
                            # Use server URL from swagger if available, otherwise use provided api_url
                            base_url = (
                                swagger_def.get("servers", [{}])[0]
                                .get(
                                    "url",
                                    str(
                                        "/".join(
                                            [x for x in str(api_url).split("/")][:3]
                                        )
                                    ),
                                )
                                .rstrip("/")
                            )
                            full_url = f"{base_url}{path}"
                            processed_routes.append(full_url)
                            found = True
                            logger.info(
                                f"Found swagger route for tag '{tag}': {full_url}"
                            )

                if not found:
                    logger.warning(f"No routes found for swagger tag: {tag}")

        except Exception as e:
            logger.error(f"Failed to fetch swagger definition: {e}")
            raise

    if not processed_routes:
        logger.warning(
            "No routes were processed. Check if routes or swagger_tags were provided."
        )

    logger.info(f"Processed {len(processed_routes)} total routes")
    return [url for url in processed_routes if "{" not in url]


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
        None,
        description="Routes to use. Strings will be appended to the api_url and used as routes.",
    )
    swagger_url: HttpUrl = Field(
        None,
        description="Url to read API definition from. Needs to return a swagger definition yaml.",
    )
    swagger_tags: list = Field(
        None,
        description="List of tags that should be included in the lost of endpoints.",
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
    # routes = [f"{params.api_url}{r}/" for r in params.routes]
    routes = process_routes(
        params.routes, params.swagger_tags, params.api_url, params.swagger_url
    )
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
            # max_objects=500,
            secret_token="oebl-pfp-api-token",
            api_url="https://oebl-pfp.acdh-ch-dev.oeaw.ac.at/apis/api/apis_ontology.",
            swagger_url="https://oebl-pfp.acdh-ch-dev.oeaw.ac.at/apis/swagger/schema/",
            swagger_tags=["rdfexport"],
            output_path="pio_data_18-1-25.ttl",
            # named_graph_uri="http://test.at",
        )
    )
