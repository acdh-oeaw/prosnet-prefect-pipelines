import datetime
import re
from string import Template
from typing import List, Optional
from SPARQLWrapper import JSON, SPARQLWrapper
from pydantic import BaseModel, Field, HttpUrl
from prefect import get_run_logger, task, flow
from prefect.tasks import exponential_backoff
from prefect.concurrency.sync import rate_limit
import typesense
from push_to_typesense import push_data_to_typesense_flow, Params as PushParams
from prefect.artifacts import create_markdown_artifact


def date_postprocessing(x):
    if x.startswith("http"):
        return None
    return x.split("T")[0]


def cocatenated_dates_postprocessing(x):
    return " / ".join([date_postprocessing(y) for y in x.split()])


def label_creator_person(data):
    label = data["name"]
    if "date_of_birth" in data or "date_of_death" in data:
        label += " ("
        if "date_of_birth" in data:
            if data["date_of_birth"] is not None:
                label += data["date_of_birth"].split("-")[0]
        if "date_of_death" in data:
            if data["date_of_death"] is not None:
                label += " - " + data["date_of_death"].split("-")[0]
        label += ")"
    if "description" in data:
        if data["description"] is not None:
            label += ": " + data["description"]
    return label


def label_creator_place(data):
    label = data["name"]
    if "country" in data:
        if data["country"] is not None:
            label += " (" + data["country"] + ")"
    if "feature_code" in data:
        if data["feature_code"] is not None:
            label += " - " + data["feature_code"]
    return label


def label_creator_organization(data):
    label = data["name"]
    if "inception" in data:
        if data["inception"] is not None:
            if len(data["inception"]) > 4:
                label += " (" + data["inception"]
    if "dissolvement" in data:
        if data["dissolvement"] is not None:
            if len(data["dissolvement"]) > 4:
                label += " - " if "(" in label else "( - "
                label += data["dissolvement"] + ")"
    if "(" in label and ")" not in label:
        label += "-)"
    if "description" in data:
        if data["description"] is not None:
            label += ": " + data["description"]
    return label


def geopoint_creator(data):
    if data.startswith("Point"):
        long, lat = data.replace("Point(", "").replace(")", "").split(" ")
        return [float(lat), float(long)]
    return None


def feature_code_postprocessing(feature_code):
    feture_codes_rank = [
        "PPLC",
        "PPLCH",
        "PPLA",
        "PPLA2",
        "PPLA3",
        "PPLA4",
        "PPL",
        "PPLF",
        "PPLG",
        "PPLH",
        "PPLL",
        "PPLQ",
        "PPLR",
        "PPLS",
        "PPLW",
        "PPLX",
        "STLMT",
        "ADM1",
        "ADM1H",
        "ADM2",
        "ADM2H",
        "ADM3",
        "ADM3H",
        "ADM4",
        "ADM4H",
        "ADM5",
        "ADM5H",
        "ADMD",
        "ADMDH",
    ]
    codes = feature_code.split()
    p_a_codes = [
        code.split(".")[1]
        for code in codes
        if (code.startswith("P") or code.startswith("A"))
    ]
    if len(p_a_codes) > 0:
        return min(
            (s for s in p_a_codes if s in feture_codes_rank),
            key=feture_codes_rank.index,
            default=None,
        )

    elif len(codes) > 0:
        return codes[0]
    return None


@task(retries=6, retry_delay_seconds=exponential_backoff(backoff_factor=30))
def retrieve_data_from_sparql_query(
    sparql_query,
    sparql_con,
    offset=None,
    limit=None,
    incremental_date=False,
    count_query=False,
):
    """Retrieve data from a SPARQL query."""
    logger = get_run_logger()
    if not count_query:
        rate_limit("wikidata-sparql-limit")
        query = Template(sparql_query).substitute(offset=offset, limit=limit)
        logger.info(f"Retrieving data from SPARQL query: {query}")
    else:
        query = sparql_query
        logger.info(f"Retrieving count from SPARQL query: {query}")
    sparql_con.setQuery(query)
    if count_query:
        res = sparql_con.query().convert()
        logger.info(f"res : {res}")
        return res["results"]["bindings"][0]["count"]["value"]
    return sparql_con.query().convert()


@task()
def create_sparql_queries(path_sparql_query, incremental_update, incremental_date):
    with open(path_sparql_query) as f:
        sparql_query = f.readlines()
        if incremental_update and incremental_date:
            incremental_date = (
                datetime.datetime.now() - datetime.timedelta(days=incremental_date)
            ).strftime("%Y-%m-%d")
            for ix, line in enumerate(sparql_query):
                if "#REMOVE_INCREMENTAL" in line:
                    sparql_query[ix] = line.replace("#REMOVE_INCREMENTAL ", "").replace(
                        "{{INCREMENTAL_DATE}}", incremental_date
                    )
        elif incremental_update and incremental_date is None:
            raise ValueError(
                "incremental_date must be set if incremental_update is True."
            )
        sparql_query = "".join(sparql_query)
        sparql_query_count = (
            "SELECT (COUNT(DISTINCT ?item) AS ?count)\nWHERE {\n"
            + re.search(
                r"WHERE.*WHERE\s*\{(.*)\}.*?LIMIT", sparql_query, flags=re.M | re.DOTALL
            ).group(1)
            + "\n}"
        )
    return sparql_query_count, sparql_query


@task()
def create_typesense_data_from_sparql_data(
    sparql_data, field_mapping, postprocessing_functions, label_creator_function=False
):
    """Create typesense data from SPARQL data."""
    res = []
    for item in sparql_data["results"]["bindings"]:
        res2 = {}
        for key, value in item.items():
            if key in field_mapping:
                key = field_mapping[key]
            if postprocessing_functions is not None:
                if key in postprocessing_functions:
                    value["value"] = globals()[postprocessing_functions[key]](
                        value["value"]
                    )
            if key == "id":
                q = value["value"].split("/")[-1]
                res2["id"] = f"http://www.wikidata.org/entity/{q}"
            else:
                res2[key] = value["value"]
        if label_creator_function:
            res2["label"] = globals()[label_creator_function](res2)
        res.append(res2)
    return res


@task()
def setup_sparql_connection(endpoint):
    sparql = SPARQLWrapper(endpoint.__str__())
    sparql.setReturnFormat(JSON)
    return sparql


class Params(BaseModel):
    path_sparql_query: str = Field(
        default="prosnet-prefect-pipelines/sparql/wikidata-person.sparql",
        description="Relativ path to SPARQL query.",
    )
    sparql_endpoint: HttpUrl = Field(
        default="https://query.wikidata.org/sparql",
        description="SPARQL endpoint to use, defaults to wikidata.",
    )
    limit: int = Field(default=200, description="Limit to use for the SPARQL queries")
    typesense_definition: dict = Field(
        default={
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
            ],
        },
        description="Typesense definition to use, if None, incremental backup needs to be set.",
    )
    incremental_update: bool = Field(
        default=True,
        description="If True, only objects changed since last run will be updated.",
    )
    incremental_date: int | None = Field(
        default=None,
        description="Number of days to retrieve update for (today - days).",
    )
    typesense_collection_name: str = Field(
        default="prosnet-wikidata-person-index",
        description="Name of the typesense collection to use.",
    )
    typesense_api_key: str = Field(
        default="typesense-api-key",
        description="Name of the Prefect secrets block that holds the API key to use for typesense.",
    )
    typesense_host: str = Field(
        default="typesense.acdh-dev.oeaw.ac.at",
        description="Host to use for typesense.",
    )
    field_mapping: dict = Field(
        default={
            "itemLabel": "name",
            "place_of_birthLabel": "place_of_birth",
            "place_of_deathLabel": "place_of_death",
        },
        description="List of tuples to map SPARQL fields to typesense fieldnames.",
    )
    data_postprocessing_functions: dict = Field(
        default={},
        description="Dict of functions to apply to values before pushing them to typesense.",
    )
    label_creator_function: str = Field(
        default="label_creator_person",
        description="Function to create the label field.",
    )


@flow(version="0.1.23")
def create_typesense_index_from_sparql_query(params: Params = Params()):
    """Create a typesense index from a SPARQL data."""
    sparql_con = setup_sparql_connection(params.sparql_endpoint)
    sparql_count_query, sparql_query = create_sparql_queries(
        params.path_sparql_query, params.incremental_update, params.incremental_date
    )
    # counts = retrieve_data_from_sparql_query(sparql_count_query, sparql_con, incremental_date=params.incremental_date, count_query=True)
    counts_typesense = 0
    offset = 0
    while offset >= 0:
        sparql_data = retrieve_data_from_sparql_query.submit(
            sparql_query,
            sparql_con,
            offset,
            params.limit,
            incremental_date=params.incremental_date,
        )
        typesense_data = create_typesense_data_from_sparql_data(
            sparql_data,
            params.field_mapping,
            params.data_postprocessing_functions,
            params.label_creator_function,
        )
        c_1 = len(typesense_data)
        counts_typesense += c_1
        if c_1 == 0:
            offset = False
            break
        push_data_to_typesense_flow(
            PushParams(
                typesense_collection_name=params.typesense_collection_name,
                typesense_api_key=params.typesense_api_key,
                typesense_host=params.typesense_host,
                typesense_definition=params.typesense_definition,
                data=typesense_data,
            )
        )
        offset += params.limit
    logger = get_run_logger()
    logger.info(f"Pushed {counts_typesense} objects to typesense.")
    typense_report = f"""
# Wikidata Person Index {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Pushed {counts_typesense} objects to typesense.
"""
    create_markdown_artifact(
        key="typesense-push-report",
        markdown=typense_report,
        description="Report of objects pushed to typesense.",
    )


if __name__ == "__main__":
    create_typesense_index_from_sparql_query(Params())
