
from datetime import timedelta
import prefect
from pydantic import BaseModel, Field
import typesense

@prefect.task()
def setup_typesense_connection(typesense_api_key, typesense_host):
    """Setup a typesense connection."""
    api_key = prefect.blocks.system.Secret.load(typesense_api_key).get()
    client = typesense.Client({
    'api_key': api_key,
    'nodes': [{
        'host': typesense_host,
        'port': '443',
        'protocol': 'https'
    }],
    'connection_timeout_seconds': 2
    })
    return client

@prefect.task()
def get_or_create_typesense_collection(collection_name, typesense_con, typesense_definition):
    """Get or create a typesense collection."""
    if typesense_definition:
        cols = typesense_con.collections.retrieve()
        if collection_name in [col["name"] for col in cols]:
            collection = typesense_con.collections[collection_name].retrieve()
        else:
            collection = typesense_con.collections.create(typesense_definition)
    else:
        collection = typesense_con.collections[collection_name].retrieve()
    return collection["name"]

@prefect.task()
def push_data_to_typesense(client, collection_name, data):
    """Push data to typesense."""
    logger = prefect.get_run_logger()
    logger.info(f"Pushing {len(data)} items to typesense.")
    res = client.collections[collection_name].documents.import_(data, {'action': 'upsert'})
    logger.info(f"res : {res}")
    return True

class Params(BaseModel):
    typesense_collection_name: str = Field(..., description="Name of the typesense collection to use.")
    typesense_api_key: str = Field("typesense_api_key", description="Name of the Prefect secrets block that holds the API key to use for typesense.")
    typesense_host: str = Field("typesense.acdh-dev.oeaw.ac.at", description="Host to use for typesense.")
    typesense_definition: dict | None = Field(None, description="Typesense definition to use, if None, existing collection is used.")
    data: list = Field(..., description="Data to push to typesense.")


@prefect.flow
def push_data_to_typesense_flow(params: Params):
    """Push data to typesense."""
    logger = prefect.get_run_logger()
    logger.info(f"Pushing {len(params.data)} items to typesense.")
    typesense_con = setup_typesense_connection(params.typesense_api_key, params.typesense_host)
    collection_name = get_or_create_typesense_collection(params.typesense_collection_name, typesense_con, params.typesense_definition)
    push_data_to_typesense(typesense_con, collection_name, params.data)

