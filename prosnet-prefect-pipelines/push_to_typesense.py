
import prefect
from pydantic import BaseModel, Field
import typesense

@prefect.task()
def setup_typesense_connection(typesense_api_key, typesense_host):
    """Setup a typesense connection."""
    api_key = prefect.secrets.get(typesense_api_key)
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
        collection = typesense_con.collections.upsert(typesense_definition)
    else:
        collection = typesense_con.collections[collection_name]
    return collection

@prefect.task()
def push_data_to_typesense(collection, data):
    """Push data to typesense."""
    logger = prefect.get_run_logger()
    logger.info(f"Pushing {len(data)} items to typesense.")
    res = collection.documents.import_(data, {'action': 'upsert'})
    logger.info(f"Got {res['success']} successes and {res['failed']} failures.")
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
    typesense_con = setup_typesense_connection(params.typesense_api_key, params.typesense_host)
    collection = get_or_create_typesense_collection(params.typesense_collection_name, typesense_con, params.typesense_definition)
    push_data_to_typesense(collection, params.data)

