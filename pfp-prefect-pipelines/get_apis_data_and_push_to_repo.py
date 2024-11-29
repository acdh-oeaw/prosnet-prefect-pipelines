from datetime import datetime
from prefect import flow, get_run_logger
from get_data_from_apis_instance import get_data_from_apis_instance, Params as GetParams
from push_rdf_file_to_github_gitlab import push_data_to_repo_flow, Params as PushParams
from pydantic import BaseModel, Field, FilePath, HttpUrl
from typing import Literal


class Params(BaseModel):
    get_params: GetParams
    push_params: PushParams


@flow(name="Get API Data and Push to GitLab")
def get_apis_data_and_push_to_gitlab(params: Params):
    """
    Flow that fetches data from APIs and pushes it to a GitLab repository.
    """
    logger = get_run_logger()

    logger.info("Starting data collection from APIs")
    # Get data from APIs
    output_file = get_data_from_apis_instance(params.get_params)

    logger.info("Pushing data to GitLab repository")
    # Create push parameters
    # Push to repository
    result = push_data_to_repo_flow(params.push_params)

    logger.info("Flow completed successfully")
    return result


if __name__ == "__main__":
    # Example usage
    api_params = Params(
        get_params=GetParams(
            accept_header="text/ttl",
            secret_token=None,
            limit=200,
            max_objects=500,
            api_url="http://localhost:8000/apis/api/apis_ontology.",
            routes=["person", "graduiertean"],
            output_path="pio_test.nq",
            named_graph_uri="https://oebl-pfp.acdh-ch-dev.oeaw.ac.at",
        ),
        push_params=PushParams(
            repo="acdh-ch/pfp/pfp-source-data",
            username_secret="gitlab-source-data-username",
            password_secret="gitlab-source-data-password",
            git_provider="oeaw-gitlab",
            branch_name="testbranch_5",
            file_path="pio_test.nq",
            file_path_git="datasets/pio_test.nq",
        ),
    )

    get_apis_data_and_push_to_gitlab(params=api_params)
