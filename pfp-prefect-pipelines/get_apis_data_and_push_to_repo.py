from datetime import datetime
import time
from prefect import flow, get_run_logger
from get_data_from_apis_instance import get_data_from_apis_instance, Params as GetParams
from push_rdf_file_to_github_gitlab import push_data_to_repo_flow, Params as PushParams
from pydantic import BaseModel, Field, FilePath, HttpUrl
from typing import Literal
from prefect.blocks.notifications import SlackWebhook


class Params(BaseModel):
    get_params: GetParams
    push_params: PushParams


@flow(name="Get API Data and Push to GitLab")
def get_apis_data_and_push_to_gitlab(params: Params):
    """
    Flow that fetches data from APIs and pushes it to a GitLab repository.
    """
    logger = get_run_logger()
    start_time = time.time()
    start_datetime = datetime.now()

    logger.info("Starting data collection from APIs")
    # Get data from APIs
    output_file = get_data_from_apis_instance(params.get_params)

    logger.info("Pushing data to GitLab repository")
    # Create push parameters
    # Push to repository
    result = push_data_to_repo_flow(params.push_params)

    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    hours, remainder = divmod(execution_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    execution_time_formatted = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

    logger.info(f"Flow completed successfully in {execution_time_formatted}")

    # Send notification with execution time
    try:
        slack_webhook_block = SlackWebhook.load("mattermost-workaround")
        api_url = params.get_params.api_url
        repo_path = params.push_params.repo
        file_path = params.push_params.file_path_git

        slack_webhook_block.notify(
            f"Imported data from {api_url} to {repo_path}/{file_path}\n"
            f"Flow started at: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Flow took: {execution_time_formatted}"
        )
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

    return result


if __name__ == "__main__":
    # Example usage
    api_params = Params(
        get_params=GetParams(
            accept_header="text/ttl",
            secret_token=None,
            limit=200,
            # max_objects=500,
            api_url="http://localhost:8000/apis/api/apis_ontology.",
            routes=[
                "person",
                "place",
                "graduiertean" "studiertelerntean",
                "habilitiertesichan",
                "warassistentinan",
                "warprivatdozentinan",
                "warotitoprofessorinan",
                "waraotitaoprofessorinan",
                "warhonorarprofessorinan",
                "warehrendoktorinan",
                "wartaetigfuerwirkteanbei",
                "warmitgruenderinvon",
                "hatteleitungsfunktionan",
                "warmitgliedvon",
                "warrektorinan",
                "wardekaninan",
                "promoviertean",
                "warprofessorinan",
                "graduiertean",
                "warhonorardozentinan",
            ],
            output_path="pio_2024-12-13.ttl",
            # named_graph_uri="https://oebl-pfp.acdh-ch-dev.oeaw.ac.at",
        ),
        push_params=PushParams(
            repo="acdh-ch/pfp/pfp-source-data",
            username_secret="gitlab-source-data-username",
            password_secret="gitlab-source-data-password",
            git_provider="oeaw-gitlab",
            branch_name="pio_branch_4",
            file_path="pio_2024-12-13.ttl",
            file_path_git="datasets/pio_2024-12-13.ttl",
        ),
    )

    get_apis_data_and_push_to_gitlab(params=api_params)
