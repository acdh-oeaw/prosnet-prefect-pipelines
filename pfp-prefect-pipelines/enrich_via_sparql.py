from pydantic import BaseModel, Field, HttpUrl
from typing import Literal
from prefect import flow, get_run_logger, task
import os
import shutil
import git
import pyoxigraph as oxi
from prefect.blocks.system import Secret
from string import Template


@task()
def create_rdflib_dataset(local_folder, file_types=["ttl"]):
    """Create an RDF dataset from all RDF files in the specified folder.

    Args:
        local_folder: Path to folder containing RDF files
        file_types: List of file extensions to consider (without dot)

    Returns:
        rdflib.Dataset: Dataset containing all graphs
    """
    logger = get_run_logger()
    store = oxi.Store()

    # Walk through all files in the folder
    for root, _, files in os.walk(local_folder):
        for file in files:
            # Check if file has one of the specified extensions
            if any(file.lower().endswith(f".{ext}") for ext in file_types):
                file_path = os.path.join(root, file)

                # Determine format based on file extension

                logger.info(f"Processing file: {file_path}")
                try:
                    # For N-Quads files which contain named graphs
                    store.load(path=file_path)

                except Exception as e:
                    logger.error(f"Error parsing {file_path}: {str(e)}")
                    raise

    # Log some statistics

    return store


@task()
def create_named_graph(graph, sparql_template, file_path):
    logger = get_run_logger()
    with open(sparql_template, "r+") as query:
        try:
            res = graph.query(query.read())
            res.serialize(
                output=file_path,
            )
        except Exception as e:
            logger.error(f"Error while retrieving counts: {e}")
            raise e
    return file_path


@task()
def clone_repo(remote, branch, local_folder, force=True):
    logger = get_run_logger()
    full_local_path = os.path.join(os.getcwd(), local_folder)

    if os.path.exists(full_local_path):
        if force:
            shutil.rmtree(full_local_path)
        else:
            # Find a new unique folder name by adding incrementing number
            counter = 1
            while os.path.exists(full_local_path):
                base_path = full_local_path.rstrip("0123456789")
                full_local_path = f"{base_path}{counter}"
                counter += 1
            logger.info(f"Original folder exists, using {full_local_path} instead")

    logger.info(f"Cloning {remote} to {full_local_path}")
    clean_branch_name = branch.replace("origin/", "")
    repo = git.Repo.clone_from(
        remote, full_local_path, multi_options=[f"--branch={clean_branch_name}"]
    )
    return repo, full_local_path


@task()
def serialize_to_file(graph, path):
    with open(path, "ab") as output:
        res = graph.serialize(output=output)
    return res


@task()
def commit_and_push(repo, commit_message="feat: adds new graph"):
    logger = get_run_logger()
    origin = repo.remote(name="origin")
    current_branch = repo.active_branch
    repo.git.add(".")
    repo.index.commit(commit_message)
    origin.push(refspec=f"{current_branch}:{current_branch}")
    logger.info("Pushed everything to remote")
    return True


class Params(BaseModel):
    repo: str = Field(
        "intavia/source-data",
        description="GitHub Repository in the format 'OWNER/{GROUP}/REPO'",
    )
    branch_name: str = Field(
        "main",
        description="Branch name to clone",
    )
    username_secret: str = Field(
        "github-username",
        description="Name of the prefect secret that contains the username",
    )
    password_secret: str = Field(
        "github-password",
        description="Name of the prefect secret that contains the password. Use tokens instead of your personal password.",
    )
    git_provider: Literal["oeaw-gitlab", "github"] = Field(
        "oeaw-gitlab",
        description="The git-provider to use. Options are OEAW-GitLab and GitHub.",
    )
    local_folder: str = Field(
        "source-data", description="Local folder to clone the repo to"
    )
    dataset_folder: str = Field(
        "datasets",
        description="Relative location of the folder that contains the datasets to use.",
    )
    sparql_template: str = Field(
        "sparql/create_provided_entities_graph.sparql",
        description="Path to the sparql template that is used for the enrichment",
    )
    file_name_enrichment: str = Field(
        "provided_entities.ttl", description="Filename to use for the enriched dataset."
    )


@flow()
def enrich_via_sparql(params: Params):
    username = Secret.load(params.username_secret).get()
    password = Secret.load(params.password_secret).get()
    if params.git_provider == "github":
        remote = f"https://{username}:{password}@github.com/{params.repo}.git"
    elif params.git_provider == "oeaw-gitlab":
        remote = f"https://{username}:{password}@gitlab.oeaw.ac.at/{params.repo}.git"
    #   repo, full_local_path = clone_repo(
    #       remote, params.branch_name, params.local_folder, True
    #   )
    full_local_path = os.path.normpath(
        "/home/sennierer/projects/prosnet-prefect-pipelines/pfp-prefect-pipelines/source-data"
    )
    graph = create_rdflib_dataset(os.path.join(full_local_path, params.dataset_folder))
    graph = create_named_graph(
        graph,
        params.sparql_template,
        os.path.join(
            full_local_path, params.dataset_folder, params.file_name_enrichment
        ),
    )


#    commit_and_push(repo)


if __name__ == "__main__":
    enrich_via_sparql(
        Params(
            repo="acdh-ch/pfp/pfp-source-data",
            username_secret="gitlab-source-data-username",
            password_secret="gitlab-source-data-password",
            branch_name="origin/pio_branch_9",
            git_provider="oeaw-gitlab",
            sparql_template="sparql/create_provided_entities_graph_v2.sparql",
        )
    )
