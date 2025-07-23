from pydantic import BaseModel, Field, HttpUrl
from typing import Literal
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
import os
import shutil
import git
import pyoxigraph as oxi
from prefect.blocks.system import Secret
from string import Template
import tempfile
import json
import uuid


@task()
def create_rdflib_dataset(local_folders, file_types=["ttl", "nt"]):
    """Create an RDF dataset from all RDF files in the specified folders.

    Args:
        local_folders: Path or list of paths to folders containing RDF files
        file_types: List of file extensions to consider (without dot)

    Returns:
        pyoxigraph.Store: Store containing all graphs
    """
    logger = get_run_logger()
    store = oxi.Store()
    logger.info("Start creating local RDF store")

    if isinstance(local_folders, str):
        local_folders = [local_folders]

    for folder in local_folders:
        logger.info(f"Processing directory: {folder}")

        # Walk through all files in the folder
        for root, _, files in os.walk(folder):
            for file in files:
                # Check if file has one of the specified extensions
                if any(file.lower().endswith(f".{ext}") for ext in file_types):
                    file_path = os.path.join(root, file)

                    logger.info(f"Processing file: {file_path}")
                    try:
                        # Load the file into the store
                        store.load(path=file_path)

                    except Exception as e:
                        logger.error(f"Error parsing {file_path}: {str(e)}")
                        raise

    logger.info(f"Loaded {len(store)} triples/quads into the store")
    return store


@task(cache_policy=NO_CACHE)
def execute_sparql(graph, sparql_template):
    logger = get_run_logger()
    with open(sparql_template, "r+") as query:
        try:
            res = graph.query(query.read())
        except Exception as e:
            logger.error(f"Error while retrieving counts: {e}")
            raise e
    with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as tmp:
        res.serialize(output=tmp, format=oxi.QueryResultsFormat.JSON)
        logger.info(f"Successfully wrote SPARQL to {tmp}")
        fn = tmp.name

    return fn


@task(cache_policy=NO_CACHE)
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


def merge_shared_values(input_dict):
    from collections import defaultdict

    # Create a mapping from each value to all keys containing that value
    value_to_keys = defaultdict(set)
    for key, values in input_dict.items():
        for value in values:
            value_to_keys[value].add(key)

    # Create a new dictionary to store the merged results
    merged_dict = {}

    # Merge values across all keys that share a common value
    for key, values in input_dict.items():
        merged_values = set(values)
        for value in values:
            for related_key in value_to_keys[value]:
                merged_values.update(input_dict[related_key])
        merged_dict[key] = list(merged_values)

    return merged_dict


@task()
def create_provided_entities_graph(
    graph: oxi.Store,
    sameas: str,
    prov_ent: str,
    output_path: os.PathLike,
    base_ns: str,
    proxi_for: str,
):
    logger = get_run_logger()

    try:
        with open(sameas, "r") as f:
            sameas_data = json.loads(f.read())
        logger.info(
            f"Loaded sameas JSON with {len(sameas_data['results']['bindings'])} results"
        )
    except Exception as e:
        logger.error(f"Error reading sameas JSON file: {e}")
        raise

    try:
        with open(prov_ent, "r") as f:
            prov_ent_data = json.loads(f.read())
        logger.info(
            f"Loaded provided entities JSON with {len(prov_ent_data['results']['bindings'])} results"
        )
    except Exception as e:
        logger.error(f"Error reading provided entities JSON file: {e}")
        raise
    sa_index = {}
    for pers in sameas_data["results"]["bindings"]:
        if "sa" not in pers:
            continue
        if pers["sa"]["value"] in sa_index:
            if pers["entity"]["value"] not in sa_index[pers["sa"]["value"]]:
                sa_index[pers["sa"]["value"]].append(pers["entity"]["value"])
        else:
            sa_index[pers["sa"]["value"]] = [
                pers["entity"]["value"],
            ]
    sa_index = merge_shared_values(sa_index)
    ent_index = {}
    for pers in prov_ent_data["results"]["bindings"]:
        if pers["ent"]["value"] in ent_index:
            if pers["ent"]["value"] not in ent_index[pers["ent"]["value"]]:
                ent_index[pers["ent"]["value"]].append(pers["prov_ent"]["value"])
        else:
            ent_index[pers["ent"]["value"]] = [
                pers["prov_ent"]["value"],
            ]
    for v in sa_index.values():
        pre_sa = []
        for ent in v:
            if ent in ent_index:
                pre_sa.extend(ent_index[ent])
        if len(pre_sa) == 0:
            id = oxi.Literal(str(uuid.uuid4()))
            for e2 in v:
                ent_index[e2] = [id]
            pre_sa = [id]
        elif len(pre_sa) > 1:
            logger.warning(f"Multiple entities found for {v}")
        for ent in v:
            for id in pre_sa:
                graph.add(
                    oxi.Quad(
                        oxi.NamedNode(ent),
                        oxi.NamedNode(proxi_for),
                        oxi.Literal(id) if isinstance(id, str) else id,
                    )
                )
    oxi.serialize(graph, output=output_path, format=oxi.RdfFormat.TURTLE)

    return output_path


@task(cache_policy=NO_CACHE)
def serialize_to_file(graph, path):
    with open(path, "ab") as output:
        res = graph.serialize(output=output)
    return res


@task(cache_policy=NO_CACHE)
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
    ontology_folders: list = Field(
        ["cidoc-crm"],
        description="List of additional ontology files to add to the store.",
    )
    file_name_enrichment: str = Field(
        "provided_entities.ttl", description="Filename to use for the enriched dataset."
    )
    schema_namespace: str = Field(
        "https://pfp-schema.acdh-ch-dev.oeaw.ac.at/schema#",
        description="Base URL to use for the schema/ontology.",
    )
    commit_message: str = Field("feat: add new provided graph [skip ci]")


@flow()
def create_provided_entities(params: Params):
    username = Secret.load(params.username_secret).get()
    password = Secret.load(params.password_secret).get()
    if params.git_provider == "github":
        remote = f"https://{username}:{password}@github.com/{params.repo}.git"
    else:
        remote = f"https://{username}:{password}@gitlab.oeaw.ac.at/{params.repo}.git"
        repo, full_local_path = clone_repo(
            remote, params.branch_name, params.local_folder, True
        )
    all_folders = [os.path.join(full_local_path, params.dataset_folder)]
    for ontology_folder in params.ontology_folders:
        all_folders.append(os.path.join(full_local_path, ontology_folder))
    graph = create_rdflib_dataset(all_folders)
    sameas = execute_sparql(
        graph,
        "pfp-prefect-pipelines/sparql/retrieve_sameas.sparql",
    )
    prov_ent = execute_sparql(
        graph,
        "pfp-prefect-pipelines/sparql/retrieve_provided_entities.sparql",
    )
    # Create path relative to current script location
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = os.getcwd()
    output_path = os.path.join(
        current_dir,
        params.local_folder,
        params.dataset_folder,
        params.file_name_enrichment,
    )
    graph_prov_entity = oxi.Store()
    res = create_provided_entities_graph(
        graph_prov_entity,
        sameas,
        prov_ent,
        output_path,
        params.schema_namespace,
        f"{params.schema_namespace}proxy_for",
    )
    commit_and_push(repo, commit_message=params.commit_message)


if __name__ == "__main__":
    create_provided_entities(
        Params(
            repo="acdh-ch/pfp/pfp-source-data",
            username_secret="gitlab-source-data-username",
            password_secret="gitlab-source-data-password",
            branch_name="origin/main",
            git_provider="oeaw-gitlab",
        )
    )
