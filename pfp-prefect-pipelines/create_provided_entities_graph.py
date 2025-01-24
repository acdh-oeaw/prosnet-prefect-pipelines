from pydantic import BaseModel, Field, HttpUrl
from typing import Literal
from prefect import flow, get_run_logger, task
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
    crm_url = "https://cidoc-crm.org/rdfs/7.1.3/CIDOC_CRM_v7.1.3.rdf"
    try:
        import requests

        response = requests.get(crm_url)
        if response.status_code == 200:
            # Create a temporary file to store the ontology

            with tempfile.NamedTemporaryFile(mode="wb", suffix=".rdfs") as tmp:
                tmp.write(response.content)
                tmp.flush()
                store.load(path=tmp.name, format=oxi.RdfFormat.RDF_XML)
                logger.info("Successfully loaded CIDOC-CRM v7.1.3")
        else:
            logger.warning(f"Failed to fetch CIDOC-CRM: {response.status_code}")
    except Exception as e:
        logger.error(f"Error loading CIDOC-CRM: {str(e)}")
        raise

    # Log some statistics

    return store


@task()
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
            id = oxi.NamedNode(f"{base_ns}/{uuid.uuid4()}".replace("//", "/"))
            for e2 in v:
                ent_index[e2] = [id]
        else:
            id = pre_sa[0]
        for ent in v:
            graph.add(oxi.Quad(oxi.NamedNode(ent), oxi.NamedNode(proxi_for), id))
    oxi.serialize(graph, output=output_path, format=oxi.RdfFormat.TURTLE)

    return output_path


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
    file_name_enrichment: str = Field(
        "provided_entities.ttl", description="Filename to use for the enriched dataset."
    )
    schema_namespace: str = Field(
        "https://pfp-schema.acdh-ch-dev.oeaw.ac.at/schema#",
        description="Base URL to use for the schema/ontology.",
    )


@flow()
def enrich_via_sparql(params: Params):
    username = Secret.load(params.username_secret).get()
    password = Secret.load(params.password_secret).get()
    if params.git_provider == "github":
        remote = f"https://{username}:{password}@github.com/{params.repo}.git"
    else:
        remote = f"https://{username}:{password}@gitlab.oeaw.ac.at/{params.repo}.git"
    repo, full_local_path = clone_repo(
        remote, params.branch_name, params.local_folder, True
    )
    graph = create_rdflib_dataset(os.path.join(full_local_path, params.dataset_folder))
    sameas = execute_sparql(
        graph,
        "sparql/retrieve_sameas.sparql",
    )
    prov_ent = execute_sparql(
        graph,
        "sparql/retrieve_provided_entities.sparql",
    )
    # Create path relative to current script location
    current_dir = os.path.dirname(os.path.abspath(__file__))
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
    commit_and_push(repo)


if __name__ == "__main__":
    enrich_via_sparql(
        Params(
            repo="acdh-ch/pfp/pfp-source-data",
            username_secret="gitlab-source-data-username",
            password_secret="gitlab-source-data-password",
            branch_name="origin/pio_branch_9",
            git_provider="oeaw-gitlab",
        )
    )
