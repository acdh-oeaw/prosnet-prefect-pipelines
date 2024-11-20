import datetime
import os
import shutil
import requests
import git
from pydantic import BaseModel, DirectoryPath, Field, FilePath, HttpUrl
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret


@task(tags=["github"])
def create_pr_github(title, branch, token, repo, base):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
    }
    url = f"https://api.github.com/repos/{repo}/pulls"
    form = {"title": title, "head": branch, "base": base}
    res = requests.post(url, headers=headers, json=form)
    return res


@task(tags=["gitlab"])
def create_mr_gitlab(title, branch, token, repo, base):
    headers = {
        "PRIVATE-TOKEN": token,
    }
    url = f"https://gitlab.oeaw.ac.at/api/v4/projects/{repo}/merge_requests"
    form = {"title": title, "source_branch": branch, "target_branch": base}
    res = requests.post(url, headers=headers, json=form)
    return res


@task(tags=["git"])
def push_data_to_repo(
    file_path,
    branch,
    repo,
    username,
    password,
    commit_message,
    file_path_git,
    local_folder,
    force,
    remote,
):
    """creates a feature branch adds the created graph file and pushes the branch

    Args:
        file_path (path): path of the file to push
        branch (str, optional): Name of the feature branch to use, suto-generated if None. Defaults to None.
    """

    logger = get_run_logger()
    full_local_path = os.path.join(os.getcwd(), local_folder)
    if os.path.exists(full_local_path) and force:
        shutil.rmtree(full_local_path)
    logger.info(f"Cloning {remote} to {full_local_path}")
    repo = git.Repo.clone_from(remote, full_local_path)
    repo.git.checkout("-b", branch)
    os.makedirs(
        os.path.dirname(os.path.join(full_local_path, *file_path_git.split("/"))),
        exist_ok=True,
    )
    shutil.copyfile(file_path, os.path.join(full_local_path, *file_path_git.split("/")))
    repo.git.add(file_path_git)
    repo.index.commit(commit_message)
    origin = repo.remote(name="origin")
    origin.push(refspec=f"{branch}:{branch}")
    return True


class Params(BaseModel):
    repo: str = Field(
        "intavia/source-data",
        description="GitHub Repository in the format 'OWNER/{GROUP}/REPO'",
    )
    branch_name: str = Field(
        ...,
        description="Branch name to use, auto-generated if not set",
    )
    branch_name_add_date: bool = Field(
        False, description="Whether to add the current date to the branch name"
    )
    username_secret: str = Field(
        "github-username",
        description="Name of the prefect secret that contains the username",
    )
    password_secret: str = Field(
        "github-password",
        description="Name of the prefect secret that contains the password. Use tokens instead of your personal password.",
    )
    file_path: FilePath = Field(..., description="Path of the file to ingest")
    file_path_git: str = Field(
        "datasets/apis_data.ttl",
        description="Path of the file to use within the Git repo",
    )
    named_graphs_used: list[HttpUrl] = Field(
        None,
        description="Named graphs used in the generation of the data",
    )
    commit_message: str = Field(
        "Updates data to latest",
        alias="Commit Message",
        description="Message used for the commit.",
    )
    auto_pr: bool = Field(True, description="Wheter to automatically add a PR")
    base_branch_pr: str = Field(
        "main",
        description="Base branch for creating the PR against",
    )
    local_folder: str = Field(
        "source-data", description="Local folder to clone the repo to"
    )
    force: bool = Field(
        True,
        description="Whether to force the deletion of the local folder if it exists",
    )
    git_provider: Literal["oeaw-gitlab", "github"] = Field(
        "oeaw-gitlab",
        decription="The git-provider to use. Options are OEAW-GitLab and GitHub.",
    )


@flow()
def push_data_to_repo_flow(params: Params):
    username = Secret.load(params.username_secret).get()
    password = Secret.load(params.password_secret).get()
    if params.git_provider == "github":
        remote = f"https://{username}:{password}@github.com/{repo}.git"
    elif params.git_provider == "oeaw-gitlab":
        remote = f"https://{username}:{password}@gitlab.oeaw.ac.at/{repo}.git"
    commit_message = f"feat: {params.commit_message}"
    if params.branch_name_add_date:
        branch_name = (
            f"{params.branch_name}-{datetime.datetime.now().strftime('%d-%m-%Y')}"
        )
    else:
        branch_name = params.branch_name
    res = push_data_to_repo(
        params.file_path,
        branch_name,
        params.repo,
        username,
        password,
        commit_message,
        params.file_path_git,
        params.local_folder,
        params.force,
    )
    if res and params.auto_pr params.git_provider == "github":
        create_pr_github(
            f"add new data from {branch_name}",
            branch_name,
            password,
            params.repo,
            params.base_branch_pr,
        )
    elif res and params.auto_pr params.git_provider == "oeaw-gitlab":
        create_mr_gitlab(
            f"add new data from {branch_name}",
            branch_name,
            password,
            params.repo,
            params.base_branch_pr,
        )

if __name__ == "__main__":
    push_data_to_repo_flow(Params())
