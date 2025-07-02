from typing import Any  # noqa: I001
from src.extract.extract_base import ExtractBase  # noqa: I001
from src.config.logging_config import LoggerFactory  # noqa: I001
import json  # noqa: I001


class ExtractCMPO(ExtractBase):
    """Extracts CMPO data and stores it in Neo4j."""

    branches: Any = None
    issues: Any = None
    commits: Any = None
    repositories: Any = None
    projects: Any = None

    def __init__(self) -> None:
        """Initialize the extractor and define streams to load from Airbyte."""
        self.logger = LoggerFactory.get_logger(__name__)
        self.streams = ["repositories", "projects_v2", "commits", "branches"]
        super().__init__()
        self.logger.debug("CMPO extractor initialized with streams: %s", self.streams)

    def fetch_data(self) -> None:
        """Fetch data."""
        self.logger.info("Fetching CMPO data streams...")
        self.load_data()

        if "repositories" in self.cache:
            self.repositories = self.cache["repositories"].to_pandas()
            self.logger.info(f"{len(self.repositories)} repositories loaded.")

        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            self.logger.info(f"{len(self.projects)} projects loaded.")

        if "commits" in self.cache:
            self.commits = self.cache["commits"].to_pandas()
            self.logger.info(f"{len(self.commits)} commits loaded.")

        if "branches" in self.cache:
            self.branches = self.cache["branches"].to_pandas()
            self.logger.info(f"{len(self.branches)} branches loaded.")

    def __load_repository(self) -> None:
        """Load repositories."""
        self.logger.info("Loading repositories...")
        for repository in self.repositories.itertuples():
            data = self.transform(repository)
            self.logger.debug("Repository transformed: %s", data)
            node = self.create_node(data, "Repository", "id")
            self.create_relationship(self.organization_node, "has", node)
            self.logger.info(f"Repository node created and linked: {data['id']}")

    def __load_repository_project(self) -> None:
        """Link repositories to projects."""
        self.logger.info("Linking repositories to projects...")
        for project in self.projects.itertuples():
            self.logger.debug("Processing project: %s", project.id)
            repository_node = self.get_node("Repository", full_name=project.repository)
            project_node = self.get_node("Project", id=project.id)

            if repository_node and project_node:
                self.create_relationship(project_node, "has", repository_node)
                self.logger.info(
                    "Linked Project: %s - %s",
                    project.id,
                    project.repository,
                )
            else:
                self.logger.info(
                    "Missing node for Project %s or Repository %s",
                    project.id,
                    project.repository,
                )

    def flatten_dict(self, d: Any, prefix: Any) -> Any:
        """Flatten a nested dictionary, prefixing keys with their parent path."""
        flat = {}
        for k, v in d.items():
            if isinstance(v, dict):
                flat.update(self.flatten_dict(v, prefix + k + "_"))
            else:
                flat[prefix + k] = v
        return flat

    def __load_commits(self) -> None:
        """Load commits."""
        self.logger.info("Loading commits...")
        for commit in self.commits.itertuples(index=False):
            data = self.transform(commit)
            data["id"] = data["sha"]
            self.logger.debug("Commit transformed: %s", data["id"])

            try:
                commit_data = json.loads(commit.commit)
            except Exception as e:
                self.logger.warning(f"Invalid commit JSON for {commit.sha}: {e}")
                continue

            node_data = {**data, **self.flatten_dict(commit_data, "")}
            node = self.create_node(node_data, "Commit", "id")

            repository_node = self.get_node("Repository", full_name=commit.repository)
            if repository_node:
                self.create_relationship(repository_node, "has", node)
                self.create_relationship(node, "belongs_to", repository_node)
            else:
                self.logger.warning(
                    "Repository not found for commit: %s", 
                    commit.repository
                )

            # Author
            if commit.author:
                user = self.transform_object(commit.author)
                user_node = self.get_node("Person", id=user.login)
                if user_node:
                    self.create_relationship(node, "created_by", user_node)
                    self.logger.debug(
                        f"Linked author {user.login} to commit {commit.sha}"
                    )
                else:
                    self.logger.warning(f"Author not found: {user.login}")

            # Committer
            if commit.committer:
                user = self.transform_object(commit.committer)
                user_node = self.get_node("Person", id=user.login)
                if user_node:
                    self.create_relationship(node, "created_by", user_node)
                    self.logger.debug(
                        f"Linked committer {user.login} to commit {commit.sha}"
                    )
                else:
                    self.logger.warning(f"Committer not found: {user.login}")

            # Branch
            branch_id = commit.branch + "-" + commit.repository
            branch_node = self.get_node("Branch", id=branch_id)
            if branch_node:
                self.create_relationship(branch_node, "has", node)
                self.create_relationship(node, "in", branch_node)
                self.logger.debug(f"Linked commit {commit.sha} to branch {branch_id}")
            else:
                self.logger.warning(f"Branch not found: {branch_id}")

    def __create_relation_commits(self) -> None:
        """Create parent relationships between commits."""
        self.logger.info("Creating parent relationships between commits...")
        for commit in self.commits.itertuples(index=False):
            try:
                parents = json.loads(commit.parents)
            except Exception as e:
                self.logger.warning(f"Invalid parent JSON for commit {commit.sha}: {e}")
                continue

            for parent in parents:
                commit_node = self.get_node("Commit", id=commit.sha)
                parent_node = self.get_node("Commit", id=parent["sha"])
                if commit_node and parent_node:
                    self.create_relationship(parent_node, "is_parent", commit_node)
                    self.create_relationship(commit_node, "has_parent", parent_node)
                    self.logger.debug(f"Linked {parent['sha']} -> {commit.sha}")
                else:
                    self.logger.info(
                        "Missing node for parent-child relation: %s ->  %s",
                        parent["sha"],
                        commit.sha,
                    )

    def __load_branchs(self) -> None:
        """Load branches."""
        self.logger.info("Loading branches...")
        for branch in self.branches.itertuples(index=False):
            data = self.transform(branch)
            data["id"] = data["name"] + "-" + data["repository"]
            self.logger.debug("Branch transformed: %s", data["id"])

            node = self.create_node(data, "Branch", "id")

            if branch.repository:
                repository_node = self.get_node(
                    "Repository", full_name=branch.repository
                )
                if repository_node:
                    self.create_relationship(repository_node, "has", node)
                    self.logger.info(
                        f"Linked branch {data['id']} to repository {branch.repository}"
                    )
                else:
                    self.logger.warning(
                        f"Repository not found for branch: {branch.repository}"
                    )

    def run(self) -> None:
        """Run the full extraction and persistence process."""
        self.logger.info("🔄 Starting CMPO extraction...")
        self.fetch_data()
        self.__load_repository()
        self.__load_repository_project()
        self.__load_branchs()
        self.__load_commits()
        self.__create_relation_commits()
        self.logger.info("✅ CMPO extraction completed.")
