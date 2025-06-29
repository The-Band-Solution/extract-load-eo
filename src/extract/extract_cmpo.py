from typing import Any  # noqa: I001
from src.extract.extract_base import ExtractBase  # noqa: I001
import json  # noqa: I001


class ExtractCMPO(ExtractBase):
    """Extractor for CMPO data.

    This class loads repositories, projects, commits, and branches from Airbyte,
    transforms them, and persists them as nodes and relationships in a Neo4j
    graph database.
    """

    # DataFrames extracted from Airbyte
    branches: Any = None
    issues: Any = None
    commits: Any = None
    repositories: Any = None
    projects: Any = None

    def __init__(self) -> None:
        """Post-initialization hook.

        Defines the data streams to fetch and calls the parent initialization method
        to set up Airbyte sources and Neo4j connections.
        """
        self.streams = ["repositories", "projects_v2", "commits", "branches"]
        super().__init__()

    def fetch_data(self) -> None:
        """Loads the data streams from Airbyte into pandas DataFrames."""  # noqa: D401
        self.load_data()

        if "repositories" in self.cache:
            self.repositories = self.cache["repositories"].to_pandas()
            print(f"✅ {len(self.repositories)} repositories loaded.")

        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            print(f"✅ {len(self.projects)} projects loaded.")

        if "commits" in self.cache:
            self.commits = self.cache["commits"].to_pandas()
            print(f"✅ {len(self.commits)} commits loaded.")

        if "branches" in self.cache:
            self.branches = self.cache["branches"].to_pandas()
            print(f"✅ {len(self.branches)} branches loaded.")

    def __load_repository(self) -> None:
        """Loads repositories into Neo4j as nodes and creates relationships
        between the organization and its repositories.
        """  # noqa: D205, D401
        for repository in self.repositories.itertuples():
            data = self.transform(repository)
            repository_node = self.create_node(data, "Repository", "id")

            self.create_relationship(self.organization_node, "has", repository_node)

    def __load_repository_project(self) -> None:
        """Creates relationships between repositories and  projects."""  # noqa: D401
        for project in self.projects.itertuples():
            repository_node = self.get_node("Repository", full_name=project.repository)

            project_node = self.get_node("Project", id=project.id)

            if repository_node and project_node:
                self.create_relationship(project_node, "has", repository_node)

    def flatten_dict(self, d: Any, prefix: Any) -> Any:
        """Transforma um dict aninhado em um dict plano com prefixos.

        Args:
        ----
            d (Any): data
            prefix (Any):  prefix.

        Returns:
        -------
            dict: a dictionary.

        """  # noqa: D401
        flat = {}
        for k, v in d.items():
            if isinstance(v, dict):
                flat.update(self.flatten_dict(v, prefix + k + "_"))
            else:
                flat[prefix + k] = v
        return flat

    def __load_commits(self) -> None:
        """Loads commits into Neo4j as nodes and creates relationships
        to repositories and authors (committers).
        """  # noqa: D205, D401
        for commit in self.commits.itertuples(index=False):
            data = self.transform(commit)
            data["id"] = data["sha"] + "-" + data["repository"]

            commit_data = json.loads(commit.commit)

            node_data = {**data, **self.flatten_dict(commit_data, "")}
            node = self.create_node(node_data, "Commit", "id")

            repository_node = self.get_node("Repository", full_name=commit.repository)

            self.create_relationship(repository_node, "has", node)
            self.create_relationship(node, "belongs_to", repository_node)

            # Author relationship
            if commit.author:
                user = self.transform_object(commit.author)

                user_node = self.get_node("Person", id=user.login)
                if user_node:
                    self.create_relationship(node, "created_by", user_node)

            # Committer relationship
            if commit.committer:
                user = self.transform_object(commit.committer)

                user_node = self.get_node("Person", id=user.login)
                if user_node:
                    self.create_relationship(node, "created_by", user_node)

            branch_node = self.get_node(
                "Branch", id=commit.branch + "-" + commit.repository
            )
            self.create_relationship(branch_node, "has", node)
            self.create_relationship(node, "in", branch_node)

    def __create_relation_commits(self) -> None:
        """Create relationships between commits."""
        for commit in self.commits.itertuples(index=False):
            parents = json.loads(commit.parents)
            for parent in parents:
                commit_node = self.get_node(
                    "Commit", id=commit.sha + "-" + commit.repository
                )
                parent_node = self.get_node(
                    "Commit", id=parent["sha"] + "-" + commit.repository
                )
                self.create_relationship(parent_node, "is_parent", commit_node)
                self.create_relationship(commit_node, "has_parent", parent_node)

    def __load_branchs(self) -> None:
        """Loads branches into Neo4j as nodes and
        creates relationships
        to their corresponding repositories.
        """  # noqa: D205, D401
        for branch in self.branches.itertuples(index=False):
            data = self.transform(branch)

            data["id"] = data["name"] + "-" + data["repository"]

            node = self.create_node(data, "Branch", "id")

            if branch.repository:
                repository_node = self.get_node(
                    "Repository", full_name=branch.repository
                )
                if repository_node is not None:
                    self.create_relationship(repository_node, "has", node)

    def run(self) -> None:
        """Orchestrates the full data extraction process for CMPO.

        Loads repositories, projects, branches, and commits, and
        creates the corresponding nodes and relationships in the Neo4j graph.
        """
        print("🔄 Extracting data using CMPO...")

        self.fetch_data()
        self.__load_repository()
        self.__load_repository_project()
        self.__load_branchs()
        self.__load_commits()
        self.__create_relation_commits()

        print("✅ Extraction completed successfully!")
