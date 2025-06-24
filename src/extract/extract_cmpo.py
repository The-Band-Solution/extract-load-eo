import json  # noqa: I001
from types import SimpleNamespace  # noqa: I001
from typing import Any  # noqa: I001
from py2neo import Node, Relationship  # noqa: I001
from extract.extract_base import ExtractBase  # noqa: I001


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

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization hook.

        Defines the data streams to fetch and calls the parent initialization method
        to set up Airbyte sources and Neo4j connections.
        """
        self.streams = ["repositories", "projects_v2", "commits", "branches"]
        super().model_post_init(__context)

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
            repository_node = Node("Repository", **data)

            self.sink.save_node(repository_node, "Repository", "id")

            self.sink.save_relationship(
                Relationship(self.organization_node, "has", repository_node)
            )

    def __load_repository_project(self) -> None:
        """Creates relationships between repositories and  projects."""  # noqa: D401
        for project in self.projects.itertuples():
            repository_node = self.sink.get_node(
                "Repository", full_name=project.repository
            )
            project_node = self.sink.get_node("Project", id=project.id)
            if repository_node and project_node:
                self.sink.save_relationship(
                    Relationship(project_node, "has", repository_node)
                )

    def __load_commits(self) -> None:
        """Loads commits into Neo4j as nodes and creates relationships
        to repositories and authors (committers).
        """  # noqa: D205, D401
        for commit in self.commits.itertuples(index=False):
            data = self.transform(commit)
            data["id"] = data["sha"] + "-" + data["repository"]

            node = Node("Commit", **data)
            self.sink.save_node(node, "Commit", "id")

            repository_node = self.sink.get_node(
                "Repository", full_name=commit.repository
            )
            self.sink.save_relationship(Relationship(repository_node, "has", node))

            # Author relationship
            if commit.author:
                user = json.loads(
                    commit.author, object_hook=lambda d: SimpleNamespace(**d)
                )
                user_node = self.sink.get_node("Person", id=user.login)
                if user_node:
                    self.sink.save_relationship(
                        Relationship(node, "created_by", user_node)
                    )

            # Committer relationship
            if commit.committer:
                user = json.loads(
                    commit.committer, object_hook=lambda d: SimpleNamespace(**d)
                )
                user_node = self.sink.get_node("Person", id=user.login)
                if user_node:
                    self.sink.save_relationship(
                        Relationship(node, "created_by", user_node)
                    )

    def __load_branchs(self) -> None:
        """Loads branches into Neo4j as nodes and
        creates relationships
        to their corresponding repositories.
        """  # noqa: D205, D401
        for branch in self.branches.itertuples(index=False):
            data = self.transform(branch)
            data["id"] = data["name"] + "-" + data["repository"]

            node = Node("Branch", **data)
            self.sink.save_node(node, "Branch", "id")

            if branch.repository:
                repository_node = self.sink.get_node(
                    "Repository", full_name=branch.repository
                )
                self.sink.save_relationship(Relationship(repository_node, "has", node))

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

        print("✅ Extraction completed successfully!")
