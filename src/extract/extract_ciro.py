from src.extract.extract_base import ExtractBase  # noqa: I001
from typing import Any  # noqa: I001
from py2neo import Node, Relationship  # noqa: I001
import json  # noqa: I001


class ExtractCIRO(ExtractBase):
    """Class responsible for extracting data fand save on  CIRO dataset.

    It loads milestones, issues, pull requests, pull request commits,
    and labels from the Airbyte source and persists them into a Neo4j graph database.
    """

    # Dataframes loaded from the cache
    milestones: Any = None
    issues: Any = None
    pull_request_commits: Any = None
    pull_requests: Any = None
    issue_labels: Any = None
    projects: Any = None

    def __init__(self) -> None:
        """Post-initialization hook.

        Defines the list of streams to fetch and calls the parent
        initialization to set up Airbyte and Neo4j connections.
        """
        self.streams = [
            "issue_milestones",
            "issues",
            "pull_request_commits",
            "pull_requests",
            "issue_labels",
        ]
        super().__init__()

    def fetch_data(self) -> None:
        """Loads the data from Airbyte cache into pandas DataFrames."""  # noqa: D401
        self.load_data()

        if "issue_milestones" in self.cache:
            self.milestones = self.cache["issue_milestones"].to_pandas()
            print(f"✅ {len(self.milestones)} issue_milestones loaded.")

        if "issues" in self.cache:
            self.issues = self.cache["issues"].to_pandas()
            print(f"✅ {len(self.issues)} issues loaded.")

        if "pull_request_commits" in self.cache:
            self.pull_request_commits = self.cache["pull_request_commits"].to_pandas()
            print(f"✅ {len(self.pull_request_commits)} pull_request_commits loaded.")

        if "pull_requests" in self.cache:
            self.pull_requests = self.cache["pull_requests"].to_pandas()
            print(f"✅ {len(self.pull_requests)} pull_requests loaded.")

        if "issue_labels" in self.cache:
            self.issue_labels = self.cache["issue_labels"].to_pandas()
            print(f"✅ {len(self.issue_labels)} issue_labels loaded.")

    def __load_milestones(self) -> None:
        """Loads milestones into the Neo4j graph as nodes and creates relationships
        to their respective repositories.
        """  # noqa: D205, D401
        for milestone in self.milestones.itertuples(index=False):
            data = self.transform(milestone)

            milestone_node = Node("Milestone", **data)

            repository_node = self.sink.get_node(
                "Repository", full_name=milestone.repository
            )
            self.create_node(milestone_node, "Milestone", "id")

            self.sink.save_relationship(
                Relationship(repository_node, "has", milestone_node)
            )

    def __load_issue(self) -> None:
        """Loads issues into the Neo4j graph and creates all relevant
        nodes and relationships.
        """  # noqa: D205, D401
        for issue in self.issues.itertuples(index=False):
            data = self.transform(issue)

            node = self._create_issue_node(data, issue)
            self._link_issue_to_repository(node, issue)
            self._link_issue_to_milestone(node, issue)
            self._link_issue_to_users(node, issue)
            self._link_issue_to_labels(node, issue)

    def _create_issue_node(self, data: dict[str, Any], issue: Any) -> Node:
        node = Node("Issue", **data)
        self.create_node(node, "Issue", "id")
        print(f"🔄 Creating Issue: {issue.title}")

        return node

    def _link_issue_to_repository(self, node: Node, issue: Any) -> None:
        repository_node = self.sink.get_node("Repository", full_name=issue.repository)
        if repository_node:
            self.sink.save_relationship(Relationship(repository_node, "has", node))
            print(f"🔄 Linking Repository to Issue: {issue.title}-{issue.repository}")

    def _link_issue_to_milestone(self, node: Node, issue: Any) -> None:
        if issue.milestone:
            milestone = self.transform_object(issue.milestone)

            ## Errado precisa buscar no base
            milestone_node = self.sink.get_node("Milestone", id=milestone.id)

            if milestone_node:
                self.sink.save_relationship(Relationship(milestone_node, "has", node))
                print(f"🔄 Linking Milestone to Issue: {issue.title}-{milestone.id}")

    def _link_issue_to_users(self, node: Node, issue: Any) -> None:
        # Creator
        if issue.user:
            self._create_user_relationship(node, issue.user, "created_by", issue.title)

        # Single assignee
        if issue.assignee:
            self._create_user_relationship(
                node, issue.assignee, "assigned_to", issue.title
            )

        # Multiple assignees
        if issue.assignees:
            assignees = json.loads(issue.assignees)
            for assignee in assignees:
                self._create_user_relationship(
                    node, assignee, "assigned_to", issue.title
                )

    def _create_user_relationship(
        self, node: Node, user_data: Any, rel_type: str, issue_title: str
    ) -> None:
        user = (
            user_data
            if isinstance(user_data, dict)
            else self.transform_object(user_data)
        )
        login = user.login if hasattr(user, "login") else user["login"]

        user_node = self.sink.get_node("Person", id=login)

        if user_node:
            self.sink.save_relationship(Relationship(node, rel_type, user_node))
            print(
                f"🔄 Linking {rel_type} between Issue and User: {login}-{issue_title}"
            )

    def _link_issue_to_labels(self, node: Node, issue: Any) -> None:
        if issue.labels:
            labels = json.loads(issue.labels)
            for label in labels:
                label_node = self.sink.get_node("Label", id=label["id"])
                if label_node:
                    self.sink.save_relationship(
                        Relationship(node, "labeled", label_node)
                    )

    def __load_labels(self) -> None:
        """Loads labels and creates relationships with repositories."""  # noqa: D401
        # noqa: D401

        for label in self.issue_labels.itertuples(index=False):
            data = self.transform(label)
            node = Node("Label", **data)
            self.create_node(node, "Label", "id")

            repository_node = self.sink.get_node(
                "Repository", full_name=label.repository
            )

            if repository_node is not None:
                self.sink.save_relationship(Relationship(repository_node, "has", node))

    def __load_pull_request_commit(self) -> None:
        """Loads pull request commit data.

        Currently this method only prints commit information. Can be extended
        to create nodes and relationships for commits in the graph.
        """  # noqa: D401
        for pull_request_commit in self.pull_request_commits.itertuples(index=False):
            print(pull_request_commit)

    def __load_pull_requests(self) -> None:
        """Loads pull requests into the Neo4j graph, creates pull request nodes,
        and builds relationships with repositories and (in the future) labels,
        merge commits, users, and assignees.
        """  # noqa: D205, D401
        for pull_request in self.pull_requests.itertuples(index=False):
            data = self.transform(pull_request)
            node = Node("PullRequest", **data)
            self.create_node(node, "PullRequest", "id")

            repository_node = self.sink.get_node(
                "Repository", full_name=pull_request.repository
            )

            self.sink.save_relationship(Relationship(repository_node, "has", node))

            if pull_request.labels:
                labels = json.loads(pull_request.labels)
                print(labels)
            if pull_request.milestone:
                pass
            if pull_request.merge_commit_sha:
                pass
            if pull_request.assignee:
                pass
            if pull_request.assignees:
                pass
            if pull_request.user:
                pass

    def run(self) -> None:
        """Orchestrates the entire extraction process.

        Loads data, processes labels, milestones, issues, pull requests, and commits,
        and saves them into the Neo4j graph.
        """
        print("🔄 Extracting CIRO data ...")
        self.fetch_data()
        self.__load_labels()
        self.__load_milestones()
        self.__load_issue()
        self.__load_pull_requests()
        self.__load_pull_request_commit()
        print("✅ Extraction completed successfully!")
