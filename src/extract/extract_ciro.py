from src.extract.extract_base import ExtractBase  # noqa: I001
from typing import Any  # noqa: I001
from py2neo import Node  # noqa: I001
from src.config.logging_config import LoggerFactory  # noqa: I001
import json  # noqa: I001

logger = LoggerFactory.get_logger("extractor")


class ExtractCIRO(ExtractBase):
    """Extract and persist data for the CIRO dataset using Airbyte and Neo4j."""

    milestones: Any = None
    issues: Any = None
    pull_request_commits: Any = None
    pull_requests: Any = None
    issue_labels: Any = None
    projects: Any = None

    def __init__(self) -> None:
        """Initialize the extractor and define streams to load from Airbyte."""
        self.streams = [
            "issue_milestones",
            "issues",
            "pull_request_commits",
            "pull_requests",
            "issue_labels",
        ]
        super().__init__()

    def fetch_data(self) -> None:
        """Fetch data from Airbyte cache and store in memory as pandas DataFrames."""
        self.load_data()

        if "issue_milestones" in self.cache:
            self.milestones = self.cache["issue_milestones"].to_pandas()
            logger.info(f"{len(self.milestones)} issue_milestones loaded.")

        if "issues" in self.cache:
            self.issues = self.cache["issues"].to_pandas()
            logger.info(f"{len(self.issues)} issues loaded.")

        if "pull_request_commits" in self.cache:
            self.pull_request_commits = self.cache["pull_request_commits"].to_pandas()
            logger.info(
                f"{len(self.pull_request_commits)} pull_request_commits loaded."
            )

        if "pull_requests" in self.cache:
            self.pull_requests = self.cache["pull_requests"].to_pandas()
            logger.info(f"{len(self.pull_requests)} pull_requests loaded.")

        if "issue_labels" in self.cache:
            self.issue_labels = self.cache["issue_labels"].to_pandas()
            logger.info(f"{len(self.issue_labels)} issue_labels loaded.")

    def __load_milestones(self) -> None:
        """Create Milestone nodes and link them to their respective repositories."""
        for milestone in self.milestones.itertuples(index=False):
            data = self.transform(milestone)
            milestone_node = self.create_node(data, "Milestone", "id")
            repository_node = self.get_node(
                "Repository", full_name=milestone.repository
            )
            self.create_relationship(repository_node, "has", milestone_node)
            logger.info(
                f"Link Repository to Milestone: {milestone.repository}{milestone.title}"
            )

    def __load_issue(self) -> None:
        """Create Issue nodes and link."""
        for issue in self.issues.itertuples(index=False):
            data = self.transform(issue)
            node = self._create_issue_node(data, issue)
            self._link_issue_to_repository(node, issue)
            self._link_issue_to_milestone(node, issue)
            self._link_issue_to_users(node, issue)
            self._link_issue_to_labels(node, issue)

    def _create_issue_node(self, data: dict[str, Any], issue: Any) -> Node:
        """Create the Issue node in Neo4j."""
        node = self.create_node(data, "Issue", "id")
        logger.info(f"Creating Issue: {issue.title}")
        return node

    def _link_issue_to_repository(self, node: Node, issue: Any) -> None:
        """Link the Issue node to its repository."""
        repository_node = self.get_node("Repository", full_name=issue.repository)
        if repository_node:
            self.create_relationship(repository_node, "has", node)
            logger.info(
                f"Linking Repository to Issue: {issue.title}-{issue.repository}"
            )

    def _link_issue_to_milestone(self, node: Node, issue: Any) -> None:
        """Link the Issue to its Milestone, if any."""
        if issue.milestone:
            milestone = self.transform_object(issue.milestone)
            milestone_node = self.get_node("Milestone", id=milestone.id)
            if milestone_node:
                self.create_relationship(milestone_node, "has", node)
                logger.info(f"Linking Milestone to Issue: {issue.title}-{milestone.id}")

    def _link_issue_to_users(self, node: Node, issue: Any) -> None:
        """Link the Issue to its creator and assignees."""
        if issue.user:
            self._create_user_relationship(node, issue.user, "created_by", issue.title)

        if issue.assignee:
            self._create_user_relationship(
                node, issue.assignee, "assigned_to", issue.title
            )

        if issue.assignees:
            assignees = json.loads(issue.assignees)
            for assignee in assignees:
                self._create_user_relationship(
                    node, assignee, "assigned_to", issue.title
                )

    def _create_user_relationship(
        self, node: Node, user_data: Any, rel_type: str, issue_title: str
    ) -> None:
        """Create a relationship between the Issue and a user (creator or assignee)."""
        user = (
            user_data
            if isinstance(user_data, dict)
            else self.transform_object(user_data)
        )
        login = user.get("login") if isinstance(user, dict) else user.login
        user_node = self.get_node("Person", id=login)
        if user_node:
            self.create_relationship(node, rel_type, user_node)
            logger.info(
                f"Linking {rel_type} between Issue and User: {login}-{issue_title}"
            )

    def _link_issue_to_labels(self, node: Node, issue: Any) -> None:
        """Link the Issue to its associated Labels."""
        if issue.labels:
            labels = json.loads(issue.labels)
            for label in labels:
                label_node = self.get_node("Label", id=label["id"])
                if label_node:
                    self.create_relationship(node, "labeled", label_node)

    def __load_labels(self) -> None:
        """Create Label nodes and link them to their respective repositories."""
        for label in self.issue_labels.itertuples(index=False):
            data = self.transform(label)
            node = self.create_node(data, "Label", "id")
            logger.info(
                f"Creating Label {label.name} for Repository {label.repository}"
            )
            repository_node = self.get_node("Repository", full_name=label.repository)
            if repository_node:
                self.create_relationship(repository_node, "has", node)

    def __load_pull_request_commit(self) -> None:
        """Link commits to their respective Pull Requests."""
        for pr_commit in self.pull_request_commits.itertuples(index=False):
            data = self.transform(pr_commit)
            commit_node = self.get_node("Commit", sha=data["sha"])
            pr_node = self.get_node(
                "PullRequest", repository=data["repository"], number=data["pull_number"]
            )
            if commit_node and pr_node:
                self.create_relationship(commit_node, "committed", pr_node)
                self.create_relationship(pr_node, "has", commit_node)
                logger.info("Created link between commit and pull_request")
            else:
                logger.warning("Commit or pull_request not found")

    def __load_pull_requests(self) -> None:
        """Create Pull Request nodes and link."""
        for pr in self.pull_requests.itertuples(index=False):
            data = self.transform(pr)
            node = self.create_node(data, "PullRequest", "id")

            repository_node = self.get_node("Repository", full_name=pr.repository)
            self.create_relationship(repository_node, "has", node)

            if pr.labels:
                labels = json.loads(pr.labels)
                for label in labels:
                    label_node = self.get_node("Label", id=label["id"])
                    if label_node:
                        self.create_relationship(node, "labeled", label_node)

            if pr.milestone:
                milestone = json.loads(pr.milestone)
                milestone_node = self.get_node("Milestone", id=milestone["id"])
                if milestone_node:
                    self.create_relationship(node, "has", milestone_node)

            if pr.merge_commit_sha:
                commit_node = self.get_node("Commit", sha=pr.merge_commit_sha)
                if commit_node:
                    self.create_relationship(node, "merged", commit_node)

            logger.info(f"Creating links between users and pull_request {pr.title}")
            self._link_issue_to_users(node, pr)

    def run(self) -> None:
        """Run the full extraction and persistence process."""
        logger.info("🔄 Extracting CIRO data ...")
        self.fetch_data()
        self.__load_labels()
        self.__load_milestones()
        self.__load_pull_requests()
        self.__load_pull_request_commit()
        self.__load_issue()
        logger.info("✅ Extraction completed successfully!")
