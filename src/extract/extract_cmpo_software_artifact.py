from typing import Any  # noqa: I001
from src.extract.extract_base import ExtractBase  # noqa: I001
from github import Github, Repository, Commit as GitCommit  # noqa: I001
import os  # noqa: I001
from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: I001
from py2neo import Graph  # noqa: I001


class ExtractCMPOSoftwareArtifact(ExtractBase):
    """Extractor for CMPO Software Artifact."""

    def __init__(self) -> None:
        """Initialize the Neo4j connection and GitHub client."""
        super().__init__()
        self.graph = Graph(
            "bolt://neo4j:7687",
            auth=(os.getenv("NEO4J_USERNAME", ""), os.getenv("NEO4J_PASSWORD", "")),
        )
        self.token = os.getenv("GITHUB_TOKEN", "")
        self.github = Github(self.token)
        self.commits = self._load_commits()

    def fetch_data(self) -> None:
        """Retrive data."""
        pass

    def _load_commits(
        self,
    ) -> Any:  # TODO: pegar sempre a data corrente ou da ultima importacao
        """Load all commits with associated repository names from Neo4j."""
        query = """
        MATCH (c:Commit)
        RETURN c.sha AS sha, c.repository AS repository
        """
        return self.graph.run(query).data()

    def process_commit(self, sha: str, repository: str) -> None:
        """Fetch files of a given commit from GitHub and create artifacts in Neo4j."""
        try:
            repo: Repository.Repository = self.github.get_repo(repository)
            commit_git: GitCommit.Commit = repo.get_commit(sha)
            commit_id = f"{sha}-{repository}"

            commit_node = self.get_node("Commit", id=commit_id)
            if not commit_node:
                print(f"❌ Commit not found in Neo4j: {commit_id}")
                return

            for file in commit_git.files:
                if file.sha:
                    data = {
                        "id": file.sha,
                        "filename": file.filename,
                        "status": file.status,
                        "additions": file.additions,
                        "deletions": file.deletions,
                        "changes": file.changes,
                        "patch": getattr(file, "patch", None),
                        "raw_url": file.raw_url,
                        "blob_url": file.blob_url,
                        "sha": file.sha,
                    }

                    file_node = self.create_node(data, "SoftwareArtifact", "id")
                    self.create_relationship(commit_node, "has", file_node)
                    self.create_relationship(file_node, "commited", commit_node)

                    print(f"✅ Processed: file {file.sha} | {sha}")

            print(f"✅ Processed: Commit {sha} | {repository}")

        except Exception as e:
            print(f"⚠️ Error processing {sha} | {repository}: {e}")

    def process_all(self, max_workers: int = 8) -> None:
        """Run all commit processing jobs in parallel using threads."""
        print(f"🚀 Processing {len(self.commits)} commits with {max_workers} threads")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.process_commit, row["sha"], row["repository"])
                for row in self.commits
            ]
            for _ in as_completed(futures):
                pass  # Log handled inside each thread

    def run(self) -> None:
        """Orchestrates the full data extraction process for CMPO.

        Loads repositories, projects, branches, and commits, and
        creates the corresponding nodes and relationships in the Neo4j graph.
        """
        print("🔄 Extracting software artifacts using CMPO...")

        self.process_all()
        print("✅ Extraction completed successfully!")
