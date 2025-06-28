import threading  # noqa: I001
import queue  # noqa: I001
import requests  # noqa: I001
import time  # noqa: I001
from typing import Any, cast # noqa: I001

class GitHubClient:
    """Extract data from Github."""  # noqa: D205

    def __init__(self, token: str, max_threads: int = 5):
        """Config on github.

        Args:
        ----
            token (str): GitHub personal access token.
            max_threads (int): Number of threads for parallel processing.

        """
        self.token = token
        self.max_threads = max_threads
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.commit_queue: queue.Queue[dict[str, str]] = queue.Queue()
        self.results: list[dict[str, Any]] = []

    
    def fetch_commit_data(self, repo: str, sha: str) -> dict[str, Any]:
        """Retrieve data for a specific commit.

        Args:
        ----
            repo (str): Repository name.
            sha (str): Commit SHA.

        Returns:
        -------
            dict: Commit details including modified files.

        """
        url = f"https://api.github.com/repos/{repo}/commits/{sha}"
        response = requests.get(url, headers=self.headers, timeout=10)
        response.raise_for_status()
        return cast(dict[str, Any], response.json())

    def worker(self) -> None:
        """Work the job."""  # noqa: D401
        while True:
            try:
                commit_info = self.commit_queue.get(timeout=3)
            except queue.Empty:
                return

            try:
                repo = commit_info["repo"]
                sha = commit_info["sha"]
                commit_data = self.fetch_commit_data(repo, sha)

                author = commit_data.get("author") or {}
                commit_author = commit_data["commit"]["author"]

                for f in commit_data.get("files", []):
                    self.results.append(
                        {
                            "repo": repo,
                            "sha": sha,
                            "filename": f["filename"],
                            "status": f["status"],
                            "author_login": author.get("login"),
                            "author_name": commit_author.get("name"),
                            "author_email": commit_author.get("email"),
                            "date": commit_author.get("date"),
                        }
                    )

                print(f"✅ Processed {repo}@{sha}")

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    # Get the reset time from GitHub headers
                    reset_timestamp = int(
                        e.response.headers.get("X-RateLimit-Reset", time.time() + 60)
                    )
                    now = time.time()
                    wait_seconds = max(reset_timestamp - now, 0)
                    print(f"⏳ Rate limit hit. Waiting {int(wait_seconds)}s")
                    time.sleep(wait_seconds + 5)  # add buffer
                    # Put the same commit back to the queue to retry
                    self.commit_queue.put(commit_info)
                else:
                    print(f"❌ Error in {commit_info}: {e}")
            except Exception as e:
                print(f"❌ Error in {commit_info}: {e}")
            finally:
                self.commit_queue.task_done()
                time.sleep(0.3)

    def run(self, commits: list[dict[str, str]]) -> list[dict[str, Any]]:
        """Fetcher on a list of commits.

        Args:
        ----
            commits (list[dict]): list of dicts with 'owner', 'repo', and 'sha'.

        Returns:
        -------
            list[dict]: Processed commit file information.

        """
        for commit in commits:
            self.commit_queue.put(commit)

        threads = []
        for _ in range(self.max_threads):
            t = threading.Thread(target=self.worker)
            t.start()
            threads.append(t)

        self.commit_queue.join()

        for t in threads:
            t.join()

        return self.results

    def print_results(self) -> None:
        """Prints all collected results."""  # noqa: D401
        print(len(self.results))
        for r in self.results:
            print(f"[{r['repo']}]: {r['filename']} - {r['status']}")
