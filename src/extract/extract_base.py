from pydantic import BaseModel
from typing import List, Any
import os
from dotenv import load_dotenv

from source.github_client import GitHubClient

class ExtractBase(BaseModel):

    organization: str = ""
    token: str = ""
    cache: Any = None

    def model_post_init(self, __context):
        load_dotenv()
        self.organization = os.getenv("GITHUB_ORG", "")
        self.token = os.getenv("GITHUB_TOKEN", "")
        self.fetch_data()

    def fetch_data(self):
        client = GitHubClient(self.token, self.organization)
        self.cache = {
            "team_members": client.get_teams_with_members(),
            "teams": client.get_teams()
        }
