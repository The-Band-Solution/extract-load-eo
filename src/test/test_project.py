
from source.github_client import GitHubClient
from dotenv import load_dotenv
import os

def test_get_projects():
    
    load_dotenv()
    organization = os.getenv("GITHUB_ORG", "")
    token = os.getenv("GITHUB_TOKEN", "")
    client = GitHubClient(token, organization)
    projects = client.get_projects()
    assert len(projects) >= 1