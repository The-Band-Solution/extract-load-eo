
from source.github_client import GitHubClient
from dotenv import load_dotenv
import os

load_dotenv()
organization = os.getenv("GITHUB_ORG", "")
token = os.getenv("GITHUB_TOKEN", "")
client = GitHubClient(token, organization)
    
def test_get_projects():
    
    projects = client.get_projects()
    assert len(projects) >= 1

def teste_get_repository():
    
    repositories = client.get_repositories()
    assert len(repositories) >= 1