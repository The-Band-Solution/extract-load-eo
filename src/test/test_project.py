
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

def test_get_repository():
    
    repositories = client.get_repositories()
    print (f"Found {len(repositories)} repositories")
    assert len(repositories) >= 1
'''
def test_get_milestones():
    repositories = client.get_repositories()
    print (f"Found {len(repositories)} repositories")
    milestones_list = []
    for repository in repositories:
        milestones = client.get_milestones(repository.full_name)
        print(f"Found {len(milestones)} milestones in {repository.name}")
        milestones_list.extend(milestones)
        
    print(f"Total milestones found: {len(milestones_list)}")
    assert len(milestones_list) >= 1
'''
'''
def test_get_issues():
    repositories = client.get_repositories()
    print (f"Found {len(repositories)} repositories")
    issues_list = []
    for repository in repositories:
        issues = client.get_issues(repository.full_name)
        print(f"Found {len(issues)} issues in {repository.name}")
        issues_list.extend(issues)
        for issue in issues:
            print(f"Issue #{issue.number}: {issue.title} - State: {issue.state}")
            if issue.assignees:
                print(f"Assignees: {[assignee.login for assignee in issue.assignees]}")
            if issue.author:
                print(f"Author: {issue.author.login}")
            if issue.milestone:
                print(f"Milestone: {issue.milestone.title}")
        
    print(f"Total issues found: {len(issues_list)}")
    assert len(issues_list) >= 1
'''    
def test_project_issue():
    project_issue_map = client.get_project_issue_map() 
    print(f"Found {len(project_issue_map)} projects with issues")
    
    assert len(project_issue_map) >= 1