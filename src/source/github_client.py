from github import Github,Auth  
from typing import List
from model.models import Team,  Member, TeamMembership,Project,Milestone, Issue, Repository
import requests
from datetime import datetime

class GitHubClient:
    def __init__(self, token: str, org_name: str):
        self.token = token
        self.github = Github(auth=Auth.Token(token)) 
        self.org = self.github.get_organization(org_name)

    def get_organization(self) -> str:
        """Retorna o nome da organização."""
        return self.org.name

    def get_teams(self) -> List[Team]:
        """Retorna uma lista de equipes como objetos Pydantic."""
        return [
            Team(name=team.name, slug=team.slug)
            for team in self.org.get_teams()
        ]

    def get_team_members(self, team_slug: str) -> List[Member]:
        """Retorna uma lista de membros da equipe como objetos MemberModel."""
        team = self.org.get_team_by_slug(team_slug)
        members = []
        for member in team.get_members():
            members.append(Member(
                login=member.login,
                name=member.name,
                email=member.email,
                avatar_url=member.avatar_url,
                html_url=member.html_url
            ))
        return members

    def get_teams_with_members(self) -> List[TeamMembership]:
        """Retorna uma lista de equipes com membros como objetos Pydantic."""
        teams_with_members = []
        for team in self.org.get_teams():
            team_instance = Team(name=team.name, slug=team.slug)
            members = [
                Member(
                    login=member.login,
                    name=member.name,
                    email=member.email,
                    avatar_url=member.avatar_url,
                    html_url=member.html_url
                )
                for member in team.get_members()
            ]
            teams_with_members.append(
                TeamMembership(name=team_instance.name, slug=team_instance.slug, team=team_instance, members=members)
            )
        return teams_with_members

    def get_projects(self) -> List[Project]:
        """Retorna todos os Projects (beta) da organização ou usuário via GraphQL."""
        url = "https://api.github.com/graphql"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        # Verifica se é uma organização ou um usuário
        is_org = hasattr(self.org, 'get_teams')  # truque simples usando PyGithub

        if is_org:
            entity_type = "organization"
        else:
            entity_type = "user"

        query = f"""
        query($login: String!, $after: String) {{
        {entity_type}(login: $login) {{
            projectsV2(first: 100, after: $after) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                id
                title
                number
            }}
            }}
        }}
        }}
        """

        projects = []
        has_next_page = True
        end_cursor = None

        while has_next_page:
            variables = {"login": self.org.login, "after": end_cursor}
            response = requests.post(
                url,
                headers=headers,
                json={"query": query, "variables": variables}
            )

            if response.status_code != 200:
                raise Exception(f"Erro na requisição GraphQL: {response.text}")

            data = response.json()
            container = data["data"][entity_type]["projectsV2"]
            projects.extend([
                Project(name=project["title"], id=project["id"], number=project["number"])
                for project in container["nodes"]
            ])
            has_next_page = container["pageInfo"]["hasNextPage"]
            end_cursor = container["pageInfo"]["endCursor"]

        return projects
 
    def get_repositories(self) -> List[Repository]:
        """Retorna todos os repositórios com suas issues, milestones e autores/assignees detalhados."""
        repositories: List[Repository] = []

        for repo in self.org.get_repos():
            issues: List[Issue] = []
            milestones: List[Milestone] = []
           
            # Cria o objeto Repository
            repositories.append(Repository(
                name=repo.name,
                full_name=repo.full_name,
                html_url=repo.html_url,
                issues=issues,
                milestones=milestones
            ))

        return repositories

    def get_milestones(self, full_repo_name: str) -> List[Milestone]:
        
        repo = self.github.get_repo(full_repo_name)

        milestones: List[Milestone] = [
            Milestone(
                number=ms.number,
                title=ms.title,
                description=ms.description,
                due_on=ms.due_on.isoformat() if isinstance(ms.due_on, datetime) else None,
                open_issues=ms.open_issues,
                closed_issues=ms.closed_issues,
                state=ms.state,
                url= ms.url,
                creator=ms.creator.login if ms.creator else None,
                created_at=ms.created_at.isoformat() if isinstance(ms.created_at, datetime) else None,
                closed_at=ms.closed_at.isoformat() if isinstance(ms.closed_at, datetime) else None,
                update_at = ms.updated_at.isoformat() if isinstance(ms.updated_at, datetime) else None

            )
            for ms in repo.get_milestones(state="all")
        ]
        
        

        return milestones
  
    def get_issues(self, full_repo_name: str) -> List[Issue]:
        repo = self.github.get_repo(full_repo_name)
        issues: List[Issue] = []

        for issue in repo.get_issues(state="all"):
            if issue.pull_request:
                continue

            assignees = [
                Member(
                    login=a.login,
                    name=a.name,
                    email=a.email,
                    avatar_url=a.avatar_url,
                    html_url=a.html_url
                ) for a in issue.assignees
            ]

            author = issue.user
            author_member = Member(
                login=author.login,
                name=author.name,
                email=author.email,
                avatar_url=author.avatar_url,
                html_url=author.html_url
            ) if author else None

            milestone_obj = None
            if issue.milestone:
                ms = issue.milestone
                milestone_obj = Milestone(
                    number=ms.number,
                    title=ms.title,
                    description=ms.description,
                    state=ms.state,
                    due_on=ms.due_on.isoformat() if isinstance(ms.due_on, datetime) else None,
                    open_issues=ms.open_issues,
                    closed_issues=ms.closed_issues,
                    url=ms.html_url,
                    creator=ms.creator.login if ms.creator else None,
                    created_at=ms.created_at.isoformat() if isinstance(ms.created_at, datetime) else None,
                    closed_at=ms.closed_at.isoformat() if isinstance(ms.closed_at, datetime) else None,
                    update_at = ms.updated_at.isoformat() if isinstance(ms.updated_at, datetime) else None

                )

            issue_type = ", ".join(label.name.lower() for label in issue.labels)
            issues.append(Issue(
                number=issue.number,
                description=issue.body,
                repository=repo.name,  # Nome do repositório
                title=issue.title,
                url=issue.html_url,
                state=issue.state,
                assignees=assignees,
                author=author_member,
                created_at=issue.created_at.isoformat() if isinstance(issue.created_at, datetime) else None,
                closed_at=issue.closed_at.isoformat() if isinstance(issue.closed_at, datetime) else None,
                milestone=milestone_obj,
                type=issue_type,
                projects=[]  # projetos podem ser preenchidos em outra função
            ))

        return issues
