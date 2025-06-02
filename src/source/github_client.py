from github import Github,Auth  
from typing import List
from model.models import Team,  Member, TeamMembership,Project
import requests

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

        