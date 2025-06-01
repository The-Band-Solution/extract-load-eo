from github import Github
from typing import List
from src.model.models import Team,  Member, TeamMembership

class GitHubClient:
    def __init__(self, token: str, org_name: str):
        self.github = Github(token)
        self.org = self.github.get_organization(org_name)

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

