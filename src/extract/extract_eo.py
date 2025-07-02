from typing import Any  # noqa: I001
from src.extract.extract_base import ExtractBase  # noqa: I001
from src.config.logging_config import LoggerFactory  # noqa: I001


class ExtractEO(ExtractBase):
    """Extracts and loads data related to teams, team members, and projects."""

    # DataFrames extracted from Airbyte
    team_members: Any = None
    teams: Any = None
    projects: Any = None
    team_memberships: Any = None
    users: Any = None
    organization_node: Any = None

    def __init__(self) -> None:
        """Post-initialization hook."""
        self.logger = LoggerFactory.get_logger(__name__)
        self.streams = ["projects_v2", "teams", "team_members"]
        super().__init__()

    def fetch_data(self) -> None:
        """Load data from the Airbyte cache into pandas DataFrames."""  # noqa: D401
        self.logger.info("Fetching data from Airbyte cache.")
        self.load_data()

        if "teams" in self.cache:
            self.teams = self.cache["teams"].to_pandas()
            self.logger.info("✅ %d teams loaded.", len(self.teams))

        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            self.logger.info("✅ %d projects_v2 loaded.", len(self.projects))

        if "team_members" in self.cache:
            self.team_members = self.cache["team_members"].to_pandas()
            self.logger.info("✅ %d team_members loaded.", len(self.team_members))

    def __load_project(self) -> None:
        """Create project nodes and relationships to the organization in Neo4j."""
        self.logger.info("Creating Project nodes and relationships...")
        for project in self.projects.itertuples():
            data = self.transform(project)
            project_node = self.create_node(data, "Project", "id")
            self.create_relationship(self.organization_node, "has", project_node)

    def __load_team_member(self) -> None:
        """Create Person and TeamMember and links them to teams and the organization."""
        self.logger.info("Creating TeamMember and Person nodes...")
        for member in self.team_members.itertuples():
            data = self.transform(member)
            data["id"] = member.login
            data["name"] = member.login

            person_node = self.create_node(data, "Person", "id")
            self.create_relationship(person_node, "present_in", self.organization_node)

            if member.team_slug:
                data["id"] = f"{member.login}-{member.team_slug}"
                data["name"] = member.login

                team_member_node = self.create_node(data, "TeamMember", "id")
                team_node = self.sink.get_node("Team", slug=member.team_slug)

                self.create_relationship(team_member_node, "done_for", team_node)
                self.create_relationship(team_node, "has", team_member_node)
                self.create_relationship(team_member_node, "is", person_node)

    def __load_team(self) -> None:
        """Create Team nodes and links them to the organization."""
        self.logger.info("Creating Team nodes and relationships...")
        for team in self.teams.itertuples():
            data = self.transform(team)
            team_node = self.create_node(data, "Team", "id")
            self.logger.info("🔄 Creating Team... %s", team.name)
            self.create_relationship(self.organization_node, "has", team_node)

    def run(self) -> None:
        """Orchestrate the full extraction and loading process."""
        self.logger.info("🔄 Starting extraction for Teams, Projects, and Members...")
        self.fetch_data()
        self.__load_project()
        self.__load_team()
        self.__load_team_member()
        self.logger.info("✅ Extraction completed successfully!")
