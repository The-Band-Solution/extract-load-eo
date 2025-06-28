from typing import Any  # noqa: I001
from src.extract.extract_base import ExtractBase  # noqa: I001


class ExtractEO(ExtractBase):
    """Extracts and loads data related to teams, team members, and projects
    into the Neo4j graph database.

    This class retrieves data from Airbyte streams (`projects_v2`,
    `teams`, `team_members`),
    transforms it, and persists it as nodes and relationships within the graph.
    """  # noqa: D205

    # DataFrames extracted from Airbyte
    team_members: Any = None
    teams: Any = None
    projects: Any = None
    team_memberships: Any = None
    users: Any = None
    organization_node: Any = None

    def __init__(self) -> None:
        """Post-initialization hook.

        Defines the streams (`projects_v2`, `teams`, `team_members`) to fetch
        and initializes the parent class configuration including Airbyte and Neo4j
        connections.
        """
        self.streams = ["projects_v2", "teams", "team_members"]
        super().__init__()

    def fetch_data(self) -> None:
        """Loads data from the Airbyte cache into pandas DataFrames."""  # noqa: D401
        self.load_data()

        if "teams" in self.cache:
            self.teams = self.cache["teams"].to_pandas()
            print(f"✅ {len(self.teams)} teams loaded.")

        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            print(f"✅ {len(self.projects)} projects_v2 loaded.")

        if "team_members" in self.cache:
            self.team_members = self.cache["team_members"].to_pandas()
            print(f"✅ {len(self.team_members)} team_members loaded.")

    def __load_project(self) -> None:
        """Creates project nodes and relationships to the organization in Neo4j."""  # noqa: D401
        for project in self.projects.itertuples():
            data = self.transform(project)
            project_node = self.create_node(data, "Project", "id")

            # Relationship: Organization has Project
            self.create_relationship(self.organization_node, "has", project_node)

            # Optional: You can create relationships between Project and
            # Repository here.

    def __load_team_member(self) -> None:
        """Creates Person and TeamMember nodes in Neo4j and links
        them to teams and the organization.
        """  # noqa: D205, D401
        for member in self.team_members.itertuples():
            data = self.transform(member)
            data["id"] = member.login

            person_node = self.create_node(data, "Person", "id")

            # Relationship: Person present in Organization
            self.create_relationship(person_node, "present_in", self.organization_node)

            if member.team_slug:
                # Create TeamMember node (membership instance)
                data["id"] = f"{member.login}-{member.team_slug}"

                team_member_node = self.create_node(data, "TeamMember", "id")

                # Get Team node
                team_node = self.sink.get_node("Team", slug=member.team_slug)

                # Relationships
                self.create_relationship(team_member_node, "done_for", team_node)
                self.create_relationship(team_node, "has", team_member_node)
                self.create_relationship(team_member_node, "is", person_node)

    def __load_team(self) -> None:
        """Creates Team nodes and links them to the organization."""  # noqa: D401
        for team in self.teams.itertuples():
            data = self.transform(team)
            team_node = self.create_node(data, "Team", "id")
            print(f"🔄 Creating Team... {team.name}")

            # Relationship: Organization has Team
            self.create_relationship(self.organization_node, "has", team_node)

    def run(self) -> None:
        """Orchestrates the full extraction and loading process.

        Loads data for projects, teams, and team members, then persists
        them into the Neo4j graph with the appropriate relationships.
        """
        print("🔄 Extracting data for Teams and Members...")
        self.fetch_data()
        self.__load_project()
        self.__load_team()
        self.__load_team_member()
        print("✅ Extraction completed successfully!")
