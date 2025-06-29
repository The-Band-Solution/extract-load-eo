from py2neo import Graph  # noqa: I001
from typing import Any  # noqa: I001
import os  # noqa: I001
from collections import defaultdict  # noqa: I001


class TeamReport:
    """Report from Team context."""

    def __init__(self) -> None:
        """Initialize connection to Neo4j database."""
        self.graph = Graph(
            os.getenv("NEO4J_URI", ""),
            auth=(
                os.getenv("NEO4J_USER", ""),
                os.getenv("NEO4J_PASSWORD", ""),
            ),
        )

    def fetch_persons_in_organizations(self) -> list[dict[str, Any]]:
        """Fetch Persons who are PRESENT_IN an Organization.

        Returns
        -------
            list: list of dictionaries representing Persons and their Organizations.

        """  # noqa: D205, D401
        query = """MATCH (p:Person)-[:present_in]->(o:Organization)
            WITH p, o
            ORDER BY p.name
            RETURN p, o"""

        result = self.graph.run(query).data()

        people = []
        for row in result:
            person_props = dict(row["p"])
            org_props = dict(row["o"])
            person_props["organization"] = org_props
            people.append(person_props)

        return people

    def create_people_markdown(self) -> str:
        """Creates a markdown table from a list of people with their organization.

        Returns
        -------
            str: Markdown formatted table.

        """  # noqa: D205, D401
        people = self.fetch_persons_in_organizations()
        md = ["# People on Organization"]
        md.append("| Name | Login | Organization |")
        md.append("|------|--------|--------------|")
        for person in people:
            name = person.get("name", "").strip()
            login = person.get("login", "").strip().lower()
            organization = person.get("organization", {}).get("name", "").strip()
            md.append(f"| {name} | {login} | {organization} |")
        return "\n".join(md)

    def fetch_team_members_with_person(self) -> list[dict[str, Any]]:
        """Fetch TeamMembers with their respective Team and Person info.

        Returns
        -------
            list: list of dictionaries, each with team, team_member, and person.

        """  # noqa: D205, D401
        query = """MATCH (t:Team)-[:has]->(tm:TeamMember)-[:is]->(p:Person)
                RETURN t, tm, p ORDER BY t.name, p.name"""

        result = self.graph.run(query).data()

        members = []
        for row in result:
            team = dict(row["t"])
            team_member = dict(row["tm"])
            person = dict(row["p"])

            members.append({"team": team, "team_member": team_member, "person": person})

        return members

    def create_team_markdown(self) -> str:
        """Create a markdown listing members grouped by team.

        Returns
        -------
            str: Markdown formatted string.

        """  # noqa: D205, D401
        members = self.fetch_team_members_with_person()

        teams = defaultdict(list)

        for entry in members:
            team_name = entry["team"].get("name", "Unknown Team").strip()
            person_name = entry["person"].get("name", "").strip()
            person_login = entry["person"].get("login", "").strip().lower()

            teams[team_name].append((person_name, person_login))

        # Generate markdown
        md = []
        for team, people in sorted(teams.items()):
            md.append(f"## {team}")
            md.append("| Name | Login |")
            md.append("|------|-------|")
            for name, login in people:
                md.append(f"| {name} | {login} |")
            md.append("")  # empty line after each team

        return "\n".join(md)

    def save_markdown_to_file(self, markdown: str, path: str) -> None:
        """Save the markdown string to a file.

        Args:
        ----
            markdown (str): The markdown content.
            path (str): The full file path (e.g., /data/output.md).

        """  # noqa: D205, D401
        with open(path, "w", encoding="utf-8") as f:
            f.write(markdown)
