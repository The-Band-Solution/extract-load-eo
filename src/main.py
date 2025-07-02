from src.extract.extract_cmpo import ExtractCMPO  # noqa: I001
from src.extract.extract_ciro import ExtractCIRO  # noqa: I001
from src.extract.extract_eo import ExtractEO  # noqa: I001


def main() -> None:
    """Entry point for the data extraction pipeline.

    This function runs the three extractors in sequence:
        1. ExtractEO   - Loads data from Teams, Team Members, and Projects.
        2. ExtractCMPO - Loads data from Commits, Branches, Repositories, and Projects.
        3. ExtractCIRO - Loads data from Issues, Milestones, Pull Requests, and Labels.

    Each extractor connects to the data source (via Airbyte), transforms the data,
    and persists it into the Neo4j graph database.

    The order ensures that foundational elements like repositories, teams, and projects
    are created before dependent entities like issues and pull requests.
    """
    # Run the EO extractor (Teams, Members, Projects)
    ExtractEO().run()

    # Run the CMPO extractor (Repositories, Commits, Branches, Projects)
    ExtractCMPO().run()

    # Run the CIRO extractor (Issues, Milestones, Pull Requests, Labels)
    ExtractCIRO().run()

    # ExtractCMPOSoftwareArtifact().run()
    # TODO: Criar um config por dominio
    # CreateConfig().run()


if __name__ == "__main__":
    # Execute the main function when this script is run directly.
    main()
