from src.config.logging_config import LoggerFactory
from src.extract.extract_ciro import ExtractCIRO
from src.extract.extract_cmpo import ExtractCMPO
from src.extract.extract_eo import ExtractEO


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
    logger = LoggerFactory.get_logger("extractor")
    logger.info("Starting data extraction pipeline.")

    try:
        # Run the EO extractor (Teams, Members, Projects)
        logger.info("Running ExtractEO (Teams, Members, Projects)...")
        ExtractEO().run()
        logger.info("ExtractEO completed successfully.")

        # Run the CMPO extractor (Repositories, Commits, Branches, Projects)
        logger.info(
            "Running ExtractCMPO (Repositories, Commits, Branches, Projects)..."
        )
        ExtractCMPO().run()
        logger.info("ExtractCMPO completed successfully.")

        # Run the CIRO extractor (Issues, Milestones, Pull Requests, Labels)
        logger.info(
        "Running ExtractCIRO (Issues, Milestones, Pull Requests, Labels)..."
        )
        ExtractCIRO().run()
        logger.info("ExtractCIRO completed successfully.")

        logger.info("✅ Extraction pipeline completed successfully")

    except Exception as e:
        # Log the exception with traceback for detailed error analysis
        logger.exception(f"❌ Extraction pipeline failed with an exception: {e}")


if __name__ == "__main__":
    main()
