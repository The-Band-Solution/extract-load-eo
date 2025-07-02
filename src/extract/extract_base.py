import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import airbyte as ab
import pandas as pd
from dotenv import load_dotenv
from py2neo import Node, Relationship

from sink.sink_neo4j import SinkNeo4j
from src.config.logging_config import LoggerFactory

logger = LoggerFactory.get_logger("extractor")


class ExtractBase(ABC):
    """Base class for data extraction."""

    # Class attributes
    config_node: Any = None  # Config node
    organization_node: Any = None  # Neo4j Node object representing the organization
    token: str = ""  # API token (e.g., GitHub token)
    client: Any = None  # API client (to be defined in subclasses)
    streams: list[str] = []  # List of Airbyte streams to extract
    cache: Any = None  # Local cache managed by Airbyte (DuckDB)
    source: Any = None  # Data source connector (Airbyte)
    sink: Any = None  # Data sink, in this case Neo4j (via SinkNeo4j)

    def __init__(self) -> None:
        """Post-initialization hook."""
        logger.info("Initializing ExtractBase...")
        load_dotenv()
        logger.debug("Environment variables loaded.")

        # Initialize the Neo4j sink
        try:
            self.sink = SinkNeo4j()
            logger.info("Neo4j sink initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Neo4j sink: {e}")
            raise

        # Load GitHub token from .env
        self.token = os.getenv("GITHUB_TOKEN", "")
        if not self.token:
            logger.warning("GITHUB_TOKEN not found in environment variables.")
        else:
            logger.debug("GitHub token loaded.")

        # If streams are configured, set up the Airbyte source
        if self.streams:
            logger.info(f"Configuring Airbyte source for streams: {self.streams}")
            repositories = os.getenv("REPOSITORIES", "")
            if not repositories:
                logger.warning("REPOSITORIES environment variable is not set.")

            config = {
                "repositories": [repositories],
                "credentials": {
                    "personal_access_token": self.token,
                },
            }
            logger.debug(f"Airbyte source initial config: {config}")

            organization_id = os.getenv("ORGANIZATION_ID", "")
            if not organization_id:
                logger.warning("ORGANIZATION_ID environment variable is not set.")

            self.config_node = self.sink.get_node("Config", id=organization_id)

            if self.config_node is not None:
                config["start_date"] = self.config_node["last_retrieve_date"]
                logger.info(f"Using start_date: {config['start_date']}")
            else:
                logger.info("No existing Config node found.")
                self.__create_retrieve()  # Ensure config_node is created if not exists

            try:
                self.source = ab.get_source(
                    "source-github",
                    install_if_missing=True,
                    config=config,
                )
                logger.info("Airbyte source-github obtained.")

                # Check if source credentials and config are valid
                self.source.check()
                logger.info("Airbyte source check passed successfully.")
            except Exception as e:
                logger.error(f"Failed to configure or check Airbyte source: {e}")
                raise
        else:
            logger.info("No Airbyte streams configured. Skipping Airbyte source setup.")

        # Load the organization node from Neo4j or create it
        self.__load_organization()
        logger.info("ExtractBase initialization complete.")

    def load_data(self) -> None:
        """Load data from the Airbyte source into the local cache."""
        if not self.source:
            logger.warning("Airbyte source not initialized. Cannot load data.")
            return

        logger.info(f"Selecting streams to load: {self.streams}")
        self.source.select_streams(self.streams)  # Select streams to load

        logger.info("Initializing DuckDB cache.")
        self.cache = ab.get_default_cache()  # Initialize DuckDB cache

        logger.info("Reading data from Airbyte source into cache...")
        try:
            self.source.read(cache=self.cache)  # Read data into cache
            logger.info("Data loaded successfully into cache.")
        except Exception as e:
            logger.error(f"Failed to load data from Airbyte source: {e}")
            raise

    def transform(self, value: Any) -> Any:
        """Transform a record from Airbyte into a clean dictionary.

        Removes auxiliary fields (starting with "_airbyte")
        and converts NaN values to None.

        Args:
        ----
            value (Any): A single record object.

        Returns:
        -------
            dict: A clean dictionary representation of the record.

        """
        logger.debug(f"Transforming record: {value}")
        data = {
            k: (v if not pd.isna(v) else None)  # Replace NaN with None
            for k, v in value._asdict().items()  # Convert to dict
            if not k.startswith("_airbyte")  # Remove metadata fields
        }
        logger.debug(f"Transformed record: {data}")
        return data

    def save_node(self, node: Node, type: str, key: str) -> Node:
        """Persist a node into Neo4j.

        Args:
        ----
            node (Node): Py2neo Node object.
            type (str): Node label (e.g., "User", "Repository").
            key (str): Unique property key to identify the node.

        Returns:
        -------
            Node: The persisted node.

        """
        logger.info(
            f"Save node of type '{type}' with key '{key}' and properties: {dict(node)}"
        )
        try:
            persisted_node = self.sink.save_node(node, type, key)
            logger.info(f"Node '{type}' with key '{key}' saved successfully.")
            return persisted_node
        except Exception as e:
            logger.error(f"Failed to save node '{type}' with key '{key}': {e}")
            raise

    def save_relationship(self, element: Relationship) -> Relationship:
        """Persist a relationship into Neo4j.

        Args:
        ----
            element (Relationship): Py2neo Relationship object.

        Returns:
        -------
            Relationship: The persisted relationship.

        """
        logger.info(f"Attempting to save relationship: {element}")
        try:
            persisted_relationship = self.sink.save_relationship(element)
            logger.info(f"Relationship '{element}' saved successfully.")
            return persisted_relationship
        except Exception as e:
            logger.error(f"Failed to save relationship '{element}': {e}")
            raise

    def get_node(self, type_element: str, **properties: Any) -> Node:
        """Retrieve a node from Neo4j based on type and properties.

        Args:
        ----
            type_element (str): Node label (e.g., "User", "Repository").
            **properties (Any): Property filters (e.g., id="123").

        Returns:
        -------
            Node: The matched node, or None if not found.

        """
        logger.info(
            f"Retrieve node of type '{type_element}' with properties: {properties}"
        )
        try:
            node = self.sink.get_node(type_element, **properties)
            if node:
                logger.info(
                    f"Node '{type_element}' with properties {properties} found."
                )
            else:
                logger.info(
                    f"Node '{type_element}' with properties {properties} not found."
                )
            return node
        except Exception as e:
            logger.error(
                f"Failed to retrieve node '{type_element}'"
                + f" with properties {properties}: {e}"
            )
            raise

    @abstractmethod
    def fetch_data(self) -> None:
        """Retrieve data from a data repository."""
        logger.info("Abstract method 'fetch_data' called.")
        pass

    def transform_object(self, value: Any) -> Any:
        """Transform dictionary to object.

        Args:
        ----
            value (Any): dictionary

        Returns:
        -------
            Object: Object

        """
        logger.debug(f"Transforming dictionary to object: {value}")
        try:
            obj = json.loads(value, object_hook=lambda d: SimpleNamespace(**d))
            logger.debug("Dictionary transformed to object successfully.")
            return obj
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error during object transformation: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during object transformation: {e}"
            )
            raise

    def create_relationship(
        self, node_from: Node, relation: str, node_to: Node
    ) -> None:
        """Create a Relationship between nodes.

        Args:
        ----
            node_from (Node): node source relationship
            relation (str): relation name
            node_to (Node): node target from relationship

        """
        logger.info(
            f"Create relationship '{relation}' "
            f"from {dict(node_from)} "
            f"to {dict(node_to)}"
        )

        try:
            self.sink.save_relationship(Relationship(node_from, relation, node_to))
            logger.info(f"Relationship '{relation}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create relationship '{relation}': {e}")
            raise

    def create_node(self, data: Any, node_type: str, id_field: str) -> Node:
        """Create a Node.

        Args:
        ----
            data (Any): data to be save in a node
            node_type (str): type of node
            id_field (str): define field that will be node's id

        Returns:
        -------
            Node: a node in a graph

        """
        logger.info(f"Create node '{node_type}' with field '{id_field}': {data}")
        data["created_node_at"] = datetime.now().isoformat()
        node = Node(node_type, **data)
        try:
            self.sink.save_node(node, node_type, id_field)
            logger.info(
                f"Node '{node_type}' - '{data.get(id_field)}' created and saved."
            )
            return node
        except Exception as e:
            logger.error(
                f"Failed to create and save '{node_type}' '{data.get(id_field)}': {e}"
            )
            raise

    def __create_retrieve(self) -> None:
        """Load retrieve date."""
        logger.info("Creating retrieve date configuration node.")
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = today.isoformat() + "Z"
        organization_id = os.getenv("ORGANIZATION_ID", "")
        organization_name = os.getenv("ORGANIZATION", "")

        if not organization_id:
            logger.warning("ORGANIZATION_ID not found for creating retrieve config.")
            return
        if not organization_name:
            logger.warning("ORGANIZATION not found for creating retrieve config.")
            return

        self.config_node = Node(
            "Config",
            id=organization_id,
            name=organization_name,
            last_retrieve_date=start_date,
        )
        try:
            self.sink.save_node(self.config_node, "Config", "id")
            logger.info(
                f"Config node created/updated with last_retrieve_date: {start_date}"
            )
        except Exception as e:
            logger.error(f"Failed to save Config node for retrieve date: {e}")
            raise

    def __load_organization(self) -> None:
        """Load the organization node."""
        organization_id = os.getenv("ORGANIZATION_ID", "")
        organization_name = os.getenv("ORGANIZATION", "")

        if not organization_id:
            logger.warning("ORGANIZATION_ID not found for loading organization node.")
            return
        if not organization_name:
            logger.warning("ORGANIZATION not found for loading organization node.")
            return

        logger.info(f"Attempting to load organization node with ID: {organization_id}")
        self.organization_node = self.sink.get_node("Organization", id=organization_id)
        # If the node does not exist, create it
        if self.organization_node is None:
            logger.info(f"Organization '{organization_id}' not found.")
            self.organization_node = Node(
                "Organization",
                id=organization_id,
                name=organization_name,
            )
            try:
                self.sink.save_node(self.organization_node, "Organization", "id")
                logger.info(
                    f"Organization node '{organization_name}' created and saved."
                )
            except Exception as e:
                logger.error(f"Failed to create and save organization node: {e}")
                raise
        else:
            logger.info(f"Organization '{organization_name}' loaded.")
