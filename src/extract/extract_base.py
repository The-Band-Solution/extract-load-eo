import os  # noqa: I001
from typing import Any  # noqa: I001
import airbyte as ab  # noqa: I001
import pandas as pd  # noqa: I001
from dotenv import load_dotenv  # noqa: I001
from py2neo import Node, Relationship  # noqa: I001
from sink.sink_neo4j import SinkNeo4j  # noqa: I001
import json  # noqa: I001
from abc import ABC, abstractmethod  # noqa: I001
from types import SimpleNamespace  # noqa: I001
from datetime import datetime  #  noqa: I001


class ExtractBase(ABC):
    """Base class for data extraction using Airbyte as the source
    and Neo4j as the destination.

    This class provides core functionalities for reading data,
    transforming records, and persisting nodes and relationships
    into a Neo4j graph database.
    """  # noqa: D205

    # Class attributes
    config_node: Any = None  # Config node
    organization_node: Any = None  # Neo4j Node object representing the organization
    token: str = ""  # API token (e.g., GitHub token)
    client: Any = None  # API client (to be defined in subclasses)
    streams: list[str] = []  # List of Airbyte streams to extract
    cache: Any = None  # Local cache managed by Airbyte (DuckDB)
    source: Any = None  # Airbyte source connector
    sink: Any = None  # Data sink, in this case Neo4j (via SinkNeo4j)

    def __init__(self) -> None:
        """Post-initialization hook.

        Loads environment variables, initializes the Neo4j sink,
        configures the Airbyte source (if streams are set),
        and loads the organization node into the graph.
        """
        load_dotenv()

        # Initialize the Neo4j sink
        self.sink = SinkNeo4j()

        # Load GitHub token from .env
        self.token = os.getenv("GITHUB_TOKEN", "")

        # If streams are configured, set up the Airbyte source
        if self.streams:
            config = {
                "repositories": [os.getenv("REPOSITORIES", "")],
                "credentials": {
                    "personal_access_token": os.getenv("GITHUB_TOKEN", ""),
                },
            }

            self.config_node = self.sink.get_node(
                "Config", id=os.getenv("ORGANIZATION_ID", "")
            )

            if self.config_node is not None:
                print(
                    "🔄 Retrieving data from Github at {}...".format(
                        self.config_node["last_retrieve_date"]
                    )
                )
                config["start_date"] = self.config_node["last_retrieve_date"]
            else:
                print("🔄 Retrieving all data from Github ...")

            self.source = ab.get_source(
                "source-github",
                install_if_missing=True,
                config=config,
            )

            # Check if source credentials and config are valid
            self.source.check()

            # Load the organization node from Neo4j or create it
            self.__load_organization()

    def load_data(self) -> None:
        """Loads data from the Airbyte source into the local cache."""  # noqa: D401
        self.source.select_streams(self.streams)  # Select streams to load
        self.cache = ab.get_default_cache()  # Initialize DuckDB cache
        self.source.read(cache=self.cache)  # Read data into cache

    def transform(self, value: Any) -> Any:
        """Transforms a record from Airbyte into a clean dictionary.

        Removes auxiliary fields (starting with "_airbyte")
        and converts NaN values to None.

        Args:
        ----
            value (Any): A single record object.

        Returns:
        -------
            dict: A clean dictionary representation of the record.

        """  # noqa: D401
        data = {
            k: (v if not pd.isna(v) else None)  # Replace NaN with None
            for k, v in value._asdict().items()  # Convert to dict
            if not k.startswith("_airbyte")  # Remove metadata fields
        }
        return data

    def save_node(self, node: Node, type: str, key: str) -> Node:
        """Persists a node into Neo4j.

        Args:
        ----
            node (Node): Py2neo Node object.
            type (str): Node label (e.g., "User", "Repository").
            key (str): Unique property key to identify the node.

        Returns:
        -------
            Node: The persisted node.

        """  # noqa: D401
        return self.sink.save_node(node, type, key)

    def save_relationship(self, element: Relationship) -> Relationship:
        """Persists a relationship into Neo4j.

        Args:
        ----
            element (Relationship): Py2neo Relationship object.

        Returns:
        -------
            Relationship: The persisted relationship.

        """  # noqa: D401
        return self.sink.save_relationship(element)

    def get_node(self, type_element: str, **properties: Any) -> Node:
        """Retrieves a node from Neo4j based on type and properties.

        Args:
        ----
            type_element (str): Node label (e.g., "User", "Repository").
            **properties (Any): Property filters (e.g., id="123").

        Returns:
        -------
            Node: The matched node, or None if not found.

        """  # noqa: D401
        return self.sink.get_node(type_element, **properties)

    @abstractmethod
    def fetch_data(self) -> None:
        """Retrieve data from a data repository."""
        pass

    def transform_object(self, value: Any) -> Any:
        """Trasnform dictionary to object.

        Args:
        ----
            value (Any): dictionary

        Returns:
        -------
            Object: Object

        """  # noqa: D401
        return json.loads(value, object_hook=lambda d: SimpleNamespace(**d))

    def create_relationship(
        self, node_from: Node, relation: str, node_to: Node
    ) -> None:
        """Create a Relationship between nodes.

        Args:
        ----
            node_from (Node): node source relationship
            relation (str): relation name
            node_to (Node): node target from relationship

        """  # noqa: D401
        self.sink.save_relationship(Relationship(node_from, relation, node_to))

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

        """  # noqa: D401
        data["created_node_at"] = datetime.now().isoformat()
        node = Node(node_type, **data)
        self.sink.save_node(node, node_type, id_field)
        return node

    def __create_retrieve(self) -> None:
        """Load retrieve date."""  # noqa: D401
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = today.isoformat() + "Z"
        self.config_node = Node(
            "Config",
            id=os.getenv("ORGANIZATION_ID", ""),
            name=os.getenv("ORGANIZATION", ""),
            last_retrieve_date=start_date,
        )
        self.sink.save_node(self.config_node, "Config", "id")

    def __load_organization(self) -> None:
        """Loads the organization node from Neo4j.

        If the node does not exist, it creates and persists it
        using the client information (e.g., GitHub organization name).
        """  # noqa: D401
        # Attempt to retrieve the organization node
        organization_id = os.getenv("ORGANIZATION_ID", "")
        print(f"Buscando a organizacao: {organization_id}")
        self.organization_node = self.sink.get_node(
            "Organization", id=os.getenv("ORGANIZATION_ID", "")
        )
        print(f"Buscando a organizacao: {self.organization_node}")
        # If the node does not exist, create it
        if self.organization_node is None:
            print("🔄 Creating Organization node...")

            self.organization_node = Node(
                "Organization",
                id=os.getenv("ORGANIZATION_ID", ""),
                name=os.getenv("ORGANIZATION", ""),
            )

            self.sink.save_node(self.organization_node, "Organization", "id")
