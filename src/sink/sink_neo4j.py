import os  # noqa: I001
from typing import Any  # noqa: I001
from dotenv import load_dotenv  # noqa: I001
from py2neo import Graph, Node, Relationship  # noqa: I001


class SinkNeo4j:
    """Handles connections and interactions with the Neo4j graph database.

    This class provides methods to persist nodes and relationships,
    and retrieve nodes from the Neo4j database using py2neo.
    """

    graph: Any = None  # Py2neo Graph instance

    def __init__(self) -> None:
        """Initializes the connection to the Neo4j database using environment variables.

        Environment variables required:
            - NEO4J_URI: URI of the Neo4j instance (e.g., bolt://localhost:7687)
            - NEO4J_USERNAME: Username for authentication
            - NEO4J_PASSWORD: Password for authentication
        """  # noqa: D401
        load_dotenv()
        self.graph = Graph(
            os.getenv("NEO4J_URI", ""),
            auth=(os.getenv("NEO4J_USERNAME", ""), os.getenv("NEO4J_PASSWORD", "")),
        )

    def save_node(self, element: Any, type_elment: str, id_element: str) -> None:
        """Saves or updates a node in the Neo4j graph.

        If a node with the specified label (`type`) and unique ID (`id_element`)
        already exists, it will be updated; otherwise, it will be created.

        Args:
        ----
            element (Any): The py2neo Node object to save.
            type_elment (str): The label of the node (e.g., "User", "Repository").
            id_element (str): key that identify a node

        """  # noqa: D401
        self.graph.merge(element, type_elment.strip().lower(), id_element)

    def save_relationship(self, element: Relationship) -> None:
        """Saves or updates a relationship in the Neo4j graph.

        If the relationship already exists between the same nodes with the same type,
        it will be updated; otherwise, it will be created.

        Args:
        ----
            element (Relationship): The py2neo Relationship object to save.

        """  # noqa: D401
        self.graph.merge(element)

    def get_node(self, type: str, **properties: Any) -> Node:
        """Retrieves the first node from Neo4j that matches the given label
        and properties.

        Args:
        ----
            type (str): The label of the node (e.g., "User", "Repository").
            **properties (Any): Arbitrary property key-value pairs to match.

        Returns:
        -------
            Node: The first matching node, or None if no node matches.

        """  # noqa: D205, D401
        matcher = self.graph.nodes.match(type, **properties)
        return matcher.first()
