import os  # noqa: I001
from sink.sink_neo4j import SinkNeo4j  # noqa: I001
from datetime import datetime  #  noqa: I001
from py2neo import Node  # noqa: I001
from typing import Any  # noqa: I001


class CreateConfig:
    """Create Config."""  # noqa: D205

    sink: Any = None  # Data sink, in this case Neo4j (via SinkNeo4j)

    def __init__(self) -> None:
        """Post-initialization hook.

        Loads environment variables, initializes the Neo4j sink,
        configures the Airbyte source (if streams are set),
        and loads the organization node into the graph.
        """
        # Initialize the Neo4j sink
        self.sink = SinkNeo4j()

    def run(self) -> None:
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
