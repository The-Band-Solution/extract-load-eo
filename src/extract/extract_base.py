from pydantic import BaseModel
from typing import List, Any
import os
from dotenv import load_dotenv
from py2neo import Graph, Node, Relationship
from source.github_client import GitHubClient

class ExtractBase(BaseModel):
    graph : Any = None
    organization: str = ""
    token: str = ""
    cache: Any = None
    client: Any = None
    
    def model_post_init(self, __context):
        load_dotenv()
        self.organization = os.getenv("GITHUB_ORG", "")
        self.token = os.getenv("GITHUB_TOKEN", "")
        self.client = GitHubClient(self.token, self.organization)
        
        self.graph = Graph(os.getenv("NEO4J_URI", ""), 
                           auth=(os.getenv("NEO4J_USERNAME", ""),
                                 os.getenv("NEO4J_PASSWORD", "")))
        
        self.fetch_data()

    def fech_data(self):
        pass