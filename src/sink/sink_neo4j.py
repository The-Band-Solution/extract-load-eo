from pydantic import BaseModel
from py2neo import Graph, Node, Relationship
import os
from dotenv import load_dotenv
from typing import Any

class SinkNeo4j (BaseModel):
    
    graph : Any = None
    
    def model_post_init(self, __context):        
        load_dotenv()
        self.graph = Graph(os.getenv("NEO4J_URI", ""), 
                           auth=(os.getenv("NEO4J_USERNAME", ""),
                            os.getenv("NEO4J_PASSWORD", "")))
        
    
    def save_node(self, element:Any, type:str, id_element:str):
        self.graph.merge(element, type, id_element)
    
    def save_relationship(self, element:Relationship):
        self.graph.merge(element)
    
    def get_node(self, type: str, **properties):
        matcher = self.graph.nodes.match(type, **properties)
        return matcher.first()
