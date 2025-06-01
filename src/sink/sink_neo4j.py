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
        
    
    def save(self, element:Any, type:str, id_element:str):
        self.graph.merge(element, type, id_element)