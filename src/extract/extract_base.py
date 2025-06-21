from pydantic import BaseModel
from typing import  Any
import os
import pandas as pd
from dotenv import load_dotenv
import airbyte as ab
from source.github_client import GitHubClient
from sink.sink_neo4j import SinkNeo4j
from py2neo import Node, Relationship

class ExtractBase(BaseModel):   
    organization: str = ""
    organization_node: Any = None
    token: str = ""
    client: Any = None
    streams: list[str] = []
    cache: Any = None
    source: Any = None
    sink: Any = None
    
    def model_post_init(self, __context):
        load_dotenv()
        
        self.sink = SinkNeo4j()  
        self.token = os.getenv("GITHUB_TOKEN", "")
        
        if self.streams:
        
            self.source = ab.get_source(
                "source-github",
                install_if_missing=True,
                config={
                    "repositories": [os.getenv("REPOSITORIES", "")],
                    "credentials": {
                        "personal_access_token": os.getenv("GITHUB_TOKEN", ""),
                    },
                }
            )

            # Verify the config and creds by running `check`:
            self.source.check()
            self.__load_organization()
            
            
    def load_data(self):
        
        self.source.select_streams(self.streams)	
        # Read into DuckDB local default cache
        self.cache = ab.get_default_cache()
        self.source.read(cache=self.cache)
       
    def trasnform(self, value):
        data = {
            k: (v if not pd.isna(v) else None)
            for k, v in value._asdict().items()
            if not k.startswith("_airbyte")
        }
        return data
    
    def save_node (self, node:Node, type:str, key:str):
        return self.sink.save_node(node, type, key)
    
    def save_relationship(self, element:Relationship):
        return self.sink.save_relationship(element)
    
    def get_node(self, type: str, **properties):
        return self.sink.get_node(type, properties)

    def __load_organization(self):
        self.organization_node = self.sink.get_node("Organization",id=self.client.get_organization())
        
        if self.organization_node is None:
            print("🔄 Criando Organização...")
    
            self.organization_node = Node("Organization", 
                                    id = self.client.get_organization(),
                                    name=self.client.get_organization())
            
            self.sink.save_node(self.organization_node, "Organization", "id")
            