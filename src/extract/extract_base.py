from pydantic import BaseModel
from typing import  Any
import os
import pandas as pd
from dotenv import load_dotenv
import airbyte as ab
from source.github_client import GitHubClient
from sink.sink_neo4j import SinkNeo4j
class ExtractBase(BaseModel):   
    organization: str = ""
    token: str = ""
    client: Any = None
    streams: list[str] = []
    cache: Any = None
    source: Any = None
    sink: Any = None
    
    def model_post_init(self, __context):
        load_dotenv()
        
        self.sink = SinkNeo4j()  
        self.organization = os.getenv("GITHUB_ORG", "")
        self.token = os.getenv("GITHUB_TOKEN", "")
        self.client = GitHubClient(self.token, self.organization)
        
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
            