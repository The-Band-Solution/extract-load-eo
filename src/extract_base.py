from pydantic import BaseModel, field_validator, model_validator
from typing import List, Any
import os
from dotenv import load_dotenv
import airbyte as ab

class ExtractBase(BaseModel):
    streams: List[str]
    repository: str = ""
    token: str = ""
    cache: Any = None

    def model_post_init(self, __context):
        load_dotenv()
        self.repository = os.getenv("GITHUB_REPOSITORY", "")
        self.token = os.getenv("GITHUB_TOKEN", "")
        self.fetch_data()

    def fetch_data(self):
        print(f"🔄 Buscando issues para {self.repository}...")
        try:
            source = ab.get_source(
                "source-github",
                docker_image="airbyte/source-github:latest",
                use_host_network=True,
                install_if_missing=True,
                config={
                    "repositories": [self.repository],
                    "credentials": {"personal_access_token": self.token},
                }
              
            )
           
            source.check()
            source.select_streams(self.streams)
            cache = ab.get_default_cache()
            source.read(cache=cache)
            self.cache = cache
        except Exception as e:
            print(f"❌ Erro ao buscar: {str(e)}")
