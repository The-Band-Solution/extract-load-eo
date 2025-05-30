from extract_base import ExtractBase
from typing import List, Any


class ExtractEO (ExtractBase):
    streams: List[str] = ["team_members","teams"]
    team_members: Any = None
    teams: Any = None
    
    def model_post_init(self, __context):
        super().model_post_init(__context)
        self.team_members = self.cache["team_members"].to_pandas()
        self.teams = self.cache["teams"].to_pandas()
    
    def load(self):
        print("🔄 Carregando dados de Equipes e Membros...")
        
        print("✅ Dados carregados com sucesso!")
    
    def run(self):
        print("🔄 Extraindo dados de Equipes e Membros...")
        print(f"📊 Total de Membros: {len(self.team_members)}")
        print(f"📊 Total de Equipes: {len(self.teams)}")
        print("✅ Extração concluída com sucesso!")
        self.load()
        