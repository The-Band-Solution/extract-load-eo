from extract.extract_base import ExtractBase
from typing import  Any
from py2neo import Node, Relationship
from sink.sink_neo4j import SinkNeo4j
class ExtractEO (ExtractBase):
   
    team_members: Any = None
    teams: Any = None
    sink: Any = None
    
    def model_post_init(self, __context):
        super().model_post_init(__context)
        self.sink = SinkNeo4j()        
        self.team_members = self.cache["team_members"]
        self.teams = self.cache["teams"]
        
    def fetch_data(self):
       
        self.cache = {
            "team_members": self.client.get_teams_with_members(),
            "teams": self.client.get_teams()
        }

    
    def load(self):
        print("🔄 Carregando dados de Equipes e Membros...")
        for team_members in self.team_members:
            team_slug = team_members.slug
            team_name = team_members.name
            team_node = Node("Team", slug=team_slug, name=team_name)
            self.sink.save(team_node, "Team", "slug")
            
            role_node = Node("OrganizationalRole", name=team_name)
            self.sink.save(role_node, "OrganizationalRole", "name")
            
            for member in team_members.members:
                # Create Person node
                person_node = Node("Person", login=member.login, name=member.name or member.login)
                self.sink.save(person_node, "Person", "login")
                
                # Create TeamMember node
                team_member_node = Node("TeamMember", id=f"{member.login}-{team_slug}")
                self.sink.save(team_member_node, "TeamMember", "id")
                
                # TeamMembership node (mediação)
                membership_id = f"membership-{member.login}-{team_slug}"
                membership_node = Node("TeamMembership", id=membership_id)
                self.sink.save(membership_node, "TeamMembership", "id")

                # Relationships
                self.sink.save(Relationship(team_member_node, "is", person_node))
                self.sink.save(Relationship(membership_node, "allocates", team_member_node))
                self.sink.save(Relationship(membership_node, "is_to_play", role_node))
                self.sink.save(Relationship(membership_node, "done_for", team_node))
            
        print("✅ Dados carregados com sucesso!")
    
    def run(self):
        print("🔄 Extraindo dados de Equipes e Membros...")
        print(f"📊 Total de Membros: {len(self.team_members)}")
        print(f"📊 Total de Equipes: {len(self.teams)}")
        print("✅ Extração concluída com sucesso!")
        self.load()
        