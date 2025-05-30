from extract.extract_base import ExtractBase
from typing import List, Any
from py2neo import Graph, Node, Relationship
import os
from dotenv import load_dotenv

class ExtractEO (ExtractBase):
    graph : Any = None
    team_members: Any = None
    teams: Any = None
    
    def model_post_init(self, __context):
        super().model_post_init(__context)
        load_dotenv()
        self.team_members = self.cache["team_members"]
        self.teams = self.cache["teams"]
        self.graph = Graph(os.getenv("NEO4J_URI", ""), 
                           auth=(os.getenv("NEO4J_USERNAME", ""),
                                 os.getenv("NEO4J_PASSWORD", "")))
    
    def load(self):
        print("🔄 Carregando dados de Equipes e Membros...")
        for team_members in self.team_members:
            team_slug = team_members.slug
            team_name = team_members.name
            team_node = Node("Team", slug=team_slug, name=team_name)
            self.graph.merge(team_node, "Team", "slug")
            
            role_node = Node("OrganizationalRole", name=team_name)
            self.graph.merge(role_node, "OrganizationalRole", "name")
            
            for member in team_members.members:
                # Create Person node
                person_node = Node("Person", login=member.login, name=member.name or member.login)
                self.graph.merge(person_node, "Person", "login")
                
                # Create TeamMember node
                team_member_node = Node("TeamMember", id=f"{member.login}-{team_slug}")
                self.graph.merge(team_member_node, "TeamMember", "id")
                
                # TeamMembership node (mediação)
                membership_id = f"membership-{member.login}-{team_slug}"
                membership_node = Node("TeamMembership", id=membership_id)
                self.graph.merge(membership_node, "TeamMembership", "id")

                # Relationships
                self.graph.merge(Relationship(person_node, "allocates", team_member_node))
                self.graph.merge(Relationship(membership_node, "allocates", team_member_node))
                self.graph.merge(Relationship(membership_node, "is_to_play", role_node))
                self.graph.merge(Relationship(membership_node, "done_for", team_node))
            
        print("✅ Dados carregados com sucesso!")
    
    def run(self):
        print("🔄 Extraindo dados de Equipes e Membros...")
        print(f"📊 Total de Membros: {len(self.team_members)}")
        print(f"📊 Total de Equipes: {len(self.teams)}")
        print("✅ Extração concluída com sucesso!")
        self.load()
        