from extract.extract_base import ExtractBase
from typing import  Any, List
from py2neo import Node, Relationship
from sink.sink_neo4j import SinkNeo4j
from model.models import Project

class ExtractEO (ExtractBase):
   
    team_members: Any = None
    teams: Any = None
    projects: List[Project] = None
    sink: Any = None
    
    def model_post_init(self, __context):
        super().model_post_init(__context)
        self.sink = SinkNeo4j()        
        
    def fetch_data(self):
        self.team_members = self.client.get_teams_with_members()
        self.teams = self.client.get_teams()
        self.projects = self.client.get_projects()
    
    def load(self):
        
        print("🔄 Carregando dados de Equipes e Membros...")
        self.fetch_data()
        print("🔄 Criando Organização...")
    
        organization_node = Node("Organization", name=self.client.get_organization())
        self.sink.save_node(organization_node, "Organization", "name")
        
        for project in self.projects:
            project_node = Node("Project", name=project.name, id=project.id, number=project.number)
            self.sink.save_node(project_node, "Project", "id")
            
            # Create relationship between Organization and Project
            self.sink.save_relationship(Relationship(organization_node, "has", project_node))
            print("🔄 Criando Projeto...")
        
        for team in self.team_members:
            team_slug = team.slug
            team_name = team.name
            
            team_node = Node("Team", slug=team_slug, name=team_name)
            self.sink.save_node(team_node, "Team", "slug")
            print("🔄 Criando Equipe...")
        
            # Create relationship between Organization and Team
            self.sink.save_relationship(Relationship(organization_node, "has", team_node))
            
            role_node = Node("OrganizationalRole", name=team_name)
            self.sink.save_node(role_node, "OrganizationalRole", "name")
            
            for member in team.members:
                # Create Person node
                person_node = Node("Person", login=member.login, name=member.name or member.login)
                self.sink.save_node(person_node, "Person", "login")
                
                # Create TeamMember node
                team_member_node = Node("TeamMember", id=f"{member.login}-{team_slug}")
                self.sink.save_node(team_member_node, "TeamMember", "id")
                
                # TeamMembership node (mediação)
                membership_id = f"membership-{member.login}-{team_slug}"
                membership_node = Node("TeamMembership", id=membership_id)
                self.sink.save_node(membership_node, "TeamMembership", "id")

                # Relationships
                self.sink.save_relationship(Relationship(team_member_node, "is", person_node))
                self.sink.save_relationship(Relationship(membership_node, "allocates", team_member_node))
                self.sink.save_relationship(Relationship(membership_node, "is_to_play", role_node))
                self.sink.save_relationship(Relationship(membership_node, "done_for", team_node))
                
        print("✅ Dados carregados com sucesso!")
    
    def run(self):
        print("🔄 Extraindo dados de Equipes e Membros...")
        print("✅ Extração concluída com sucesso!")
        self.load()
        