from extract.extract_base import ExtractBase
from typing import  Any, List
from py2neo import Node, Relationship
from sink.sink_neo4j import SinkNeo4j
from model.models import Project

class ExtractEO (ExtractBase):
   
    team_members: Any = None
    teams: Any = None
    projects: Any = None
    team_memberships: Any = None
    users: Any = None
    sink: Any = None
    
    def model_post_init(self, __context):
        self.streams = ["projects_v2", 'teams', 'team_members', 'users', 'team_memberships']
        super().model_post_init(__context)
       
            
        self.sink = SinkNeo4j()        
        
    def fetch_data(self):
        self.load_data()
        
        if "teams" in self.cache:
            self.teams = self.cache["teams"].to_pandas()
            print(f"✅ {len(self.teams)} teams carregadas.")
        
        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            print(f"✅ {len(self.projects)} projects_v2 carregadas.")
       
        if "team_members" in self.cache:
            self.team_members = self.cache["team_members"].to_pandas()
            print(f"✅ {len(self.team_members)} team_members carregadas.")
            
        if "users" in self.cache:
            self.users = self.cache["users"].to_pandas()
            print(f"✅ {len(self.users)} users carregadas.")
        
        if "team_memberships" in self.cache:
            self.team_memberships = self.cache["team_memberships"].to_pandas()
            print(f"✅ {len(self.team_memberships)} team_memberships carregadas.")
        
        
        #self.team_members = self.client.get_teams_with_members()
        #self.teams = self.client.get_teams()
        #self.projects = self.client.get_projects()
    
    def load(self):
        
        print("🔄 Carregando dados de Equipes e Membros...")
        self.fetch_data()
        print("🔄 Criando Organização...")
    
        organization_node = Node("Organization", 
                                 id = self.client.get_organization(),
                                 name=self.client.get_organization())
        
        self.sink.save_node(organization_node, "Organization", "id")
        
        for project in self.projects:
            project_node = Node("Project", 
                                name=project.name, 
                                id=project.id, 
                                number=project.number)
            
            self.sink.save_node(project_node, "Project", "id")
            
            # Create relationship between Organization and Project
            self.sink.save_relationship(Relationship(organization_node, "has", project_node))
            print("🔄 Criando Projeto...")
        
        for team in self.team_members:
            team_slug = team.slug
            team_name = team.name
            
            team_node = Node("Team", 
                             id = f"{team_slug}-{organization_node['id']}",
                             slug=team_slug, 
                             name=team_name)
            
            self.sink.save_node(team_node, "Team", "id")
            print("🔄 Criando Equipe...")
        
            # Create relationship between Organization and Team
            self.sink.save_relationship(Relationship(organization_node, "has", team_node))
            
            role_node = Node("OrganizationalRole", 
                             id=f"{team_name}-{organization_node['id']}",
                             name=team_name)
            
            self.sink.save_node(role_node, "OrganizationalRole", "id")
            
            for member in team.members:
                # Create Person node
                person_node = Node("Person", 
                                   id=member.login,
                                   login=member.login, 
                                   name=member.name or member.login)
                
                self.sink.save_node(person_node, "Person", "id")
                
                # Create TeamMember node
                team_member_node = Node("TeamMember", 
                                        id=f"{member.login}-{team_slug}")
                
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
        self.fetch_data()
        #self.load()
        