from extract.extract_base import ExtractBase
from typing import  Any, List, Dict
from py2neo import Node, Relationship
from sink.sink_neo4j import SinkNeo4j
from model.models import Project

class ExtractEO (ExtractBase):
   
    team_members: Any = None
    
    teams: Any = None
    teams_dict: Dict[str, Any] = {}
    
    projects: Any = None
    projects_dict: Dict[str, Any] = {}
    
    team_memberships: Any = None
    users: Any = None
    sink: Any = None
    
    def model_post_init(self, __context):
        self.streams = ["projects_v2", 'teams', 'team_members']
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
            
        
        
    def __load_organization(self):
        
        print("🔄 Criando Organização...")
    
        organization_node = Node("Organization", 
                                 id = self.client.get_organization(),
                                 name=self.client.get_organization())
        
        self.sink.save_node(organization_node, "Organization", "id")
        
        return organization_node 
    
    def __load_project(self, organization_node):
        
        for project in self.projects.itertuples():
            
            if project.id not in self.projects_dict:
                
                print(f"🔄 Criando Projeto... {project.id} -{project.title} - {project.repository}")
                 
                project_node = Node("Project", 
                                    name=project.title, 
                                    short_description=project.short_description,
                                    url=project.url,
                                    id=project.id, 
                                    number=project.number)
                
                self.projects_dict[project.id] = project_node
                    
            self.sink.save_node(project_node, "Project", "id")
            
            # Create relationship between Organization and Project
            self.sink.save_relationship(Relationship(organization_node, "has", project_node))
            # relacionar os projetos os repositorios
    
    def __load_team_member(self, organization_node):
        
        for member in self.team_members.itertuples():
            # Create Person node
            person_node = Node("Person", 
                                id=member.login,
                                login=member.login)
            
            self.sink.save_node(person_node, "Person", "id")
            
            if member.team_slug:
                
                # Create TeamMember node
                team_member_node = Node("TeamMember", 
                                        id=f"{member.login}-{member.team_slug}")
                
                self.sink.save_node(team_member_node, "TeamMember", "id")    
                
                team_node = self.teams_dict[member.team_slug]
                self.sink.save_relationship(Relationship(team_member_node, "done_for", team_node))
            
            # Relationships
            self.sink.save_relationship(Relationship(team_member_node, "is", person_node))
            self.sink.save_relationship(Relationship(person_node, "present_in", organization_node))
            
            
    def __load_team(self, organization_node):
        
        for team in self.teams.itertuples():  
              
            if team.id not in self.teams_dict:
                
                team_node = Node("Team", 
                                id = team.id,
                                slug=team.slug, 
                                name=team.name)
                
                self.sink.save_node(team_node, "Team", "id")
                print(f"🔄 Criando Equipe... {team.name}")
                self.teams_dict[team.slug] = team_node
                
                # Create relationship between Organization and Team
                self.sink.save_relationship(Relationship(organization_node, "has", team_node))
                print(f"🔄 Criando Relação entre ... {team.name} .. {organization_node}")
                
        
    def load(self):
        
        print("🔄 Carregando dados de Equipes e Membros...")
        self.fetch_data()
        
        organization_node = self.__load_organization()
        self.__load_project(organization_node=organization_node)
        self.__load_team(organization_node=organization_node)
        self.__load_team_member(organization_node=organization_node)
                
        print("✅ Dados carregados com sucesso!")
    
    def run(self):
        print("🔄 Extraindo dados de Equipes e Membros...")
        self.fetch_data()
        self.load()
        print("✅ Extração concluída com sucesso!")
        
        