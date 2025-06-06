from extract.extract_base import ExtractBase
from typing import  Any, Dict
from py2neo import Node, Relationship
from sink.sink_neo4j import SinkNeo4j
import json
from types import SimpleNamespace

class ExtractCIRO (ExtractBase):
   
    
    milestones: Any = None
    milestones_dict: Dict[str, Any] = {}
    
    issues: Any = None
    pull_request_commits: Any = None
    issue_labels: Any = None
    organization_node: Any = None
    
    repositories: Any = None
    repositories_dict: Dict[str, Any] = {}
    
    projects: Any = None
    
    
    def model_post_init(self, __context):
        #self.streams = [  ,  'pull_request_commits','issue_labels','repositories','projects_v2']
        self.streams = ['repositories','projects_v2','issue_milestones','issues']
        super().model_post_init(__context)
        
    def fetch_data(self):
        self.load_data()
        
        if "issue_milestones" in self.cache:
            self.milestones = self.cache["issue_milestones"].to_pandas()
            print(f"✅ {len(self.milestones)} issue_milestones carregadas.")
        
        if "issues" in self.cache:
            self.issues = self.cache["issues"].to_pandas()
            print(f"✅ {len(self.issues)} issues carregadas.")
       
        if "pull_request_commits" in self.cache:
            self.pull_request_commits = self.cache["pull_request_commits"].to_pandas()
            print(f"✅ {len(self.pull_request_commits)} pull_request_commits carregadas.")
        
        if "issue_labels" in self.cache:
            self.issue_labels = self.cache["issue_labels"].to_pandas()
            print(f"✅ {len(self.issue_labels)} issue_labels carregadas.")
       
        if "repositories" in self.cache:
            self.repositories = self.cache["repositories"].to_pandas()
            print(f"✅ {len(self.repositories)} repositories carregadas.") 
        
        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            print(f"✅ {len(self.projects)} projects carregadas.")
       
     
    def __load_repository(self):
        
        for repository in self.repositories.itertuples():
            data = self.trasnform(repository)
            repository_node = Node("Repository",**data)
            
            self.sink.save_node(repository_node, "Repository", "id")
            print(f"✅ Repositório {repository.name} adicionado")
            
            self.sink.save_relationship(Relationship(self.organization_node, "has", repository_node))
            print(f"🔄 Criando relacionamento entre Organização e Repositório: {repository.name}")
            
            self.repositories_dict[repository.full_name] = repository_node
    
    def __load_repository_project(self):
           
        for project in self.projects.itertuples():
            
            if project.repository in self.repositories_dict:
                repository_node = self.repositories_dict[project.repository]
                project_node = self.sink.get_node("Project",project.id)
                
                if repository_node and project_node:
                    self.sink.save_relationship(Relationship(project_node, "has", repository_node))
                    print(f"🔄 Criando relacionamento entre Repositorio e Projeto: {project.title}--{project.repository}")
                
    def __load_milestones (self):
        
        for milestone in self.milestones.itertuples(index=False):
            data = self.trasnform(milestone)
            milestone_node = Node("Milestone", **data)
            
            repository_node = self.repositories_dict[milestone.repository]
            self.sink.save_node(milestone_node, "Milestone", "id")
            print(f"🔄 Criando Milestone: {milestone.title}")
            
            self.milestones_dict[milestone.id] = milestone_node
            
            self.sink.save_relationship(Relationship(repository_node, "has", milestone_node))
            print(f"🔄 Criando relacionamento entre Repositório e Milestone: {milestone.title}--{milestone.repository}")
            
    def __load_issue(self):
        
        for issue in self.issues.itertuples(index=False):
            data = self.trasnform(issue)
            print (data)
            
            node = Node("Issue", **data)
            self.sink.save_node(node, "Issue", "id")
            print(f"🔄 Criando Issue: {issue.title}")
            
            # Criando a relaçao entre repositorio e issue
            repository_node = self.repositories_dict[issue.repository]
            self.sink.save_relationship(Relationship(repository_node, "has", node))
            print(f"🔄 Criando relacionamento entre Repositório e Issue: {issue.title}-{issue.repository}")
            
            if issue.milestone:
                milestone = json.loads(issue.milestone, object_hook=lambda d: SimpleNamespace(**d))
                if milestone.id in self.milestones_dict:
                    milestone_node = self.milestones_dict[milestone.id]
                    self.sink.save_relationship(Relationship(milestone_node, "has", node))
                    print(f"🔄 Criando relacionamento entre Milestone e Issue: {issue.title}-{milestone.id}")
            
            if issue.user:
                user = json.loads(issue.user, object_hook=lambda d: SimpleNamespace(**d))
                user_node = self.sink.get_node("Person", user.login)
                self.sink.save_relationship(Relationship(node, "created_by", user_node))
                print(f"🔄 Criando relacionamento entre User e Issue: {user.login}-{issue.title}")                
            
            if issue.assignee:
                user = json.loads(issue.assignee, object_hook=lambda d: SimpleNamespace(**d))
                user_node = self.sink.get_node("Person", user.login)
                self.sink.save_relationship(Relationship(node, "assigneed_by", user_node))
                print(f"🔄 Criando relacionamento entre Assignee e Issue: {user.login}-{issue.title}")  
            
            if issue.assignees:
                print (issue.assignees)
                for assignee in issue.assignees:
                    user = json.loads(assignee, object_hook=lambda d: SimpleNamespace(**d))
                    user_node = self.sink.get_node("Person", user.login)
                    self.sink.save_relationship(Relationship(node, "assigneed_by", user_node))
                    print(f"🔄 Criando relacionamento entre Assignees e Issue: {user.login}-{issue.title}")  
                            
                
           
    def load(self):
        self.fetch_data()
        self.organization_node = Node("Organization", 
                                 id =  self.client.get_organization(),
                                 name= self.client.get_organization())
        self.sink.save_node(self.organization_node, "Organization", "id")
        
        self.__load_repository()
        self.__load_repository_project()
        self.__load_milestones()
        self.__load_issue()
        
        
    def run(self):
        print("🔄 Extraindo dados de Repositorios...")
        self.load()
        print("✅ Extração concluída com sucesso!")
        