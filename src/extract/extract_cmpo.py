from extract.extract_base import ExtractBase
from typing import  Any, Dict
from py2neo import Node, Relationship
import json
from types import SimpleNamespace

class ExtractCMPO (ExtractBase):
   
    branches : Any = None
    issues: Any = None
    
    organization_node: Any = None
    
    commits: Any = None
    
    repositories: Any = None
    repositories_dict: Dict[str, Any] = {}
    
    projects: Any = None
    
    
    def model_post_init(self, __context):
        self.streams = ['repositories','projects_v2', 'commits', 'branches']
        super().model_post_init(__context)
        
    def fetch_data(self):
        self.load_data()
        
        if "repositories" in self.cache:
            self.repositories = self.cache["repositories"].to_pandas()
            print(f"✅ {len(self.repositories)} repositories carregadas.") 
        
        if "projects_v2" in self.cache:
            self.projects = self.cache["projects_v2"].to_pandas()
            print(f"✅ {len(self.projects)} projects carregadas.")
        
        if "commits" in self.cache:
            self.commits = self.cache["commits"].to_pandas()
            print(f"✅ {len(self.commits)} commits carregadas.")
            
        if "branches" in self.cache:
            self.branches = self.cache["branches"].to_pandas()
            print(f"✅ {len(self.branches)} branchs carregadas.")
            
     
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
                
            
                                     
    def __load_commits(self):
        for commit in self.commits.itertuples(index=False):
            data = self.trasnform(commit)
            data["id"] = data["sha"]+"-"+data["repository"]
            node = Node("Commit", **data)
            self.sink.save_node(node, "Commit", "id")
            
            # Criando a relaçao entre repositorio e issue
            repository_node = self.repositories_dict[commit.repository]
            self.sink.save_relationship(Relationship(repository_node, "has", node))
            print(f"🔄 Criando relacionamento entre Repositório e Issue: {commit.sha}-{commit.repository}")
            
            if commit.author:
                user = json.loads(commit.author, object_hook=lambda d: SimpleNamespace(**d))
                user_node = self.sink.get_node("Person", user.login)
                if user_node is not None:
                    self.sink.save_relationship(Relationship(node, "created_by", user_node))
                    print(f"🔄 Criando relacionamento entre Author e Commit: {user.login}-{commit.sha}")  
            
            if commit.committer:
                user = json.loads(commit.committer, object_hook=lambda d: SimpleNamespace(**d))
                user_node = self.sink.get_node("committer", user.login)
                if user_node is not None:
                    self.sink.save_relationship(Relationship(node, "created_by", user_node))
                    print(f"🔄 Criando relacionamento entre Author e Commit: {user.login}-{commit.sha}")
    
    
    def __load_branchs(self):
            print(self.branches.columns)
            for branch in self.branches.itertuples(index=False):
                data = self.trasnform(branch)
                data["id"] = data["name"]+"-"+data["repository"]
                node = Node("Branch", **data)
                self.sink.save_node(node, "Branch", "id")
                if branch.repository:
                   # Criando a relaçao entre repositorio e issue
                    repository_node = self.repositories_dict[branch.repository]
                    self.sink.save_relationship(Relationship(repository_node, "has", node))
                    print(f"🔄 Criando relacionamento entre Repositório e Branch: {branch.repository}-{branch.name}")
                    
        
         
    def load(self):
        self.fetch_data()
        self.organization_node = Node("Organization", 
                                 id =  self.client.get_organization(),
                                 name= self.client.get_organization())
        self.sink.save_node(self.organization_node, "Organization", "id")
        
        self.__load_repository()
        self.__load_repository_project()
        self.__load_branchs()
        self.__load_commits()
        
    def run(self):
        print("🔄 Extraindo dadso usando CMPO ... ")
        self.load()
        print("✅ Extração concluída com sucesso!")
        