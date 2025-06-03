from extract.extract_base import ExtractBase
from typing import  Any, List
from py2neo import Node, Relationship
from sink.sink_neo4j import SinkNeo4j
from model.models import Repository

class ExtractCIRO (ExtractBase):
   
    repositories: List[Repository] = None
    sink: Any = None
    
    def model_post_init(self, __context):
        super().model_post_init(__context)
        self.sink = SinkNeo4j()        
        
    def fetch_data(self):
        self.repositories = self.client.get_repositories()
    
    def __create_milestone_node(self, milestone, repository_node):
        
        # falta colocar a datade conclusao
        milestone_node = Node("Milestone", 
                              id=f"{milestone.number}-{repository_node['id']}",	
                              number=milestone.number,
                              title=milestone.title, 
                              description=milestone.description,
                              url=milestone.url, 
                              state=milestone.state, 
                              open_issues=milestone.open_issues, 
                              closed_issues=milestone.closed_issues,
                              due_on=milestone.due_on)
        
        self.sink.save_node(milestone_node, "Milestone", "number")
        print(f"🔄 Criando Milestone: {milestone.title}")
        
        self.sink.save_relationship(Relationship(repository_node, "has", milestone_node))
        print(f"🔄 Criando relacionamento entre Repositório e Milestone: {milestone.title}")
        
    def __create_issue_node(self, issue, repository_node):
        ## Falta associar ao milestone, a pessoa e projetos
        issue_node = Node("Issue", 
                        id =f"{issue.number}-{repository_node['id']}",
                        number=issue.number,
                        description=issue.description,
                        title=issue.title,
                        url=issue.url,
                        state=issue.state,
                        created_at=issue.created_at,
                        closed_at=issue.closed_at)
                        
        self.sink.save_node(issue_node, "Issue", "id")
        print(f"🔄 Criando Issue: {issue.title}")
        self.sink.save_relationship(Relationship(repository_node, "has", issue_node))
        print(f"🔄 Criando relacionamento entre Repositório e Issue: {issue.title}")
        
        if issue.assignees:
            print(f"🔄 Associando Issue {issue.title} a Assignees")
        if issue.author:
            print(f"🔄 Associando Issue {issue.title} ao Autor: {issue.author.login}")
            
        if issue.milestone:
            print(f"🔄 Associando Issue {issue.title} ao Milestone: {issue.milestone.title}")
        if issue.projects:
            print(f"🔄 Associando Issue {issue.title} a Projetos")
    
    def load(self):
        self.fetch_data()
        organization_node = Node("Organization", 
                                 id = self.client.get_organization(),
                                 name=self.client.get_organization())
        self.sink.save_node(organization_node, "Organization", "id")
        
        for repository in self.repositories:
            repository_node = Node("Repository",
                                   id =f"{repository.full_name}", 
                                   name=repository.name, 
                                   full_name=repository.full_name, 
                                   html_url=repository.html_url)    
            self.sink.save_node(repository_node, "Repository", "id")
            print(f"✅ Repositório {repository.name} adicionado")
            
            self.sink.save_relationship(Relationship(organization_node, "has", repository_node))
            print(f"🔄 Criando relacionamento entre Organização e Repositório: {repository.name}")
            
            milestones = self.client.get_milestones(repository.full_name)
            
            for milestone in milestones:
               self.__create_milestone_node(milestone, repository_node)
            
            issues = self.client.get_issues(repository.full_name)
            for issue in issues:
                self.__create_issue_node(issue, repository_node)
                
            print(f"🔄 Repositório {repository.name} processado com sucesso!")
               
    def run(self):
        print("🔄 Extraindo dados de Repositorios...")
        print("✅ Extração concluída com sucesso!")
        self.load()