from pydantic import BaseModel
from typing import List
from typing import Optional
from datetime import datetime

class Project(BaseModel):
    name: str
    id: str
    number: int

class Member(BaseModel):
    login: str
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    html_url: Optional[str] = None

class Team(BaseModel):
    name: str
    slug: str

class TeamMembership(BaseModel):
    team: Team
    members: List[Member]
    name: str
    slug: str

class Milestone(BaseModel):
    number: int
    title: str
    description: Optional[str]
    state: str
    due_on: Optional[str] = None  # Formato ISO 8601
    open_issues: int
    closed_issues: int
    url: str
    creator: Optional[str] = None
    created_at: Optional[str] = None
    closed_at: Optional[str] = None
    update_at: Optional[str] = None



class Issue(BaseModel):
    number: int
    title: str
    description: Optional[str] = None
    url: str
    state: str
    assignees: Optional[List[Member]]
    author: Optional[Member] = None  # 👈 Adicionado aqui
    created_at: str
    closed_at: Optional[str] = None
    milestone: Optional[Milestone] = None
    repository: Optional[str] = None  # Nome do repositório
    projects: List[Project] = []
    
class Repository(BaseModel):
    name: str
    full_name: str
    html_url: str
    issues: Optional[List[Issue]]
    milestones: Optional[List[Milestone]]