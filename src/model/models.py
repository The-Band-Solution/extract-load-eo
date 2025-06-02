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
    title: str
    description: Optional[str]
    state: str
    due_on: Optional[datetime]
    open_issues: int
    closed_issues: int
    url: str


class Issue(BaseModel):
    number: int
    title: str
    url: str
    state: str
    assignees: Optional[List[Member]]
    author: Optional[Member] = None  # 👈 Adicionado aqui
    created_at: datetime
    closed_at: Optional[datetime] = None
    milestone: Optional[Milestone] = None
    projects: List[Project] = []
    
class Repository(BaseModel):
    name: str
    full_name: str
    html_url: str
    issues: Optional[List[Issue]]
    milestones: Optional[List[Milestone]]