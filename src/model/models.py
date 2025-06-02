from pydantic import BaseModel
from typing import List
from typing import Optional

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


