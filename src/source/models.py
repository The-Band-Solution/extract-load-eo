from pydantic import BaseModel
from typing import List
from typing import Optional


class Member(BaseModel):
    login: str
    name: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    html_url: Optional[str] = None

class Team(BaseModel):
    name: str
    slug: str


class TeamWithMember(BaseModel):
    name: str
    slug: str
    members: List[Member]

