from pydantic import BaseModel
from typing import Optional, Dict


class AnnotationCreate(BaseModel):
    paper_id: int
    paragraph_id: str | None = None
    coordinates: dict | None = None
    content: str


class AnnotationOut(BaseModel):
    id: int
    paper_id: int
    author_id: int
    paragraph_id: Optional[str] = None
    coordinates: Optional[Dict[str, float]] = None
    content: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    class Config:
        orm_mode = True