# state.py
from typing import Optional
from pydantic import BaseModel

class Progress(BaseModel):
    progress: int
    done: bool
    error: Optional[str] = None
    result: Optional[str] = None   # ← champ ajouté

# Le status des runs, partagé par main et worker
STATUS: dict[str, Progress] = {}