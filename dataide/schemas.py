from typing import List, Dict, Optional

from pydantic import BaseModel


class ColumnMeta(BaseModel):
    name: str
    dtype: str
    description: Optional[str] = None
    nullable: bool = True
    pii: Optional[str] = "none"
    semantics: Optional[str] = None


class TableSchema(BaseModel):
    name: str
    description: Optional[str] = None
    primary_key: Optional[List[str]] = None
    foreign_keys: Optional[List[Dict[str, str]]] = None
    columns: List[ColumnMeta]


class SuggestedChart(BaseModel):
    title: str
    kind: str
    table: str
    x: str
    y: Optional[str] = None
    note: Optional[str] = None


class DataIDEPayload(BaseModel):
    dataset_description: str
    tables: List[TableSchema]
    mermaid_erd: str
    sample_rows: Dict[str, List[Dict[str, object]]]
    profiling_summary: Dict[str, Dict[str, object]]
    suggested_charts: List[SuggestedChart]
    caveats: Optional[List[str]] = None


