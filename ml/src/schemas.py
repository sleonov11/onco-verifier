from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, List
from enum import Enum


class Role(str, Enum):
    doctor = "doctor"
    patient = "patient"


class Locale(str, Enum):
    ru = "ru"
    en = "en"


class Diagnosis(BaseModel):
    name: str
    icd10: Optional[str] = None
    stage: Optional[str] = None
    tnm: Optional[str] = None
    histology: Optional[str] = None


class MolecularMarkers(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    EGFR: Optional[str] = None
    ALK: Optional[str] = None
    PD_L1: Optional[str] = Field(default=None, alias="PD-L1")


class PatientContext(BaseModel):
    age: Optional[int] = None
    sex: Optional[str] = None
    comorbidities: Optional[List[str]] = []
    symptoms: Optional[List[str]] = []


class Treatment(BaseModel):
    therapy_line: int
    proposed_regimen: List[str]
    cycle: Optional[str] = None
    notes: Optional[str] = None


class FreeText(BaseModel):
    doctor_notes: Optional[str] = ""
    patient_message: Optional[str] = ""


class InputData(BaseModel):
    diagnosis: Diagnosis
    molecular_markers: Optional[MolecularMarkers] = None
    patient_context: PatientContext
    treatment: Treatment
    free_text: Optional[FreeText] = None


class Options(BaseModel):
    guideline_scope: Optional[str] = "ru_minzdrav"
    explain_level: Optional[str] = "detailed"
    return_sources: bool = True
    safety_mode: Optional[str] = "strict"


class CheckRequest(BaseModel):
    request_id: str
    received_at: Optional[str] = None
    role: Role
    locale: Locale = Locale.ru
    input: InputData
    options: Options = Options()


class Issue(BaseModel):
    code: str
    severity: str
    title: str
    details: str
    suggested_action: Optional[str] = None
    confidence: float


class Source(BaseModel):
    source: str
    section: Optional[str] = None
    text: Optional[str] = None
    doc_id: Optional[str] = None


class Result(BaseModel):
    is_compliant: bool
    summary: Optional[str] = ""
    issues: List[Issue] = []
    doctor_explanation: str
    patient_explanation: str
    recommended_next_steps: List[str] = []


class CheckResponse(BaseModel):
    request_id: str
    status: str = "ok"
    result: Result
    sources: List[Source] = []
    warnings: List[str] = []

class ChatMessage(BaseModel):
    role: str  # "user" или "assistant"
    content: str

class ChatRequest(BaseModel):
    request_id: str
    message: str
    role: Role
    history: List[ChatMessage] = []
    context: Optional[Dict] = None

class ChatResponse(BaseModel):
    request_id: str
    message: str
    history: List[ChatMessage]