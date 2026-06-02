"""Data models for practice questions and test series."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

QuestionDifficulty = Literal["easy", "medium", "hard"]
ExamType = Literal[
    "NDA",
    "JEE",
    "NEET",
    "UPSC",
    "BANKING",
    "RAILWAY",
    "SSC",
    "STATE",
    "OTHER",
]


@dataclass
class Question:
    id: int | None
    question_text: str
    option_a: str
    option_b: str
    option_c: str
    option_d: str
    correct_option: Literal["a", "b", "c", "d"]
    explanation: str
    exam_type: ExamType
    subject: str
    topic: str
    difficulty: QuestionDifficulty
    source_url: str
    source_site: str
    year: int | None
    language: Literal["en", "hi"]
    is_verified: bool
    created_at: datetime | None


@dataclass
class TestSeries:
    id: int | None
    name: str
    exam_type: ExamType
    description: str
    total_questions: int
    duration_minutes: int
    created_by: str
    is_active: bool
    created_at: datetime | None
