"""PostgreSQL repositories for question bank and test series data."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime
from typing import Any

from services.test_series.models import Question, TestSeries

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional postgres dependency
    psycopg = None
    dict_row = None


def _database_url(database_url: str | None = None) -> str:
    return database_url or os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN") or ""


def question_hash(question_text: str) -> str:
    return hashlib.sha256(" ".join(question_text.split()).lower().encode()).hexdigest()


def _require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is required for test series repositories")


def _question_from_row(row: dict[str, Any]) -> Question:
    return Question(
        id=row.get("id"),
        question_text=row.get("question_text") or "",
        option_a=row.get("option_a") or "",
        option_b=row.get("option_b") or "",
        option_c=row.get("option_c") or "",
        option_d=row.get("option_d") or "",
        correct_option=(row.get("correct_option") or "a").lower(),
        explanation=row.get("explanation") or "",
        exam_type=row.get("exam_type") or "OTHER",
        subject=row.get("subject") or "General",
        topic=row.get("topic") or "",
        difficulty=row.get("difficulty") or "medium",
        source_url=row.get("source_url") or "",
        source_site=row.get("source_site") or "",
        year=row.get("year"),
        language=row.get("language") or "en",
        is_verified=bool(row.get("is_verified")),
        created_at=row.get("created_at") if isinstance(row.get("created_at"), datetime) else None,
    )


def _test_from_row(row: dict[str, Any]) -> TestSeries:
    return TestSeries(
        id=row.get("id"),
        name=row.get("name") or "",
        exam_type=row.get("exam_type") or "OTHER",
        description=row.get("description") or "",
        total_questions=int(row.get("total_questions") or 0),
        duration_minutes=int(row.get("duration_minutes") or 60),
        created_by=row.get("created_by") or "admin",
        is_active=bool(row.get("is_active")),
        created_at=row.get("created_at") if isinstance(row.get("created_at"), datetime) else None,
    )


class QuestionRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = _database_url(database_url)

    async def _connect(self):
        _require_psycopg()
        if not self.database_url:
            raise RuntimeError("DATABASE_URL or POSTGRES_DSN is required")
        return await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row)

    async def insert(self, q: Question) -> int:
        """Insert a question and return its id, silently reusing duplicates."""
        q_hash = question_hash(q.question_text)
        sql = """
            INSERT INTO question_bank (
                question_text, question_hash, option_a, option_b, option_c, option_d,
                correct_option, explanation, exam_type, subject, topic, difficulty,
                source_url, source_site, year, language, is_verified
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (question_hash) DO NOTHING
            RETURNING id
        """
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    sql,
                    (
                        q.question_text,
                        q_hash,
                        q.option_a,
                        q.option_b,
                        q.option_c,
                        q.option_d,
                        q.correct_option,
                        q.explanation,
                        q.exam_type,
                        q.subject,
                        q.topic,
                        q.difficulty,
                        q.source_url,
                        q.source_site,
                        q.year,
                        q.language,
                        q.is_verified,
                    ),
                )
                row = await cur.fetchone()
                if row:
                    return int(row["id"])
                await cur.execute("SELECT id FROM question_bank WHERE question_hash = %s", (q_hash,))
                existing = await cur.fetchone()
                return int(existing["id"]) if existing else 0

    async def get_practice_questions(
        self,
        exam_type: str,
        subject: str | None,
        difficulty: str,
        count: int,
        exclude_ids: list[int],
    ) -> list[Question]:
        sql = """
            SELECT * FROM question_bank
            WHERE exam_type = %s
              AND difficulty = %s
              AND (%s::text IS NULL OR subject = %s)
              AND NOT (id = ANY(%s::int[]))
            ORDER BY RANDOM()
            LIMIT %s
        """
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, (exam_type, difficulty, subject, subject, exclude_ids or [], count))
                rows = await cur.fetchall()
        return [_question_from_row(row) for row in rows]

    async def get_for_test(self, exam_type: str, difficulty_mix: dict, exclude_ids: list[int]) -> list[Question]:
        questions: list[Question] = []
        for difficulty, count in difficulty_mix.items():
            if int(count or 0) <= 0:
                continue
            questions.extend(
                await self.get_practice_questions(exam_type, None, difficulty, int(count), exclude_ids + [q.id for q in questions if q.id])
            )
        return questions

    async def mark_verified(self, question_id: int) -> None:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute("UPDATE question_bank SET is_verified = TRUE WHERE id = %s", (question_id,))

    async def count_by_exam(self, exam_type: str | None = None) -> dict:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                if exam_type:
                    await cur.execute("SELECT difficulty, COUNT(*) AS count FROM question_bank WHERE exam_type = %s GROUP BY difficulty", (exam_type,))
                    rows = await cur.fetchall()
                    await cur.execute("SELECT COUNT(*) AS count FROM question_bank WHERE exam_type = %s", (exam_type,))
                    total = await cur.fetchone()
                    return {"exam_type": exam_type, "total": int(total["count"]), "by_difficulty": {row["difficulty"]: int(row["count"]) for row in rows}}
                await cur.execute("SELECT exam_type, COUNT(*) AS count FROM question_bank GROUP BY exam_type ORDER BY exam_type")
                rows = await cur.fetchall()
        return {row["exam_type"]: int(row["count"]) for row in rows}

    async def list_unverified(self, limit: int = 50) -> list[Question]:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM question_bank WHERE is_verified = FALSE ORDER BY created_at DESC LIMIT %s", (limit,))
                rows = await cur.fetchall()
        return [_question_from_row(row) for row in rows]

    async def list_questions(self, exam_type: str | None = None, difficulty: str | None = None, unverified: bool = False, limit: int = 100) -> list[Question]:
        clauses = []
        params: list[Any] = []
        if exam_type:
            clauses.append("exam_type = %s")
            params.append(exam_type)
        if difficulty:
            clauses.append("difficulty = %s")
            params.append(difficulty)
        if unverified:
            clauses.append("is_verified = FALSE")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT * FROM question_bank {where} ORDER BY created_at DESC LIMIT %s", (*params, limit))
                rows = await cur.fetchall()
        return [_question_from_row(row) for row in rows]

    async def get(self, question_id: int) -> Question | None:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM question_bank WHERE id = %s", (question_id,))
                row = await cur.fetchone()
        return _question_from_row(row) if row else None

    async def get_student_history(self, student_id: str) -> list[int]:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT question_id FROM student_question_history WHERE student_id = %s", (student_id,))
                rows = await cur.fetchall()
        return [int(row["question_id"]) for row in rows]

    async def record_student_history(self, student_id: str, question_ids: list[int]) -> None:
        if not question_ids:
            return
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                for question_id in question_ids:
                    await cur.execute(
                        "INSERT INTO student_question_history(student_id, question_id) VALUES(%s, %s) ON CONFLICT DO NOTHING",
                        (student_id, question_id),
                    )

    async def record_attempt(self, student_id: str, test_id: int, score: int, total_marks: int, answers: dict[str, str]) -> int:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO student_attempts(student_id, test_id, submitted_at, score, total_marks, answers)
                    VALUES(%s, %s, NOW(), %s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (student_id, test_id, score, total_marks, __import__("json").dumps(answers)),
                )
                row = await cur.fetchone()
        return int(row["id"])


class TestSeriesRepository:
    def __init__(self, database_url: str | None = None):
        self.database_url = _database_url(database_url)

    async def _connect(self):
        _require_psycopg()
        if not self.database_url:
            raise RuntimeError("DATABASE_URL or POSTGRES_DSN is required")
        return await psycopg.AsyncConnection.connect(self.database_url, row_factory=dict_row)

    async def create(self, t: TestSeries) -> int:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO test_series(name, exam_type, description, total_questions, duration_minutes, created_by, is_active)
                    VALUES(%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (t.name, t.exam_type, t.description, t.total_questions, t.duration_minutes, t.created_by, t.is_active),
                )
                row = await cur.fetchone()
        return int(row["id"])

    async def list_active(self) -> list[TestSeries]:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM test_series WHERE is_active = TRUE ORDER BY created_at DESC")
                rows = await cur.fetchall()
        return [_test_from_row(row) for row in rows]

    async def get(self, test_id: int) -> TestSeries | None:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT * FROM test_series WHERE id = %s", (test_id,))
                row = await cur.fetchone()
        return _test_from_row(row) if row else None

    async def add_questions(self, test_id: int, question_ids: list[int]) -> None:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                for order, question_id in enumerate(question_ids, start=1):
                    await cur.execute(
                        """
                        INSERT INTO test_questions(test_id, question_id, question_order)
                        VALUES(%s, %s, %s)
                        ON CONFLICT (test_id, question_id) DO UPDATE SET question_order = EXCLUDED.question_order
                        """,
                        (test_id, question_id, order),
                    )
                await cur.execute("UPDATE test_series SET total_questions = %s WHERE id = %s", (len(question_ids), test_id))

    async def get_questions(self, test_id: int) -> list[Question]:
        async with await self._connect() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT qb.*
                    FROM test_questions tq
                    JOIN question_bank qb ON qb.id = tq.question_id
                    WHERE tq.test_id = %s
                    ORDER BY tq.question_order, qb.id
                    """,
                    (test_id,),
                )
                rows = await cur.fetchall()
        return [_question_from_row(row) for row in rows]
