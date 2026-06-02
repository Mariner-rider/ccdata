"""Crawler and heuristic extractor for competitive-exam MCQ question banks."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from services.deep_crawler.crawler import DeepCrawler
from services.test_series.models import Question, QuestionDifficulty
from services.test_series.repository import QuestionRepository

QUESTION_SOURCES = [
    {
        "site": "testbook",
        "base_url": "https://testbook.com/previous-year-papers",
        "exam_types": ["SSC", "BANKING", "RAILWAY", "UPSC"],
    },
    {
        "site": "adda247",
        "base_url": "https://www.adda247.com/previous-year-questions",
        "exam_types": ["BANKING", "SSC", "RAILWAY"],
    },
    {
        "site": "sscadda",
        "base_url": "https://www.sscadda.com/previous-papers",
        "exam_types": ["SSC"],
    },
    {
        "site": "bankersadda",
        "base_url": "https://www.bankersadda.com/previous-year-papers",
        "exam_types": ["BANKING"],
    },
    {
        "site": "exampur",
        "base_url": "https://www.exampur.com/previous-year-papers",
        "exam_types": ["RAILWAY", "SSC", "UPSC"],
    },
]

QUESTION_START_RE = re.compile(r"(?=(?:^|\n)\s*(?:Q\s*)?\d{1,3}\s*[.)]\s+)", re.I)
OPTION_RE = re.compile(
    r"(?:^|\n|\s)(?:\(?([a-dA-D])\)|([a-dA-D])[.)])\s*(.*?)(?=(?:\n|\s)(?:\(?[a-dA-D]\)|[a-dA-D][.)])\s|\n\s*(?:Answer|Correct|Explanation|Solution)\s*:|$)",
    re.S,
)
ANSWER_RE = re.compile(r"(?:Answer|Correct(?:\s+Answer)?)\s*:\s*\(?([a-dA-D])\)?", re.I)
EXPLANATION_RE = re.compile(r"(?:Explanation|Solution)\s*:\s*(.*)", re.I | re.S)
HINDI_RE = re.compile(r"[\u0900-\u097F]")


class QuestionCrawler:
    def __init__(self, repository: QuestionRepository | None = None):
        self.repository = repository or QuestionRepository()
        self._current_exam_type = "OTHER"

    async def crawl_source(self, source: dict, exam_type: str) -> list[Question]:
        """
        Crawls the source URL for the given exam type and extracts MCQs.
        """
        self._current_exam_type = exam_type
        start_url = source["base_url"]
        if exam_type.lower() not in start_url.lower():
            start_url = urljoin(start_url.rstrip("/") + "/", exam_type.lower())
        crawler = DeepCrawler(max_pages=20, rate_limit_seconds=1.5)
        crawler._base_url = start_url
        crawler._base_netloc = __import__("urllib.parse", fromlist=["urlparse"]).urlparse(start_url).netloc.lower()
        js_required = await crawler._detect_js_required(start_url)
        html_pages: list[tuple[str, str]] = []
        for url in crawler._build_url_list(start_url):
            await asyncio.sleep(0)
            html = await (crawler._fetch_page_playwright(url) if js_required else crawler._fetch_page_httpx(url))
            if html:
                html_pages.append((url, html))
        questions: list[Question] = []
        for page_url, html in html_pages:
            questions.extend(self._extract_questions(html, page_url, source["site"]))
        return questions

    def _extract_questions(self, html: str, source_url: str, site: str) -> list[Question]:
        """Extract MCQ questions from HTML text blocks."""
        soup = BeautifulSoup(html or "", "html.parser")
        for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text("\n", strip=True)
        chunks = [chunk.strip() for chunk in QUESTION_START_RE.split(text) if chunk.strip()]
        questions: list[Question] = []
        for chunk in chunks:
            parsed = self._parse_question_chunk(chunk)
            if not parsed:
                continue
            question_text, options, correct, explanation = parsed
            if len(options) < 3 or correct not in options:
                continue
            language = "hi" if HINDI_RE.search(question_text) else "en"
            questions.append(
                Question(
                    id=None,
                    question_text=question_text,
                    option_a=options.get("a", ""),
                    option_b=options.get("b", ""),
                    option_c=options.get("c", ""),
                    option_d=options.get("d", ""),
                    correct_option=correct,
                    explanation=explanation,
                    exam_type=self._current_exam_type,
                    subject=self._auto_detect_subject(question_text),
                    topic="",
                    difficulty=self._auto_classify_difficulty(question_text),
                    source_url=source_url,
                    source_site=site,
                    year=self._extract_year(source_url),
                    language=language,
                    is_verified=False,
                    created_at=datetime.now(timezone.utc),
                )
            )
        return questions

    def _parse_question_chunk(self, chunk: str) -> tuple[str, dict[str, str], str, str] | None:
        answer_match = ANSWER_RE.search(chunk)
        if not answer_match:
            return None
        correct = answer_match.group(1).lower()
        explanation_match = EXPLANATION_RE.search(chunk)
        explanation = self._clean(explanation_match.group(1)) if explanation_match else ""
        options: dict[str, str] = {}
        for match in OPTION_RE.finditer(chunk):
            key = (match.group(1) or match.group(2) or "").lower()
            value = self._clean(match.group(3))
            value = ANSWER_RE.split(value)[0]
            value = EXPLANATION_RE.split(value)[0]
            if key and value:
                options[key] = self._clean(value)
        first_option = re.search(r"(?:\(?[a-dA-D]\)|[a-dA-D][.)])\s*", chunk)
        if not first_option:
            return None
        question_text = self._clean(chunk[: first_option.start()])
        question_text = re.sub(r"^(?:Q\s*)?\d{1,3}\s*[.)]\s*", "", question_text, flags=re.I)
        return (question_text, options, correct, explanation) if question_text else None

    @staticmethod
    def _clean(value: str) -> str:
        return " ".join(value.replace("\xa0", " ").split()).strip(" -:;\t\n")

    @staticmethod
    def _extract_year(url: str) -> int | None:
        match = re.search(r"\b(19\d{2}|20\d{2})\b", url)
        return int(match.group(1)) if match else None

    def _auto_classify_difficulty(self, question_text: str) -> QuestionDifficulty:
        """Classify difficulty using simple wording and keyword heuristics."""
        lowered = question_text.lower()
        words = lowered.split()
        easy_keywords = ("which", "what is", "who", "when", "capital of", "full form of")
        hard_keywords = (
            "calculate",
            "find the value",
            "if ",
            " then",
            "percentage change",
            "ratio of",
            "multi-step",
            "compound interest",
            "data sufficiency",
        )
        if len(words) > 40 or any(keyword in lowered for keyword in hard_keywords):
            return "hard"
        if len(words) < 15 and any(keyword in lowered for keyword in easy_keywords):
            return "easy"
        return "medium"

    def _auto_detect_subject(self, question_text: str) -> str:
        """Detect a broad subject from keyword matches."""
        lowered = question_text.lower()
        subject_keywords: dict[str, tuple[str, ...]] = {
            "Math": ("number", "calculate", "find", "percentage", "ratio", "profit", "loss", "time", "speed", "distance"),
            "GK/GA": ("president", "prime minister", "capital", "country", "year", "award", "treaty", "organisation", "organization"),
            "English": ("grammar", "synonym", "antonym", "fill in the blank", "spelling", "passage", "comprehension"),
            "Reasoning": ("series", "analogy", "odd one out", "direction", "coding", "decoding", "arrangement"),
            "Physics": ("force", "velocity", "mass", "energy", "current", "voltage", "resistance", "wave"),
            "Chemistry": ("element", "compound", "reaction", "acid", "base", "periodic table", "bond", "valence"),
            "Biology": ("cell", "organism", "disease", "vitamin", "hormone", "dna", "protein", "photosynthesis"),
        }
        for subject, keywords in subject_keywords.items():
            if any(keyword in lowered for keyword in keywords):
                return subject
        return "General"

    async def crawl_all_sources(self, exam_type: str | None = None) -> dict[str, int]:
        """Crawl all configured sources and insert non-duplicate questions."""
        results: dict[str, int] = {}
        for source in QUESTION_SOURCES:
            exam_types = [exam_type] if exam_type else source["exam_types"]
            for current_exam in exam_types:
                if current_exam not in source["exam_types"] and exam_type is not None:
                    continue
                added = 0
                for question in await self.crawl_source(source, current_exam):
                    inserted_id = await self.repository.insert(question)
                    if inserted_id:
                        added += 1
                results[source["site"]] = results.get(source["site"], 0) + added
        return results
