from services.test_series.crawler import QuestionCrawler


def test_question_extractor_mcq_patterns():
    html = """
    <p>Q1. What is the capital of India?</p>
    <p>(a) Mumbai (b) Delhi (c) Kolkata (d) Chennai</p>
    <p>Answer: (b)</p>
    <p>Explanation: Delhi is the capital of India.</p>
    """
    crawler = QuestionCrawler()
    crawler._current_exam_type = "SSC"
    questions = crawler._extract_questions(html, "https://example.com/ssc-2024", "fixture")
    assert len(questions) == 1
    assert questions[0].correct_option == "b"
    assert questions[0].subject == "GK/GA"
    assert questions[0].year == 2024


def test_difficulty_and_subject_heuristics():
    crawler = QuestionCrawler()
    assert crawler._auto_classify_difficulty("Who is the president?") == "easy"
    assert crawler._auto_classify_difficulty("Calculate the percentage change if the ratio of two values changes over three steps") == "hard"
    assert crawler._auto_detect_subject("Find the ratio of profit and loss") == "Math"
    assert crawler._auto_detect_subject("Choose the correct synonym") == "English"
