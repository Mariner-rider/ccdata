CREATE TABLE IF NOT EXISTS question_bank (
  id                SERIAL PRIMARY KEY,
  question_text     TEXT NOT NULL,
  question_hash     VARCHAR(64) UNIQUE NOT NULL,
  option_a          TEXT NOT NULL,
  option_b          TEXT NOT NULL,
  option_c          TEXT NOT NULL,
  option_d          TEXT NOT NULL,
  correct_option    CHAR(1) NOT NULL CHECK (correct_option IN ('a','b','c','d')),
  explanation       TEXT DEFAULT '',
  exam_type         VARCHAR(20) NOT NULL,
  subject           VARCHAR(50) DEFAULT 'General',
  topic             VARCHAR(100) DEFAULT '',
  difficulty        VARCHAR(10) NOT NULL CHECK (difficulty IN ('easy','medium','hard')),
  source_url        TEXT,
  source_site       VARCHAR(50),
  year              INT,
  language          VARCHAR(5) DEFAULT 'en',
  is_verified       BOOLEAN DEFAULT FALSE,
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qb_exam_difficulty
  ON question_bank(exam_type, difficulty);
CREATE INDEX IF NOT EXISTS idx_qb_subject
  ON question_bank(subject);
CREATE INDEX IF NOT EXISTS idx_qb_verified
  ON question_bank(is_verified);

CREATE TABLE IF NOT EXISTS test_series (
  id                SERIAL PRIMARY KEY,
  name              TEXT NOT NULL,
  exam_type         VARCHAR(20) NOT NULL,
  description       TEXT DEFAULT '',
  total_questions   INT NOT NULL DEFAULT 0,
  duration_minutes  INT NOT NULL DEFAULT 60,
  created_by        VARCHAR(50) DEFAULT 'admin',
  is_active         BOOLEAN DEFAULT TRUE,
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS test_questions (
  test_id           INT REFERENCES test_series(id) ON DELETE CASCADE,
  question_id       INT REFERENCES question_bank(id) ON DELETE CASCADE,
  question_order    INT NOT NULL DEFAULT 0,
  PRIMARY KEY (test_id, question_id)
);

CREATE TABLE IF NOT EXISTS student_question_history (
  student_id        VARCHAR(100) NOT NULL,
  question_id       INT REFERENCES question_bank(id) ON DELETE CASCADE,
  seen_at           TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (student_id, question_id)
);

CREATE INDEX IF NOT EXISTS idx_sqh_student
  ON student_question_history(student_id);

CREATE TABLE IF NOT EXISTS student_attempts (
  id                SERIAL PRIMARY KEY,
  student_id        VARCHAR(100) NOT NULL,
  test_id           INT REFERENCES test_series(id),
  started_at        TIMESTAMPTZ DEFAULT NOW(),
  submitted_at      TIMESTAMPTZ,
  score             INT DEFAULT 0,
  total_marks       INT DEFAULT 0,
  answers           JSONB DEFAULT '{}'
);
