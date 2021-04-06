CREATE TABLE question
(
    que_id SERIAL PRIMARY KEY,
    que_title TEXT NOT NULL,
    que_body TEXT NOT NULL,
    que_author VARCHAR(255) NOT NULL,
    que_creationDate TIMESTAMP NOT NULL
);

CREATE TABLE answer
(
    ans_id SERIAL PRIMARY KEY,
    ans_questionId INTEGER REFERENCES question (que_id),
    ans_body TEXT NOT NULL,
    ans_author VARCHAR(255) NOT NULL,
    ans_creationDate TIMESTAMP NOT NULL
);