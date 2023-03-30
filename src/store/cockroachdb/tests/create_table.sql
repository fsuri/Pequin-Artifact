CREATE TABLE table1 (a INT PRIMARY KEY, b INT NOT NULL DEFAULT 3 CHECK (b > 0), INDEX(b));

CREATE TABLE users (
        id UUID PRIMARY KEY,
        city STRING,
        name STRING,
        address STRING,
        credit_card STRING,
        dl STRING
);