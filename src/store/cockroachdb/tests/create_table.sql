CREATE TABLE IF NOT EXISTS students 
(rollNumber INT, 
name VARCHAR(30), 
class VARCHAR(30), 
section VARCHAR(1), 
mobile VARCHAR(10),
PRIMARY KEY (rollNumber, mobile));

INSERT INTO students (rollNumber, name, class, section, mobile) 
VALUES (1, 'BEN','FOURTH"', 'B', '4204206969');