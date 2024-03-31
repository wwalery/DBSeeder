CREATE TABLE test_table_1 (
  id SERIAL PRIMARY KEY,
  enum_field VARCHAR(50),
  big_field VARCHAR(1000),
  read_only INTEGER,
  is_deleted SMALLINT,
  double_field DOUBLE PRECISION,
  char_field CHAR(10),
  date_field DATE,
  time_field TIME,
  timestamp_field TIMESTAMP,
  decimal_field_1 DECIMAL(5),
  decimal_field_2 DECIMAL(10,2),
  numeric_field NUMERIC(20,0),
  boolean_field BOOLEAN,
  smallint_field SMALLINT,
  bigint_field BIGINT,
  real_field REAL,
  binary_field BYTEA,
  varbinary_field BYTEA,
  other_field TEXT
);

CREATE TABLE test_table_2 (
  id SERIAL PRIMARY KEY,
  enum_field_2 VARCHAR(50),
  big_field_2 VARCHAR(1000),
  read_only INTEGER,
  test_array INTEGER[],
  test_array2 VARCHAR(50)[],
  test_object TEXT,
  is_deleted SMALLINT,
  test_table_1_id INT
);

ALTER TABLE test_table_2
  ADD FOREIGN KEY (test_table_1_id) REFERENCES test_table_1(id);

CREATE TABLE test_table_3 (
  id SERIAL PRIMARY KEY,
  enum_field_2 VARCHAR(50),
  big_field_2 VARCHAR(1000),
  read_only INTEGER,
  is_deleted SMALLINT,
  test_table_1_id INT,
  test_table_2_id INT
);

ALTER TABLE test_table_3
  ADD FOREIGN KEY (test_table_1_id) REFERENCES test_table_1(id);
ALTER TABLE test_table_3
  ADD FOREIGN KEY (test_table_2_id) REFERENCES test_table_2(id);
