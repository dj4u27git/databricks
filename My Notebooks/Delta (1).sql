-- Databricks notebook source
CREATE TABLE employees
             (id INT,name STRING,salary DOUBLE);

-- COMMAND ----------

INSERT INTO employees
VALUES
     (1,"Rohit",3500.0),
     (2,"Yashasvi",4020.5),
     (3,"Shubman",2999.3),
     (4,"Ajinkya",4000.3),
     (5,"Virat",2500.0),
     (6,"Ravindra",6200.3);

-- COMMAND ----------

SELECT * FROM employees;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name like "Y%";

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

select * from employees;

SELECT * FROM employees VERSION AS OF 1;
SELECT * FROM employees@v1;
DELETE FROM employees;

RESTORE TABLE employees TO VERSION AS OF 3;
select * from employees;
DESCRIBE HISTORY employees;

-- COMMAND ----------

DESCRIBE DETAIL employees;

-- COMMAND ----------

OPTIMIZE employees ZORDER BY id;

-- COMMAND ----------

DESCRIBE HISTORY employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees;

-- COMMAND ----------

VACUUM employees RETAIN 5 hours;

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 5 hours;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

DROP TABLE employees;

-- COMMAND ----------

CREATE TABLE external_default
       (width int, length int, height int)
LOCATION 'dbfs:/mnt/demo/external_default';

INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

DESCRIBE EXTENDED external_default;

-- COMMAND ----------

DROP TABLE external_default;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

CREATE SCHEMA custom_default
LOCATION 'dbfs:/Shared/schemas/custom.db'
