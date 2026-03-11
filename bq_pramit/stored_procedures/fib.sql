CREATE OR REPLACE PROCEDURE `prophecy-databricks-qa`.avpreetTables.fib()
BEGIN
WITH RECURSIVE fibonacci AS (
  -- 1. BASE CASE: Define the starting point of the sequence
  -- step tracks the iteration, fib_num is the current number, next_num is the upcoming one
  SELECT 
    1 AS step, 
    0 AS fib_num, 
    1 AS next_num
  
  UNION ALL
  
  -- 2. RECURSIVE STEP: Add the previous two numbers together
  SELECT 
    step + 1 AS step, 
    next_num AS fib_num, 
    fib_num + next_num AS next_num
  FROM 
    fibonacci
  WHERE 
    step < 20  -- TERMINATION CONDITION: Generates the first 20 Fibonacci numbers
)

-- 3. FINAL SELECT: Retrieve the calculated sequence
SELECT 
  step, 
  fib_num
FROM 
  fibonacci
ORDER BY 
  step;
END;