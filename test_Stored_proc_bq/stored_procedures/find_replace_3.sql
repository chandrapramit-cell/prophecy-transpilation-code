CREATE OR REPLACE PROCEDURE `prophecy-databricks-qa`.avpreetModels.find_replace_3(IN base_value STRING, IN rules_json STRING, OUT result STRING)
BEGIN
DECLARE rules_arr ARRAY<STRING>;
DECLARE i INT64 DEFAULT 0;
DECLARE rule_element JSON;
DECLARE find_pattern STRING;
DECLARE replace_value STRING;
SET result = base_value;
IF result IS NULL THEN
    RETURN;
END IF;
SET rules_arr = (SELECT ARRAY(SELECT value FROM UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(rules_json))) AS value));
rules_loop: LOOP
    IF i >= ARRAY_LENGTH(rules_arr) THEN
      LEAVE rules_loop;
END IF;
SET rule_element = PARSE_JSON(rules_arr[OFFSET(i)]);
SET find_pattern = JSON_VALUE(rule_element, '$.search_term');
SET replace_value = JSON_VALUE(rule_element, '$.replacement');
IF find_pattern IS NOT NULL AND replace_value IS NOT NULL THEN
      SET result = REGEXP_REPLACE(result, CONCAT('(?i)', find_pattern), replace_value);
END IF;
SET i = i + 1;
END LOOP;
END;