CREATE OR REPLACE PROCEDURE `prophecy-databricks-qa`.avpreetModels.ii(IN abc INT64, OUT abc2 INT64)
BEGIN
DECLARE a INT64 DEFAULT 0;
DECLARE b INT64 DEFAULT 1;
DECLARE temp INT64;
DECLARE counter INT64 DEFAULT 2;
IF abc <= 0 THEN
        SET abc2 = 0;
ELSEIF abc = 1 THEN
        SET abc2 = 1;
ELSE
        WHILE counter <= abc DO
            SET temp = a + b;
SET a = b;
SET b = temp;
SET counter = counter + 1;
END WHILE;
SET abc2 = b;
END IF;
END;