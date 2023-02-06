CREATE VIEW "powers" (pk VARCHAR PRIMARY KEY, "personal"."hero" VARCHAR, "personal"."power" VARCHAR, "professional"."name" VARCHAR);
SELECT power1."professional"."name" as "Name1", power2."professional"."name" as "Name2", power1."personal"."power" as "Power"
FROM "powers" AS power1
    INNER JOIN "powers" as power2
ON power1."personal"."power" = power2."personal"."power"
WHERE power1."personal"."hero" = 'yes' AND power2."personal"."hero" = 'yes';
