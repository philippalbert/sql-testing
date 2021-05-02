CREATE VIEW MEAN_AGE_PER_COUNTRY AS
SELECT AVG(AGE) AS AVG_AGE, COUNTRIES.NAME
FROM PEOPLE
         LEFT JOIN COUNTRIES ON PEOPLE.COUNTRY_ID = COUNTRIES.ID
GROUP BY COUNTRIES.ID;