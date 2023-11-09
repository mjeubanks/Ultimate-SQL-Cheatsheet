--------------------------------------------------------------------------------------------------
--                                  CREATING AND EDITING TABLE BASICS
--------------------------------------------------------------------------------------------------

CREATE DATABASE my_database; -- Database stores tables with information revelent to each other

CREATE TABLE my_table(my_column1 bigserial, my_column2 varchar(25), my_column date); -- Create table then outline the column name and data type

INSERT INTO my_table(my_column2, my_column3) -- insert values into desired table and state which columns you will update
VALUES  ('varchar1', 'date1'), --insert the values and make sure the value matches the data type of the column
        ('varchar2', 'date2'), -- note that c1 is not added because its automatically updated from the bigserial type
        ('varchar3', 'date4');

SELECT * FROM my_table; -- displays all column and row values from the desired table

SELECT DISTINCT school_name FROM teachers --displays only unique values of school names (no duplicates)

DELETE FROM my_table;      -- deletes all rows from tables
DELETE TABLE my_table;      -- deletes the table entirely
DROP TABLE table_name;

--BASIC QUERYS MUST BE IN THE FOLLOWING ORDER: SELECT, FROM, WHERE, GROUP, ORDER, LIMIT
SELECT school, sum(salary)  -- select 'school' and sum of salary columns
FROM teachers               -- from the 'teachers' table
WHERE school ILIKE '%yer%'  -- filtered by a name that contains 'yer'. -- NOTE: Use HAVING instead of WHERE when filtering with aggregates and  
GROUP BY school             -- aggregate by school. NOTE: you can do more than one group
ORDER BY school DESC        -- order the rows by school in descending order 
LIMIT 1;                    -- only how a maximum of 1 rows


--HOW TO CREATE AN INDEX
CREATE INDEX street_idx ON new_york_addresses (street) --the index was given the name 'street_idx'

-- Listing 9-3: Using count() for table row counts
-- inserting a column name will give the rows of the specified column.
-- inserting DISTINCT inside the parentheses will give the count of unique values 
SELECT count(*) FROM pls_fy2018_libraries;

-- Bonus: find duplicate libnames
SELECT libname, count(libname)
FROM pls_fy2018_libraries
GROUP BY libname
ORDER BY count(libname) DESC;



--HOW TO MODIFY TABLES
ALTER TABLE table_name DROP CONSTRAINT column_name_key;
ALTER TABLE table_name ADD CONSTRAINT column_name_key PRIMARY KEY (my_column);
ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL;
ALTER TABLE table_name ALTER COLUMN column_name SET NOT NULL;
ALTER TABLE table_name ADD COLUMN column_name real; --real data type only as an example
ALTER TABLE table_name DROP COLUMN column_name;
ALTER TABLE table_name ALTER COLUMN column_name SET DATA TYPE real;
ALTER TABLE table_name RENAME TO new_table_name;
ALTER TABLE table_name RENAME COLUMN old_column_name TO new_column_name; 

UPDATE table_name SET my_column = value;

--HOW TO CREATE A BACKUP TABLE
CREATE TABLE meat_poultry_egg_establishments_backup AS
SELECT * FROM meat_poultry_egg_establishments;

--------------------------------------------------------------------------------------------------
--                                  DATA TYPES AND HOW TO CONVERT 
--------------------------------------------------------------------------------------------------

--LIST OF DATA TYPES
char(n)                   fixed n length column
varchar(n)                variable length column with maximum n length
text                      unlimited length and stored as text
smallint/smallserial      5 sigfigs
integer/serial            10 sigfigs
bigint/bigserial          20 sigfigs
numeric(x,y)              x total numbers with y decimal points
real                      6 decimal points
double precision          15 decimal points
timestamp                 date and time
timestamp with timezone   date and time
timestamptz               timestamp
date                      date only
time                      time only. note that this is meaningless without a timzone
interval                  time interval

--  HOW TO TRANSFORM DATA TYPES
SELECT numeric_column,
       CAST(numeric_column AS integer), --converts the column value to a different datatype
       CAST(numeric_column AS text)
FROM number_data_types;

--------------------------------------------------------------------------------------------------
--                                      HOW TO IMPORT AND EXPORT FILES
--------------------------------------------------------------------------------------------------

--  HOW TO IMPORT
COPY table_name                             -- NOTE: You must create the table first before importing the CSV
FROM 'C:\YourDirectory\your_file.csv'       -- create a table based off of a CSV on your local computer
WITH (FORMAT CSV, HEADER);                  -- note, optional DELIMITER '|' if value other than comma

-- HOW TO IMPORT SPECIFIC COLUMNS
COPY table_name (c1, c2, c3)                -- Use this option if the CSV only has certain columns
FROM 'C:\YourDirectory\your_file.csv'       
WITH (FORMAT CSV, HEADER);                  

--  HOW TO EXPORT
COPY table_name                             -- NOTE: You must create the table first before importing the CSV
TO 'C:\YourDirectory\your_file.csv'         -- create a table based off of a CSV on your local computer
WITH (FORMAT CSV, HEADER, DELIMITER '|');   -- note, optional DELIMITER '|' if value other than comma

-- HOW TO EXPORT SPECIFIC COLUMNS
COPY us_counties_pop_est_2019 
    (county_name, internal_point_lat, internal_point_lon)
TO 'C:\YourDirectory\us_counties_latlon_export.txt'
WITH (FORMAT CSV, HEADER, DELIMITER '|');

-- HOW TO EXPORT QUERY RESULTS
COPY (
    SELECT county_name, state_name
    FROM us_counties_pop_est_2019
    WHERE county_name ILIKE '%mill%'
     )
TO 'C:\YourDirectory\us_counties_mill_export.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------
--                                      BASIC MATH AND STATS FUNCTIONS
--------------------------------------------------------------------------------------------------

--BASIC MATH FUNCTIONS
--NOTE: MATH FOLLOWS PEMDAS RULE
+               -- addition                         
-               -- subtraction                      
*               -- multiplication                   
/               -- integer division                 
%               -- modulo division(remainder only)  
11.0 /6         -- decimal division                 
^               -- exponentiation                   
|/              -- square root (operator)           
sqrt(10)        -- square root (function)           
||/ 10          -- cube root                        
factorial(4)    -- factorial (function)             


-- EXAMPLE: CREATING A COLUMN BASED ON MATH RESULTS
SELECT county_name AS county,
       state_name AS state,
       births_2019 AS births,
       deaths_2019 AS deaths,
       births_2019 - deaths_2019 AS natural_increase
FROM us_counties_pop_est_2019
ORDER BY state_name, county_name;

-- LIST OF AGGREGATE FUNCTIONS

sum()                                                    --sum
avg()                                                    --average
percentile_cont(0.5) WITHIN GROUP (ORDER BY my_column)   --median
mode() WITHIN GROUP (ORDER BY my_column)                 --mode
max()                                                    --max
min()                                                    --min
corr(Y, x)                                               --correlation. note that it is (Y,x); not (x,Y). 1 = strong correlation. 0 = no correlation
regr_slope(Y,x)                                          --slope
regr_intercept(Y,x)                                      --intercept
regr_r2(Y,x)                                             --Rsquared
var_pop(numeric)                                         --Variance Population
var_samp(numeric)                                        --Variance Sample
stddev_pop(numeric)                                      --Standard Deviation Population
stddev_samp(numeric)                                     --Standard Deviation Sample
rank() OVER(ORDER BY column_name DESC)                   --Rank (no ties)
dense_rank() OVER(ORDER BY column_name DESC)             --Rank (with ties)
rank() OVER(PARTITION BY c1 ORDER BY column_name DESC)   --Rank (no ties). Creates a subgategory of groups
avg() OVER(ORDER BY column_name                          --12 Simple Moving Average
             ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)

sum() OVER (ORDER BY column_Name)                         --Cumulative Sum
count()                                                   --Counts the value of duplicates

--LIST OF MISC FUNCTION
length()                                                 --length of string characters
||                                                       --Place a string on the left and right side and it will join the string into a single value.
round(column_value, number_of_decimals)                  --round
--------------------------------------------------------------------------------------------------
--                                      TABLE JOINS
--------------------------------------------------------------------------------------------------
-- EXAMPLES: HOW TO CREATE CONTRAINTS AND REFERENCES
CREATE TABLE departments (
    dept_id integer,
    dept text NOT NULL,                                         -- NOT NULL will give an error if there is no value
    city text,
    CONSTRAINT dept_key PRIMARY KEY (dept_id),                  -- makes dept_id as the unique identifier. compsite keys also acceptable.
    CONSTRAINT dept_city_unique UNIQUE (dept, city),            -- values must have this unique combo.
    CONSTRAINT check_salary_not_below_zero CHECK (salary >= 0)  -- CHECK value ensures column meets criteria
);

CREATE TABLE employees (
    emp_id integer,
    first_name text,
    last_name text,
    salary numeric(10,2),
    dept_id integer REFERENCES departments (dept_id), -- this dept_id must contain the same values as dept_id from another table.
    CONSTRAINT emp_key PRIMARY KEY (emp_id)
);

-- NOTE: Imagine looking at two different tables side-by-side
-- HOW TO JOIN TABLES

SELECT *
FROM employees JOIN departments
ON employees.dept_id = departments.dept_id
ORDER BY employees.dept_id;
        -- join types:
        -- join     displays only values that are present in both tables
        -- left     join displays all values on left table and only matching values from right table
        -- right    join displays all values on right table and only matching values from left table.
        -- full     outer join joins all row values
        -- cross    join returns every possible combination of rows

-- HOW TO FIND NULL VALUES
SELECT *
FROM district_2020 LEFT JOIN district_2035
ON district_2020.id = district_2035.id
WHERE district_2035.id IS NULL;

-- HOW TO QUERY SPECIFIC COLUMNS
-- NOTE: MATH CAN BE PERFORMED ON COLUMNS FROM TWO DIFFERENT TABLES
SELECT district_2020.id,
       district_2020.school_2020,
       district_2035.school_2035
FROM district_2020 LEFT JOIN district_2035
ON district_2020.id = district_2035.id
ORDER BY district_2020.id;

--  HOW TO QUERY USING AN ALIAS
SELECT d20.id,
       d20.school_2020,
       d35.school_2035
FROM district_2020 AS d20 LEFT JOIN district_2035 AS d35
ON d20.id = d35.id
ORDER BY d20.id;

-- HOW TO JOIN MULTIPLE TABLES

SELECT d20.id,
       d20.school_2020,
       en.enrollment,
       gr.grades
FROM district_2020 AS d20 JOIN district_2020_enrollment AS en
    ON d20.id = en.id
JOIN district_2020_grades AS gr
    ON d20.id = gr.id
ORDER BY d20.id;

--UNIONS 
--NOTE: RETURNS COLUMNS SO THAT VALUES ARE IN ONE COLUMN INSTEAD OF SIDE-BY-SIDE

--union         rows from 2nd query added to first. duplicates removed
--intersect     only rows that exist in both queries
--except        only rows that exist in first query but not the second

SELECT * FROM district_2020
UNION
SELECT * FROM district_2035
ORDER BY id;

--------------------------------------------------------------------------------------------------
--                                      INDEXING
--------------------------------------------------------------------------------------------------

--OVERRIDING THE PRIMARY KEY
INSERT INTO surrogate_key_example
OVERRIDING SYSTEM VALUE
VALUES (4, 'Chicken Coop', '2021-09-03 10:33-07');

ALTER TABLE surrogate_key_example ALTER COLUMN order_number RESTART WITH 5;

INSERT INTO surrogate_key_example (product_name, order_time)
VALUES ('Aloe Plant', '2020-03-15 10:09-07');

SELECT * FROM surrogate_key_example;

--TRANSACTIONS TO REVERT CHANGES
    START TRANSACTION;

    UPDATE meat_poultry_egg_establishments
    SET company = 'AGRO Merchantss Oakland LLC'
    WHERE company = 'AGRO Merchants Oakland, LLC';

-- view changes
    SELECT company
    FROM meat_poultry_egg_establishments
    WHERE company LIKE 'AGRO%'
    ORDER BY company;

    -- Revert changes
    ROLLBACK;

-- See restored state
    SELECT company
    FROM meat_poultry_egg_establishments
    WHERE company LIKE 'AGRO%'
    ORDER BY company;

-- Alternately, commit changes at the end:
    START TRANSACTION;

    UPDATE meat_poultry_egg_establishments
    SET company = 'AGRO Merchants Oakland LLC'
    WHERE company = 'AGRO Merchants Oakland, LLC';

    COMMIT;

--------------------------------------------------------------------------------------------------
--                                      DATES AND TIMES
--------------------------------------------------------------------------------------------------

-- HOW TO EXTRACT TIMESTAMP VALUES
SELECT date_part('year', '2022-12-01 18:37:12 EST'::timestamptz)     -- acceptable text values: year, month, minute, seconds, timezone_hour, week, quarter, epoch

-- HOW TO MAKE DATES
    SELECT make_date(2022, 2, 22);                                       -- ORDER: year, month, day
    SELECT make_time(18, 4, 30.3);                                       -- ORDER: hour, minute, seconds
    SELECT make_timestamptz(2022, 2, 22, 18, 4, 30.3, 'Europe/Lisbon');  -- ORDER: year, month day, hour, minute, seconds, timezone

-- HOW TO GET DATE AND TIME FROM LOCAL MACHINE
    SELECT
        current_timestamp,
        localtimestamp,
        current_date,
        current_time,
        localtime,
        current_setting('timezone'),
        now();

--
SHOW timezone; -- This also works
to_timestamp(seconds) AT TIMEZONE 'UTC' converts seconds integer to timestamp

-- Listing 12-5: Showing time zone abbreviations and names
SELECT * FROM pg_timezone_abbrevs ORDER BY abbrev;
SELECT * FROM pg_timezone_names ORDER BY name;

-- Listing 12-6: Setting the time zone for a client session
SET TIME ZONE 'US/Pacific';

--You CAST function to do math on dates
SELECT '1929-09-30'::date - '1929-09-27'::date;
SELECT '1929-09-30'::date + '5 years'::interval;

-- Listing 12-13: Calculating cumulative intervals using OVER

SELECT segment,
       arrival - departure AS segment_duration,
       sum(arrival - departure) OVER (ORDER BY trip_id) AS cume_duration
FROM train_rides;


--------------------------------------------------------------------------------------------------
--                                      ADVANCED QUERY TECHNIQUES
--------------------------------------------------------------------------------------------------

--SUBQUERY can be used to make calculations on the main query

--HOW TO FILTER USING A SUBQUERY
SELECT county_name,
       state_name,
       pop_est_2019
FROM us_counties_pop_est_2019
WHERE pop_est_2019 >= (
    SELECT percentile_cont(.9) WITHIN GROUP (ORDER BY pop_est_2019)
    FROM us_counties_pop_est_2019
    )
ORDER BY pop_est_2019 DESC;

-- HOW TO ADD A SUBQUERY AS A COLUMN (NOTE: math can be performed on subqueries)

SELECT county_name,
       state_name AS st,
       pop_est_2019,
       (SELECT percentile_cont(.5) WITHIN GROUP (ORDER BY pop_est_2019)
        FROM us_counties_pop_est_2019) AS us_median
FROM us_counties_pop_est_2019;

-- HOW TO CREATE A TABLE BASED ON A SUBQUERY

SELECT round(calcs.average, 0) as average,
       calcs.median,
       round(calcs.average - calcs.median, 0) AS median_average_diff
FROM (
     SELECT avg(pop_est_2019) AS average,
            percentile_cont(.5)
                WITHIN GROUP (ORDER BY pop_est_2019)::numeric AS median
     FROM us_counties_pop_est_2019
     )
AS calcs;

--HOW TO JOIN TABLES IN SUBQUERY

SELECT census.state_name AS st,
       census.pop_est_2018,
       est.establishment_count,
       round((est.establishment_count/census.pop_est_2018::numeric) * 1000, 1)
           AS estabs_per_thousand
FROM
    (
         SELECT st,
                sum(establishments) AS establishment_count
         FROM cbp_naics_72_establishments
         GROUP BY st
    )
    AS est
JOIN
    (
        SELECT state_name,
               sum(pop_est_2018) AS pop_est_2018
        FROM us_counties_pop_est_2019
        GROUP BY state_name
    )
    AS census
ON est.st = census.state_name
ORDER BY estabs_per_thousand DESC;

-- USING WHERE EXISTS or WHERE NOT EXISTS(). NOTE: Uses a True/False boolean value

SELECT first_name, last_name
FROM employees
WHERE EXISTS (
    SELECT id
    FROM retirees
    WHERE id = employees.emp_id);

--HOW TO USE A LATERAL. NOTE: this is for when you need to do a calculation on a calculated value.
SELECT county_name,
       state_name,
       pop_est_2018,
       pop_est_2019,
       raw_chg,
       round(pct_chg * 100, 2) AS pct_chg
FROM us_counties_pop_est_2019,
     LATERAL (SELECT pop_est_2019 - pop_est_2018 AS raw_chg) rc,
     LATERAL (SELECT raw_chg / pop_est_2018::numeric AS pct_chg) pc
ORDER BY pct_chg DESC;

--HOW TO USE CTE. NOTE: CTE uses WITH statements to create temporary tables. This feature can also be used with join.

WITH
    counties (st, pop_est_2018) AS
    (SELECT state_name, sum(pop_est_2018)
     FROM us_counties_pop_est_2019
     GROUP BY state_name),

    establishments (st, establishment_count) AS
    (SELECT st, sum(establishments) AS establishment_count
     FROM cbp_naics_72_establishments
     GROUP BY st)

SELECT counties.st,
       pop_est_2018,
       establishment_count,
       round((establishments.establishment_count / 
              counties.pop_est_2018::numeric(10,1)) * 1000, 1)
           AS estabs_per_thousand
FROM counties JOIN establishments
ON counties.st = establishments.st
ORDER BY estabs_per_thousand DESC;

--HOW TO MAKE A CROSSTAB/PIVOT TABLE
SELECT *
FROM crosstab('SELECT office,			--gives the name values of the rows
                      flavor,			--gives the name values of the columns
                      count(*)			--gives the values in the intersection
               FROM ice_cream_survey
               GROUP BY office, flavor
               ORDER BY office',

              'SELECT flavor			--groups the flavor names. Note: GROUP BY was used because it has duplicate values
               FROM ice_cream_survey
               GROUP BY flavor
               ORDER BY flavor')

AS (office text,						--gives the name and data type of the columns
    chocolate bigint,
    strawberry bigint,
    vanilla bigint);

--HOW TO RECLASSIFY VALUES WITH CASE

SELECT max_temp,
       CASE WHEN max_temp >= 90 THEN 'Hot'
            WHEN max_temp >= 70 AND max_temp < 90 THEN 'Warm'
            WHEN max_temp >= 50 AND max_temp < 70 THEN 'Pleasant'
            WHEN max_temp >= 33 AND max_temp < 50 THEN 'Cold'
            WHEN max_temp >= 20 AND max_temp < 33 THEN 'Frigid'
            WHEN max_temp < 20 THEN 'Inhumane'
            ELSE 'No reading'
        END AS temperature_group
FROM temperature_readings
ORDER BY station_name, observation_date;

--------------------------------------------------------------------------------------------------
--                                      DATA MINING
--------------------------------------------------------------------------------------------------
--STRING FUNCTIONS
upper('Neal7'); --Returns 'NEAL7'
lower('Randy'); --Returns 'randy
initcap('at the end of the day'); --Returns 'At The End Of The Day. Note initcap's imperfect for acronyms
char_length(' Pat '); --Returns 5. This value includes spaces
position(', ' in 'Tan, Bella'); --Returns 4.
trim('s' from 'socks'); --Returns 'ock' removes all s from beginning and end of strong
trim(trailing 's' from 'socks'); -Returns 'ocks'.
trim(' Pat '); --Returns 'Pat. Default is to remove spaces
ltrim('socks', 's'); --Returns 'ocks'. Removes charcaters on left.
rtrim('socks', 's'); --Returns 'sock'. Removes characters on right.
left('703-555-1212', 3); --Returns '703' Extracts first 3 values of text.
right('703-555-1212', 8); --Returns '555-1212'. Extracts last 3 values of text.
concat(first_name,' ',last_name) --strings all of the words together
string_agg(distinct cmp.name, ', ') --an aggregates column texts and separates by comma

--REGULAR EXPRESSION (REGEX)
.       --Any character expect a new line
[FGz]   --Any character within the brackets. In this example F,G, or z
[a-z]   --A range of characters. In this example lowercase a to z.
[^a-z]  --Anything NOT lowercase a-z
\w      --Any word character or underscore. Same as [A-Za-z0-9_].
\d      --Any digit
\s      --Any space
\t       --Any tab
\n      --Any New line
\r      --Any return character
^       --Match at start of string
$       --Match at end of string
?       --Get the preceding match zero or one time
*       --Get the preceding match zero or more times
+       --Get the preceding match one ore more times
{m}     --Get the preceding match exactly m times
{m,n}   --Get the preceding match exactly between m and n times
a|b     --Find either a or b
()      --Create and report a capture group or set precedence.
(?:)    --Negate the reporting of a capture group.


-- Any character one or more times
SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from '.+'); -- The game starts at 7 p.m. on May 2, 2024.
-- One or two digits followed by a space and a.m. or p.m. in a noncapture group

SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from '\d{1,2} (?:a.m.|p.m.)');
   --This is important to NOTE. If there was only (a.m.|p.m.). This would mean that you want the entire string 
   --to match this format but you only want to extract the values in the perenthesis. 

SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from '^\w+'); --Returns 'The'

SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from '\w+.$'); --Returns '2024'

SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from 'May|June'); --Returns 'May'

SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from '\d{4}'); --Returns '2024'

SELECT substring('The game starts at 7 p.m. on May 2, 2024.' from 'May \d, \d{4}'); --Returns 'May 2, 2024'

--HOW TO USED REGEX FOR WHERE
-- ~ case sensitive. Similar to LIKE.
-- ~* case insensitive. Similar to ILIKE.
SELECT county_name
FROM us_counties_pop_est_2019
WHERE county_name ~* '(lade|lare)'
ORDER BY county_name;


SELECT regexp_replace('05/12/2024', '\d{4}', '2023'); --(String, Pattern, Replacement Text). Returns 05/12/2023
SELECT regexp_split_to_table('Four,score,and,seven,years,ago', ','); --Creates a new row using comma as the identifier.
SELECT regexp_split_to_array('Phil Mike Tony Steve', ' '); --Creates an Array {Phil, Mike, Tony, Steve}
SELECT array_length(regexp_split_to_array('Phil Mike Tony Steve', ' '), 1); --Find array length

--NOTE: regexp_match() is similar to substring() expect that it returns the value as an array and can give null values.

--EXAMPLE OF FULL TEXT

--4/16/17-4/17/17
--2100-0900 hrs.
--46000 Block Ashmere Sq.
--Sterling
--Larceny: The victim reported that a
--bicycle was stolen from their opened
--garage door during the overnight hours.
--C0170006614
SELECT crime_id,
       regexp_match(original_text, '\d{1,2}\/\d{1,2}\/\d{2}') AS date_1,
       CASE WHEN EXISTS (SELECT regexp_matches(original_text, '-(\d{1,2}\/\d{1,2}\/\d{2})'))
            THEN regexp_match(original_text, '-(\d{1,2}\/\d{1,2}\/\d{2})')
            ELSE NULL
            END AS date_2,
       regexp_match(original_text, '\/\d{2}\n(\d{4})') AS hour_1,
       CASE WHEN EXISTS (SELECT regexp_matches(original_text, '\/\d{2}\n\d{4}-(\d{4})'))
            THEN regexp_match(original_text, '\/\d{2}\n\d{4}-(\d{4})')
            ELSE NULL
            END AS hour_2,
       regexp_match(original_text, 'hrs.\n(\d+ .+(?:Sq.|Plz.|Dr.|Ter.|Rd.))') AS street,
       regexp_match(original_text, '(?:Sq.|Plz.|Dr.|Ter.|Rd.)\n(\w+ \w+|\w+)\n') AS city,
       regexp_match(original_text, '\n(?:\w+ \w+|\w+)\n(.*):') AS crime_type,
       regexp_match(original_text, ':\s(.+)(?:C0|SO)') AS description,
       regexp_match(original_text, '(?:C0|SO)[0-9]+') AS case_number
FROM crime_reports
ORDER BY crime_id;

--FULL TEXT SEARCH

SELECT to_tsvector('english', 'I am walking across the sitting room to sit with you.'); --returns 'across':4 'room':7 'sit':6,9 'walk':3
SELECT to_tsquery('english', 'walking & sitting'); --'walk' & 'sit'. NOTE: You can use operators |, & and !.

-- Listing 14-17: Querying a tsvector type with a tsquery

SELECT to_tsvector('english', 'I am walking across the sitting room') @@ --True. The tsvector contains the tsquery
       to_tsquery('english', 'walking & sitting');

SELECT to_tsvector('english', 'I am walking across the sitting room') @@ --False The tsvector does NOT contain the tsquery
       to_tsquery('english', 'walking & running');

-- Listing 14-21: Finding speeches containing the word "Vietnam"

SELECT president, speech_date
FROM president_speeches
WHERE search_speech_text @@ to_tsquery('english', 'Vietnam')
ORDER BY speech_date;

-- Listing 14-22: Displaying search results with ts_headline()

SELECT president,
       speech_date,
       ts_headline(speech_text, to_tsquery('english', 'tax'),
                   'StartSel = <,
                    StopSel = >,
                    MinWords=5,
                    MaxWords=7,
                    MaxFragments=1')
FROM president_speeches
WHERE search_speech_text @@ to_tsquery('english', 'tax')
ORDER BY speech_date;

-- Listing 14-25: Scoring relevance with ts_rank()

SELECT president,
       speech_date,
       ts_rank(search_speech_text,
               to_tsquery('english', 'war & security & threat & enemy'))
               AS score
FROM president_speeches
WHERE search_speech_text @@ 
      to_tsquery('english', 'war & security & threat & enemy')
ORDER BY score DESC
LIMIT 5;



--------------------------------------------------------------------------------------------------
--                                      SPATIAL DATA
--------------------------------------------------------------------------------------------------
-- Listing 15-3: Using ST_GeomFromText() to create spatial objects

SELECT ST_GeomFromText('POINT(-74.9233606 42.699992)', 4326);

SELECT ST_GeomFromText('LINESTRING(-74.9 42.7, -75.1 42.7)', 4326);

SELECT ST_GeomFromText('POLYGON((-74.9 42.7, -75.1 42.7,
                                 -75.1 42.6, -74.9 42.7))', 4326);

SELECT ST_GeomFromText('MULTIPOINT (-74.9 42.7, -75.1 42.7)', 4326);

SELECT ST_GeomFromText('MULTILINESTRING((-76.27 43.1, -76.06 43.08),
                                        (-76.2 43.3, -76.2 43.4,
                                         -76.4 43.1))', 4326);

SELECT ST_GeomFromText('MULTIPOLYGON((
                                     (-74.92 42.7, -75.06 42.71,
                                      -75.07 42.64, -74.92 42.7),
                                     (-75.0 42.66, -75.0 42.64,
                                      -74.98 42.64, -74.98 42.66,
                                      -75.0 42.66)))', 4326);


SELECT
ST_GeogFromText('SRID=4326;MULTIPOINT(-74.9 42.7, -75.1 42.7, -74.924 42.6)');

-- Listing 15-5: Functions specific to making points

SELECT ST_PointFromText('POINT(-74.9233606 42.699992)', 4326);

SELECT ST_MakePoint(-74.9233606, 42.699992);
SELECT ST_SetSRID(ST_MakePoint(-74.9233606, 42.699992), 4326);

SELECT ST_LineFromText('LINESTRING(-105.90 35.67,-105.91 35.67)', 4326);
SELECT ST_MakeLine(ST_MakePoint(-74.9, 42.7), ST_MakePoint(-74.1, 42.4));

SELECT ST_PolygonFromText('POLYGON((-74.9 42.7, -75.1 42.7,
                                    -75.1 42.6, -74.9 42.7))', 4326);

SELECT ST_MakePolygon(
           ST_GeomFromText('LINESTRING(-74.92 42.7, -75.06 42.71,
                                       -75.07 42.64, -74.92 42.7)', 4326));

SELECT ST_MPolyFromText('MULTIPOLYGON((
                                       (-74.92 42.7, -75.06 42.71,
                                        -75.07 42.64, -74.92 42.7),
                                       (-75.0 42.66, -75.0 42.64,
                                        -74.98 42.64, -74.98 42.66,
                                        -75.0 42.66)
                                      ))', 4326);

-- Add column
ALTER TABLE farmers_markets ADD COLUMN geog_point geography(POINT,4326);

ST_AsEWKT() -- tells you the WKT version of the coordinates
ST_AsText()
ST_Area() --Returns area of polygon. NOTE: a geography type returns units in square meters; geometry in SRID (which isn't really usefull)

-- Add a spatial (R-Tree) index using GIST
CREATE INDEX market_pts_idx ON farmers_markets USING GIST (geog_point);

-- Listing 15-10: Using ST_DWithin() to locate farmers' markets within 10 kilometers of a point
-- Returns a boolean value if a geopoint is within a distance, in meters, of a specific geopoint.
SELECT market_name,
       city,
       st,
       geog_point
FROM farmers_markets
WHERE ST_DWithin(geog_point,
                 ST_GeogFromText('POINT(-93.6204386 41.5853202)'),
                 10000)
ORDER BY market_name;

-- Listing 15-11: Using ST_Distance() to calculate the miles between Yankee Stadium
-- and Citi Field (Mets)
-- 1609.344 meters/mile

SELECT ST_Distance(
                   ST_GeogFromText('POINT(-73.9283685 40.8296466)'),
                   ST_GeogFromText('POINT(-73.8480153 40.7570917)')
                   ) / 1609.344 AS mets_to_yanks;

-- Listing 15-13: Using the <-> distance operator for a nearest neighbors search

SELECT market_name,
       city,
       st,
       round(
           (ST_Distance(geog_point,
                        ST_GeogFromText('POINT(-68.2041607 44.3876414)')
                        ) / 1609.344)::numeric, 2
            ) AS miles_from_bh
FROM farmers_markets
ORDER BY geog_point <-> ST_GeogFromText('POINT(-68.2041607 44.3876414)')
LIMIT 3;

-- Listing 15-20: Spatial join with ST_Intersects() to find roads crossing the Santa Fe river

SELECT water.fullname AS waterway,
       roads.rttyp,
       roads.fullname AS road
FROM santafe_linearwater_2019 water JOIN santafe_roads_2019 roads
    ON ST_Intersects(water.geom, roads.geom)
WHERE water.fullname = 'Santa Fe Riv' 
      AND roads.fullname IS NOT NULL
ORDER BY roads.fullname;

--------------------------------------------------------------------------------------------------
--                                      JSON
--------------------------------------------------------------------------------------------------

--The following code refers to the JSON below
{"title": "The Incredibles", "year": 2004, "rating": {"MPAA": "PG"}, "characters": [{"name": "Mr. Incredible", "actor": "Craig T. Nelson"}, {"name": "Elastigirl", "actor": "Holly Hunter"}, {"name": "Frozone", "actor": "Samuel L. Jackson"}], "genre": ["animation", "action", "sci-fi"]}
{"title": "Cinema Paradiso", "year": 1988, "characters": [{"name": "Salvatore", "actor": "Salvatore Cascio"}, {"name": "Alfredo", "actor": "Philippe Noiret"}], "genre": ["romance", "drama"]}

-- Listing 16-2: Creating a table to hold JSON data and adding an index

CREATE TABLE films (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    film jsonb NOT NULL
);

COPY films (film)
FROM 'C:\YourDirectory\films.json';

CREATE INDEX idx_film ON films USING GIN (film);

SELECT * FROM films;

-- JSON AND JSONB EXTRACTION OPERATORS
SELECT id,  film -> 'characters' AS characters,         --returns [{"name": "Mr. Incredible", "actor": "Craig T. Nelson"}..]} as jsonb 
            film ->> 'title'  AS title,                 --returns The Incredibles as text
            film -> 'genre' AS genre,                   --returns ["animation", "action", "sci-fi"] as jsonb
            film -> 'genre' -> 0 AS genres,             --extracts 1st element array "animation" jsonb
            film -> 'genre' -> -1 AS genres,            --extracts last element array "scifi" jsonb
            film -> 'genre' -> 2 AS genres,             --extracts 3rd element array "scifi" jsonb
            film -> 'genre' ->> 0 AS genres,            --extracts  1st element array "animation" as text
            film #> '{rating, MPAA}' AS mpaa_rating,    --extracts 'PG'
            film -> 'rating' ->'MPAA'                   --extracts 'PG' ALTERNATIVE METHOD
            film #> '{characters, 0, name}' AS name,    --extracts first element in array under 'name': 'Mr. Incredible'
            film -> 'characters' -> 0-> 'name' AS name, --extracts first element in array under 'name': 'Mr. Incredible' ALTERNATIVE METHOD
            film #> '{characters, 1, actor}' AS actor,  --extracts 2nd element in array under actor: 'Samuel L Jackson'
            film #>> '{characters, 0, name}' AS name,   --extracts first element in array under 'name': 'Mr. Incredible' as text,
            film @> CAST('{"title": "The Incredibles"}' AS jsonb) AS is_incredible, --Returns boolean value if table contains JSON value.
            CAST('{"title": "The Incredibles"}' AS jsonb) <@ film AS is_incredible --Returns boolean value if JSON value is inside the table.,
            film ? 'rating' AS existence,               --Returns true if exists as top-level key or array value,
            film ?| '{rating, genre}' AS existence_array,   --Returns true if any of the text in the array are a top-level key or array value.
            film ?& '{rating, genre}' AS existence_all      --Returns true if ALL of the text in the array are a top-level key or array value.

FROM films
ORDER BY id;

-- GENERATING AND MANIPULATING JSON

-- Listing 16-21: Turning query results into JSON with to_json()

-- Convert an entire row from the table
SELECT to_json(employees) AS json_rows
FROM employees;

-- Listing 16-22: Specifying columns to convert to JSON
-- Returns key names as f1, f2, etc.
SELECT to_json(row(emp_id, last_name)) AS json_rows
FROM employees;

-- Listing 16-23: Generating key names with a subquery
SELECT to_json(employees) AS json_rows
FROM (
    SELECT emp_id, last_name AS ln FROM employees
) AS employees;

-- Listing 16-24: Aggregating the rows and converting to JSON
SELECT json_agg(to_json(employees)) AS json
FROM (
    SELECT emp_id, last_name AS ln FROM employees
) AS employees;

-- Listing 16-25: Adding a top-level key/value pair via concatenation
-- Two examples

UPDATE films
SET film = film || '{"studio": "Pixar"}'::jsonb
WHERE film @> '{"title": "The Incredibles"}'::jsonb; 

UPDATE films
SET film = film || jsonb_build_object('studio', 'Pixar')
WHERE film @> '{"title": "The Incredibles"}'::jsonb; 

SELECT film FROM films -- check the updated data
WHERE film @> '{"title": "The Incredibles"}'::jsonb; 

-- Listing 16-26: Setting an array value at a path

UPDATE films
SET film = jsonb_set(film,
                 '{genre}',
                  film #> '{genre}' || '["World War II"]',
                  true)
WHERE film @> '{"title": "Cinema Paradiso"}'::jsonb; 

SELECT film FROM films -- check the updated data
WHERE film @> '{"title": "Cinema Paradiso"}'::jsonb; 

-- Listing 16-27: Deleting values from JSON

-- Removes the studio key/value pair from The Incredibles
UPDATE films
SET film = film - 'studio'
WHERE film @> '{"title": "The Incredibles"}'::jsonb; 

-- Removes the third element in the genre array of Cinema Paradiso
UPDATE films
SET film = film #- '{genre, 2}'
WHERE film @> '{"title": "Cinema Paradiso"}'::jsonb; 


-- JSON PROCESSING FUNCTIONS

-- Listing 16-28: Finding the length of an array

SELECT id,
       film ->> 'title' AS title,
       jsonb_array_length(film -> 'characters') AS num_characters
FROM films
ORDER BY id;

-- Listing 16-29: Returning array elements as rows

SELECT id,
       jsonb_array_elements(film -> 'genre') AS genre_jsonb,
       jsonb_array_elements_text(film -> 'genre') AS genre_text
FROM films
ORDER BY id;

-- Listing 16-30: Returning key values from each item in an array

SELECT id, 
       jsonb_array_elements(film -> 'characters')
FROM films
ORDER BY id;

WITH characters (id, json) AS (
    SELECT id,
           jsonb_array_elements(film -> 'characters')
    FROM films
)
SELECT id, 
       json ->> 'name' AS name,
       json ->> 'actor' AS actor
FROM characters
ORDER BY id;
--------------------------------------------------------------------------------------------------
--                                  VIEWS, FUNCTIONS AND TRIGGERS
--------------------------------------------------------------------------------------------------

--A view is good for two reasons: 1.) It saves your query so you don't have to re-format the table again and 2.) it can hide sensitive information.
--Making updates to a view will also update the main table.
CREATE OR REPLACE VIEW nevada_counties_pop_2019 AS
    SELECT county_name,
           state_fips,         
           county_fips,
           pop_est_2019
    FROM us_counties_pop_est_2019
    WHERE state_name = 'Nevada';

    CREATE MATERIALIZED VIEW nevada_counties_pop_2019 AS #insertqueryhere --A materialized view stores the database


-- Create view:
CREATE OR REPLACE VIEW employees_tax_dept WITH (security_barrier) AS --security barrier adds restrictions. Cannot make alterations outside scope of view.
     SELECT emp_id,
            first_name,
            last_name,
            dept_id
     FROM employees
     WHERE dept_id = 1
     WITH LOCAL CHECK OPTION; --ensures users can only insert data shown in the view. Cannot make alterations outside scope of view.

-- Listing 17-11: Creating a percent_change function
-- To delete this function: DROP FUNCTION percent_change(numeric,numeric,integer);

CREATE OR REPLACE FUNCTION                                              --Initiate creation
percent_change(new_value numeric,                                       --provide the funciton variables
               old_value numeric,
               decimal_places integer DEFAULT 1)
RETURNS numeric AS                                                      --state the result value
'SELECT round(
       ((new_value - old_value) / old_value) * 100, decimal_places 
);'                                                                     --define the function
LANGUAGE SQL                                                            --state the language
IMMUTABLE                                                               --The result cannot modify the database
RETURNS NULL ON NULL INPUT;                                             --gaurantees a result is given.

--PYTHON FUNCTION
CREATE OR REPLACE FUNCTION trim_county(input_string text)
RETURNS text AS $$
    import re
    cleaned = re.sub(r' County', '', input_string)
    return cleaned
$$
LANGUAGE plpython3u;

-- Listing 17-15: Creating an update_personal_days() procedure
--Procedures are usefull for updating the table itself
--The below example  alsocould be done by simply using the UPDATE table.
CREATE OR REPLACE PROCEDURE update_personal_days()                      --initiate creation
AS $$                                                                   --The $$ signifies the start and the end
BEGIN                                                                   --signifies the beginning of the block
    UPDATE teachers
    SET personal_days =
        CASE WHEN (now() - hire_date) >= '10 years'::interval
                  AND (now() - hire_date) < '15 years'::interval THEN 4
             WHEN (now() - hire_date) >= '15 years'::interval
                  AND (now() - hire_date) < '20 years'::interval THEN 5
             WHEN (now() - hire_date) >= '20 years'::interval
                  AND (now() - hire_date) < '25 years'::interval THEN 6
             WHEN (now() - hire_date) >= '25 years'::interval THEN 7
             ELSE 3
        END;                                                            --signifies the beginning of the block
    RAISE NOTICE 'personal_days updated!';                              --gives this statement when the udpate is complete
END;                                                                    --signifies the beginning of the block
$$                                                                      --The $$ signifies the start and the end
LANGUAGE plpgsql;                                                       --states the language

-- To invoke the procedure:
CALL update_personal_days();

--A trigger does a funciton if a certain event occours on the table such as INSERT, UPDATE or DELETE.
--A trigger can occour before, after or instead of an event.
CREATE OR REPLACE FUNCTION record_if_grade_changed()
    RETURNS trigger AS
$$
BEGIN
    IF NEW.grade <> OLD.grade THEN --OLD is the value before the update, NEW is the value after the update (or a brand new value)
    INSERT INTO grades_history (
        student_id,
        course_id,
        change_time,
        course,
        old_grade,
        new_grade)
    VALUES
        (OLD.student_id,
         OLD.course_id,
         now(),
         OLD.course,
         OLD.grade,
         NEW.grade);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Listing 17-21: Creating the grades_update trigger

CREATE TRIGGER grades_update
  AFTER UPDATE
  ON grades
  FOR EACH ROW
  EXECUTE PROCEDURE record_if_grade_changed();

  -- Listing 17-25: Creating the temperature_insert trigger

CREATE TRIGGER temperature_insert
    BEFORE INSERT
    ON temperature_test
    FOR EACH ROW
    EXECUTE PROCEDURE classify_max_temp();