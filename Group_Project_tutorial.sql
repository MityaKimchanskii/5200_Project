----------------------------------------------------------------------------------
-- 1. Upload data to Hadoop server
----------------------------------------------------------------------------------
scp "/Users/mityakim/Desktop/LA.zip" dkim171@129.146.230.230:/home/dkim171/
scp "/Users/mityakim/Desktop/Austin.zip" dkim171@129.146.230.230:/home/dkim171/
scp "/Users/mityakim/Desktop/Dallas.zip" dkim171@129.146.230.230:/home/dkim171/
scp "/Users/mityakim/Desktop/Chicago.zip" dkim171@129.146.230.230:/home/dkim171/

scp "/Users/mityakim/Desktop/Austin_Crime.csv" dkim171@129.146.230.230:/home/dkim171/

-- Enter to the server
ssh dkim171@129.146.230.230

unzip LA.zip
unzip Austin.zip
unzip Dallas.zip
unzip Dallas.zip
----------------------------------------------------------------------------------
-- 2. Create directory for each city
----------------------------------------------------------------------------------
-- LA
hdfs dfs -mkdir LA
hdfs dfs -mkdir LA/From10
hdfs dfs -mkdir LA/FromPr
hdfs dfs -mkdir LA/LAPop

-- Chicago 
hdfs dfs -mkdir Chicago
hdfs dfs -mkdir Chicago/ChicagoCrime
hdfs dfs -mkdir Chicago/ChicagoPop

-- Austin
hdfs dfs -mkdir Austin
hdfs dfs -mkdir Austin/AustinCrime
hdfs dfs -mkdir Austin/AustinPop

-- Dallas
hdfs dfs -mkdir Dallas
hdfs dfs -mkdir Dallas/DallasCrime
hdfs dfs -mkdir Dallas/DallasoPop

hdfs dfs -ls -- list all files and directories
hdfs dfs -ls Chicago -- list all files in Chicago 

----------------------------------------------------------------------------------
-- 3. Move all uploaded files to the HDFS directory 
----------------------------------------------------------------------------------
-- LA ----------------------------------------------------------------------------------
hdfs dfs -put Crime_Data_from_2010_to_2019.csv LA/From10/ -- put file to the location on the HDFS
hdfs dfs -put Crime_Data_from_2020_to_Present.csv LA/FromPr/ 
hdfs dfs -put LA_Population.csv LA/LAPop/

-- Chicago -----------------------------------------------------------------------------
hdfs dfs -put Chicago_Crimes_-_2001_to_Present.csv Chicago/ChicagoCrime/ 
hdfs dfs -put Chicago_Population.csv Chicago/ChicagoPop/
-- check 
hdfs dfs -ls Chicago/ChicagoPop/
hdfs dfs -ls Chicago/ChicagoCrime/ 

-- Austin -----------------------------------------------------------------------------
hdfs dfs -put Austin_APD_Computer_Aided_Dispatch_Incidents.csv Austin/AustinCrime/
hdfs dfs -put Austin_Crime.csv Austin/AustinCrime/
hdfs dfs -put Austin_Population.csv Austin/AustinPop/
-- check 
hdfs dfs -ls Austin/AustinPop/
hdfs dfs -ls Austin/AustinCrime/ 

-- Dallas -----------------------------------------------------------------------------
hdfs dfs -put Dallas_Police_Incidents_20241014.csv Dallas/DallasCrime/
hdfs dfs -put Dallas_Population.csv Dallas/DallasoPop/
-- check 
hdfs dfs -ls Dallas/DallasoPop/
hdfs dfs -ls Dallas/DallasCrime/ 

----------------------------------------------------------------------------------
-- 4. Open beeline
----------------------------------------------------------------------------------
beeline
-- use my database 
use dkim171;
--/-----------------------------------------------------------------------------------/-- 
--/--------------------------------------LA-------------------------------------------/-- 
--/-----------------------------------------------------------------------------------/-- 
-- Create Tabel la_crime_from_2010_to_2019
DROP TABLE IF EXISTS la_crime_10_19;
CREATE EXTERNAL TABLE IF NOT EXISTS la_crime_10_19(
    DR_NO STRING,
    Date_Rpt STRING,
    DATE_OCC STRING,
    TIME_OCC STRING,
    AREA STRING,
    AREA_NAME STRING,
    RptDist_No STRING,
    Part_1_2 STRING,
    Crm_Cd STRING,
    Crm_Cd_Desc STRING,
    Mocodes STRING,
    Vict_Age STRING,
    Vict_Sex STRING,
    Vict_Descent STRING,
    Premis_Cd STRING,
    Premis_Desc STRING,
    Weapon_Used_Cd STRING,
    Weapon_Desc STRING,
    Status STRING,
    Status_Desc STRING,
    Crm_Cd_1 STRING,
    Crm_Cd_2 STRING,
    Crm_Cd_3 STRING,
    Crm_Cd_4 STRING,
    LOCATION STRING,
    Cross_Street STRING,
    LAT STRING,
    LON STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE LOCATION '/user/dkim171/LA/From10/'
TBLPROPERTIES ('skip.header.line.count'='1');
"
-- check if table exists
show tables;
select * from la_crime_10_19 LIMIT 10;

-- create another table la_crime_present
DROP TABLE IF EXISTS la_crime_present;
CREATE EXTERNAL TABLE IF NOT EXISTS la_crime_present(
    DR_NO STRING,
    Date_Rpt STRING,
    DATE_OCC STRING,
    TIME_OCC STRING,
    AREA STRING,
    AREA_NAME STRING,
    RptDist_No STRING,
    Part_1_2 STRING,
    Crm_Cd STRING,
    Crm_Cd_Desc STRING,
    Mocodes STRING,
    Vict_Age STRING,
    Vict_Sex STRING,
    Vict_Descent STRING,
    Premis_Cd STRING,
    Premis_Desc STRING,
    Weapon_Used_Cd STRING,
    Weapon_Desc STRING,
    Status STRING,
    Status_Desc STRING,
    Crm_Cd_1 STRING,
    Crm_Cd_2 STRING,
    Crm_Cd_3 STRING,
    Crm_Cd_4 STRING,
    LOCATION STRING,
    Cross_Street STRING,
    LAT STRING,
    LON STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  -- Use OpenCSVSerde to handle commas in quoted strings
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE 
LOCATION '/user/dkim171/LA/FromPr/' 
TBLPROPERTIES ('skip.header.line.count'='1');
"

-- check if table exists
show tables;
select * from la_crime_present LIMIT 10;

-- create la_population table 
DROP TABLE IF EXISTS la_population;
CREATE EXTERNAL TABLE IF NOT EXISTS la_population(
    Year INT,
    Population INT,
    `Year on Year Change` STRING,  
    `Change in Percent` STRING     
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",            
   "quoteChar"     = "\""                
)
STORED AS TEXTFILE LOCATION '/user/dkim171/LA/LAPop/'
TBLPROPERTIES ('skip.header.line.count'='1');
"
-- check if table exists
show tables;
select * from la_population;
--/-----------------------------------------------------------------------------------/-- 
--/----------------------------------Chicago------------------------------------------/-- 
--/-----------------------------------------------------------------------------------/-- 
-- create chicago_crime table
DROP TABLE IF EXISTS chicago_crime;
CREATE EXTERNAL TABLE IF NOT EXISTS chicago_crime(
    ID int,
    Case_Number string,
    `Date` string,
    Block string,
    IUCR string,
    Primary_Type string,
    Description string,
    Location_Description string,
    Arrest string,
    Domestic string,
    Beat string,
    District string,
    Ward string,
    Community_Area string,
    FBI_Code string,
    X_Coordinate string,
    Y_Coordinate string,
    Year string,
    Updated_On string,
    Latitude string,
    Longitude string,
    Location string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/dkim171/Chicago/ChicagoCrime/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- check if table exists
show tables;
select * from chicago_crime LIMIT 100;

-- create chicago_population table
DROP TABLE IF EXISTS chicago_population;
CREATE EXTERNAL TABLE IF NOT EXISTS chicago_population(
    Year INT,
    Population INT,
    `Year on Year Change` STRING,  
    `Change in Percent` STRING     
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",            
   "quoteChar"     = "\""                  
)
STORED AS TEXTFILE LOCATION '/user/dkim171/Chicago/ChicagoPop/'
TBLPROPERTIES ('skip.header.line.count'='1');
"
-- check if table exists
show tables;
select * from chicago_population;

--/-----------------------------------------------------------------------------------/-- 
--/----------------------------------Austin-------------------------------------------/-- 
--/-----------------------------------------------------------------------------------/-- 
-- create austin_crime table
DROP TABLE IF EXISTS austin_crime;
CREATE EXTERNAL TABLE IF NOT EXISTS austin_crime(
    Incident_Number STRING, 
    Highest_Offense_Description STRING, 
    Highest_Offense_Code INT, 
    Family_Violence STRING, 
    Occurred_Date_Time STRING, 
    Occurred_Date STRING, 
    Occurred_Time STRING, 
    Report_Date_Time STRING, 
    Report_Date STRING, 
    Report_Time STRING, 
    Location_Type STRING, 
    Council_District STRING, 
    APD_Sector STRING, 
    APD_District STRING, 
    Clearance_Status STRING, 
    Clearance_Date STRING, 
    UCR_Category STRING, 
    Category_Description STRING, 
    Census_Block_Group STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/dkim171/Austin/AustinCrime/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- check if table exists
show tables;
select * from austin_crime LIMIT 10;

-- create austin_population table
DROP TABLE IF EXISTS austin_population;
CREATE EXTERNAL TABLE IF NOT EXISTS austin_population(
    Year INT,
    Population INT,
    `Year on Year Change` STRING,  
    `Change in Percent` STRING     
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",            
   "quoteChar"     = "\""                  
)
STORED AS TEXTFILE LOCATION '/user/dkim171/Austin/AustinPop/'
TBLPROPERTIES ('skip.header.line.count'='1');
"
-- check if table exists
show tables;
select * from austin_population;

--/-----------------------------------------------------------------------------------/-- 
--/----------------------------------Dallas-------------------------------------------/-- 
--/-----------------------------------------------------------------------------------/-- 
-- create dallas_crime table
DROP TABLE IF EXISTS dallas_crime;
CREATE EXTERNAL TABLE IF NOT EXISTS dallas_crime(
    Incident_Number STRING,
    Year_of_Incident STRING,
    Service_Number_ID STRING,
    Watch STRING,
    Call_Problem STRING,
    Type_of_Incident STRING,
    Type_Location STRING,
    Type_Property STRING,
    Incident_Address STRING,
    Apartment_Number STRING,
    Reporting_Area STRING,
    Beat STRING,
    Division STRING,
    Sector STRING,
    Council_District STRING,
    Target_Area_Action_Grids STRING,
    Community STRING,
    Date1_of_Occurrence STRING,
    Year1_of_Occurrence STRING,
    Month1_of_Occurrence STRING,
    Day1_of_the_Week STRING,
    Time1_of_Occurrence STRING,
    Day1_of_the_Year STRING,
    Date2_of_Occurrence STRING,
    Year2_of_Occurrence STRING,
    Month2_of_Occurrence STRING,
    Day2_of_the_Week STRING,
    Time2_of_Occurrence STRING,
    Day2_of_the_Year STRING,
    Date_of_Report STRING,
    Date_Incident_Created STRING,
    Offense_Entered_Year STRING,
    Offense_Entered_Month STRING,
    Offense_Entered_Day_of_the_Week STRING,
    Offense_Entered_Time STRING,
    Offense_Entered_Date_Time STRING,
    CFS_Number STRING,
    Call_Received_Date_Time STRING,
    Call_Date_Time STRING,
    Call_Cleared_Date_Time STRING,
    Call_Dispatch_Date_Time STRING,
    Special_Report_Pre_RMS STRING,
    Person_Involvement_Type STRING,
    Victim_Type STRING,
    Victim_Race STRING,
    Victim_Ethnicity STRING,
    Victim_Gender STRING,
    Responding_Officer_1_Badge_No STRING,
    Responding_Officer_1_Name STRING,
    Responding_Officer_2_Badge_No STRING,
    Responding_Officer_2_Name STRING,
    Reporting_Officer_Badge_No STRING,
    Assisting_Officer_Badge_No STRING,
    Reviewing_Officer_Badge_No STRING,
    Element_Number_Assigned STRING,
    Investigating_Unit_1 STRING,
    Investigating_Unit_2 STRING,
    Offense_Status STRING,
    UCR_Disposition STRING,
    Modus_Operandi_MO STRING,
    Family_Offense STRING,
    Hate_Crime STRING,
    Hate_Crime_Description STRING,
    Weapon_Used STRING,
    Gang_Related_Offense STRING,
    Drug_Related_Incident STRING,
    RMS_Code STRING,
    Criminal_Justice_Information_Service_Code STRING,
    Penal_Code STRING,
    UCR_Offense_Name STRING,
    UCR_Offense_Description STRING,
    UCR_Code STRING,
    Offense_Type STRING,
    NIBRS_Crime STRING,
    NIBRS_Crime_Category STRING,
    NIBRS_Crime_Against STRING,
    NIBRS_Code STRING,
    NIBRS_Group STRING,
    NIBRS_Type STRING,
    Update_Date STRING,
    X_Coordinate STRING,
    Y_Coordinate STRING,
    Zip_Code STRING,
    City STRING,
    State STRING,
    Location1 STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",            
   "quoteChar"     = "\""                    
)
STORED AS TEXTFILE LOCATION '/user/dkim171/Dallas/DallasCrime/'
TBLPROPERTIES ('skip.header.line.count'='1');
"
-- check if table exists
show tables;
select * from dallas_crime LIMIT 10;

-- create dallas_population table
DROP TABLE IF EXISTS dallas_population;
CREATE EXTERNAL TABLE IF NOT EXISTS dallas_population(
    Year INT,
    Population INT,
    `Year on Year Change` STRING,  
    `Change in Percent` STRING     
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",            
   "quoteChar"     = "\""                  
)
STORED AS TEXTFILE LOCATION '/user/dkim171/Dallas/DallasoPop/'
TBLPROPERTIES ('skip.header.line.count'='1');
"
-- check if table exists
show tables;
select * from dallas_population;

----------------------------------------------------------------------------------
-- 6. Create union table for LA crimes
----------------------------------------------------------------------------------

-- create on union tabelfor la_crime_data
DROP TABLE IF EXISTS la_crime_union;
CREATE TABLE la_crime_union AS
SELECT *
FROM la_crime_10_19
UNION ALL
SELECT *
FROM la_crime_present;

-- check if table exists
show tables;
select * from la_crime_union LIMIT 10;

----------------------------------------------------------------------------------
-- 7. Select necessary columns only and create Category column with Case When, 
--    add city, add cast(substr()) as year. 
----------------------------------------------------------------------------------
-- Create table with necessary columns la_only_nessasary_columns

----------------------------------------------------------------------------------
-- LA final table creation -------------------------------------------------------
----------------------------------------------------------------------------------
DROP TABLE IF EXISTS la_only_nessasary_columns;
CREATE TABLE la_only_nessasary_columns AS
SELECT 
    CAST((SUBSTR(Date_rpt, 7, 4)) AS INT)AS `year`, 
    CAST(CONCAT(SUBSTR(date_rpt, 7, 4), '-', SUBSTR(date_rpt, 1, 2), '-', SUBSTR(date_rpt, 4, 2)) AS DATE) AS full_date,
    Crm_Cd,
    crm_cd_desc,
    DR_NO,
    'Los Angeles' AS city,
    LAT,  
    LON  
FROM la_crime_union;

-- Check la_only_nessasary_columns 
select * from la_only_nessasary_columns LIMIT 100;

-- create table la_crimes_with_population
DROP TABLE IF EXISTS la_crimes_with_population;
CREATE TABLE la_crimes_with_population AS
SELECT
   c.year,
   c.full_date,
   c.Crm_Cd,
   c.crm_Cd_desc,
   c.DR_NO,
   c.city,
   p.population,
   c.LAT, 
   c.LON
FROM
   la_only_nessasary_columns c
LEFT JOIN
   la_population p
ON
   c.year = p.year
ORDER BY
   YEAR(c.full_date);

-- check la_crimes_with_population
show tables;
select * from la_crimes_with_population LIMIT 100;

-- -- Create the new table crime code
-- DROP TABLE IF EXISTS la_crime_cod;
-- CREATE TABLE la_crime_cod 
-- ROW FORMAT DELIMITED 
-- FIELDS TERMINATED BY ',' 
-- STORED AS TEXTFILE 
-- LOCATION '/user/jlee464/Project/la_new' 
-- AS 
-- SELECT DISTINCT 
--     c.Crm_Cd, 
--     c.crm_cd_desc
-- FROM la_cri_pop c;

-- -- check la_output
-- show tables;
-- select * from la_cricd LIMIT 5;

-- check la_output
show tables;
select * from la_cricd LIMIT 5;

------------------------------------------------------------------------
-- Our Version create final file
------------------------------------------------------------------------
-- create final file
-- Create the new table la_output
-- DROP TABLE IF EXISTS la_output;
-- CREATE TABLE la_output AS
-- SELECT
--     full_date,
--     CASE
--         -- Crimes Against Persons
--         WHEN crm_cd_desc LIKE '%ASSAULT%' 
--             OR crm_cd_desc LIKE '%HOMICIDE%' 
--             OR crm_cd_desc LIKE '%BATTERY%' 
--             OR crm_cd_desc LIKE '%KIDNAPPING%'
--             OR crm_cd_desc LIKE '%SEXUAL%' 
--             OR crm_cd_desc LIKE '%RAPE%' 
--             OR crm_cd_desc LIKE '%CHILD ABUSE%' 
--             OR crm_cd_desc LIKE '%INTIMATE PARTNER%' 
--             OR crm_cd_desc LIKE '%EXTORTION%' 
--             OR crm_cd_desc LIKE '%BRANDISH WEAPON%'
--         THEN 'Crimes Against Persons'
        
--         -- Financial & Fraud-Related Crimes
--         WHEN crm_cd_desc LIKE '%THEFT OF IDENTITY%' 
--             OR crm_cd_desc LIKE '%FORGERY%' 
--             OR crm_cd_desc LIKE '%FRAUD%' 
--             OR crm_cd_desc LIKE '%STOLEN%'
--         THEN 'Financial & Fraud-Related Crimes'
        
--         -- Property Crimes
--         WHEN crm_cd_desc LIKE '%BURGLARY%' 
--             OR crm_cd_desc LIKE '%SHOPLIFTING%' 
--             OR crm_cd_desc LIKE '%THEFT%' 
--             OR crm_cd_desc LIKE '%VANDALISM%' 
--             OR crm_cd_desc LIKE '%TRESPASSING%' 
--             OR crm_cd_desc LIKE '%VEHICLE%'
--         THEN 'Property Crimes'
        
--         -- Weapon-Related Crimes
--         WHEN crm_cd_desc LIKE '%DEADLY WEAPON%' 
--             OR crm_cd_desc LIKE '%BRANDISH%' 
--             OR crm_cd_desc LIKE '%WEAPON%'
--         THEN 'Weapon-Related Crimes'
        
--         -- Public Order Crimes
--         WHEN crm_cd_desc LIKE '%DISTURBING THE PEACE%' 
--             OR crm_cd_desc LIKE '%TRESPASSING%'
--         THEN 'Public Order Crimes'
        
--         -- Miscellaneous Crimes
--         WHEN crm_cd_desc LIKE '%CRUELTY%' 
--             OR crm_cd_desc LIKE '%BOMB%' 
--             OR crm_cd_desc LIKE '%OTHER%'
--         THEN 'Miscellaneous'
        
--         -- Unknown Category
--         ELSE 'Unknown Category'
--     END AS crime_category,
--     crm_cd_desc,
--     dr_no,
--     city,
--     LAT,  
--     LON,
--     population
-- FROM la_crimes_with_population
-- ORDER BY full_date;

-- check la_output
-- show tables;
-- select * from la_output LIMIT 300;

------------------------------------------------------------------------
-- Jae Version create final file
------------------------------------------------------------------------
-- Create LA Master Table with 7 categories
/* Public Order Crimes, Financial & Fraud-Related Crimes, Crimes Against Persons, Weapon-Related Crimes, Property Crimes, Unknown Category, Miscellaneous */

DROP TABLE IF EXISTS la_cri_pop_cat;
CREATE TABLE la_cri_pop_cat AS
SELECT 
   c.year,
   c.full_date,
   c.Crm_Cd,
   c.crm_Cd_desc,
   c.DR_NO,
   c.city,
   c.LAT, 
   c.LON,
 FORMAT_NUMBER(CAST(REGEXP_REPLACE(c.population, '[^0-9]', '') AS BIGINT), 0) AS population,
    CASE 
        WHEN c.crm_Cd IN (110, 113, 121, 122, 210, 220, 230, 231, 235, 236, 622, 623, 624, 625, 626, 627, 910, 920, 921, 922) 
            THEN 'Crimes Against Persons'
        WHEN c.crm_Cd IN (310, 320, 330, 341, 343, 350, 351, 352, 354, 420, 440, 470, 510, 485, 610) 
            THEN 'Property Crimes'
        WHEN c.crm_Cd IN (653, 660, 661, 668, 662, 649) 
            THEN 'Financial & Fraud-Related Crimes'
        WHEN c.crm_Cd IN (250, 251, 756) 
            THEN 'Weapon-Related Crimes'
        WHEN c.crm_Cd IN (880, 882, 884, 888) 
            THEN 'Public Order Crimes'
        WHEN c.crm_Cd IN (944, 948) 
            THEN 'Miscellaneous'
        ELSE 'Unknown Category'
    END AS Category
FROM 
    la_crimes_with_population c;

-- check la_output
show tables;
select * from la_cri_pop_cat LIMIT 300;

----------------------------------------------------------------------------------
-- Jae code 
----------------------------------------------------------------------------------
-- Create Anova test table
DROP TABLE IF EXISTS la_anova;
CREATE TABLE la_anova AS
SELECT 
c.year, c.Category, c.population ,COUNT(DISTINCT c.DR_NO) AS cnt, c.city
FROM la_cri_pop_cat c
Where c.year between 2014 and 2023
Group By c.year, c.Category, c.population, c.city;


select * from la_anova Order by `year`;

-- Create LA Propostion 47 
DROP TABLE IF EXISTS la_cri_pop_mis;
CREATE TABLE la_cri_pop_mis 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/dkim171/Project/la_prop47' 
AS 
SELECT  
    c.year,
    c.full_date,
    c.dr_no,
    c.city,
    c.lat,
    c.lon,
    CASE 
        WHEN c.crm_cd IN (420, 440, 442, 471, 474, 654, 670, 951) THEN 'Proposition 47'
        ELSE 'Not Proposition 47'
    END AS prop_47_status,
    CAST(REGEXP_REPLACE(c.population, '[^0-9]', '') AS BIGINT) AS population,
    c.crm_cd,
    c.crm_cd_desc
FROM 
    la_crimes_with_population c 
WHERE 
    c.crm_cd IN (420, 440, 442, 471, 474, 654, 670, 951);


-- check table la_cri_pop_mis
show tables;
Select * from la_cri_pop_mis limit 100;

-- check the if the table saved on the right location
hdfs dfs -ls Project/la_prop47

-- copy a final la_cri_pop_mis file from hdfs to linux
hdfs dfs -get Project/la_prop47/000000_0

--download file
scp dkim171@129.146.230.230:/home/dkim171/000000_0 ~/Desktop/

----------------------------------------------------------------------------------
-- Chicago final table creation --------------------------------------------------
----------------------------------------------------------------------------------
DROP TABLE IF EXISTS chicago_only_nessasary_columns;
CREATE TABLE chicago_only_nessasary_columns AS
SELECT
    ID,
    Case_Number,
    CAST((SUBSTR(`date`, 7, 4)) AS INT)AS c_year,
    CAST(CONCAT(SUBSTR(`date`, 7, 4), '-', SUBSTR(`date`, 1, 2), '-', SUBSTR(`date`, 4, 2)) AS DATE) AS formatted_date,
    description,
    'Chicago' as city
FROM chicago_crime;

-- Check table chicago_only_nessasary_columns 
select * from chicago_only_nessasary_columns LIMIT 100;

-- create chicago_crimes_with_population table
DROP TABLE IF EXISTS chicago_crimes_with_population;
CREATE TABLE chicago_crimes_with_population AS
SELECT
    c.ID,
    c.Case_Number,
    c.c_year,
    c.formatted_date,
    c.description,
    c.city,
    p.Population
FROM
    chicago_only_nessasary_columns c
JOIN
    chicago_population p
ON
    YEAR(c.formatted_date) = p.Year  
ORDER BY
    c.formatted_date;

-- check la_crimes_with_population
show tables;
select * from chicago_crimes_with_population LIMIT 100;

----------------------------------------------------------------
-- My version
----------------------------------------------------------------
-- create final table with categories chicago_output table


-- DROP TABLE IF EXISTS chicago_output;
-- CREATE TABLE chicago_output AS
-- SELECT 
--     id,
--     case_number,
--     formatted_date,
--     description,
--     CASE
--         -- Crimes Against Persons
--         WHEN description LIKE '%AGGRAVATED%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%SIMPLE%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%STRONGARM%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%SEX ASSLT%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%CRIMINAL SEXUAL%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%CHILD%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%PO HANDS%' THEN 'Crimes Against Persons'
--         WHEN description LIKE '%KNIFE%' THEN 'Crimes Against Persons'

--         -- Financial & Fraud-Related Crimes
--         WHEN description LIKE '%FINANCIAL%' THEN 'Financial & Fraud-Related Crimes'
--         WHEN description LIKE '%FRAUD%' THEN 'Financial & Fraud-Related Crimes'
--         WHEN description LIKE '%CREDIT CARD%' THEN 'Financial & Fraud-Related Crimes'
--         WHEN description LIKE '%EMBEZZLEMENT%' THEN 'Financial & Fraud-Related Crimes'
--         WHEN description LIKE '%FORGERY%' THEN 'Financial & Fraud-Related Crimes'
--         WHEN description LIKE '%THEFT OF LABOR%' THEN 'Financial & Fraud-Related Crimes'

--         -- Miscellaneous
--         WHEN description LIKE '%TELEPHONE%' THEN 'Miscellaneous'
--         WHEN description LIKE '%HARASSMENT%' THEN 'Miscellaneous'
--         WHEN description LIKE '%SOLICIT%' THEN 'Miscellaneous'
--         WHEN description LIKE '%CONTRIBUTE%' THEN 'Miscellaneous'
--         WHEN description LIKE '%VIOLATE ORDER%' THEN 'Miscellaneous'
--         WHEN description LIKE '%OTHER CRIME%' THEN 'Miscellaneous'

--         -- Property Crimes
--         WHEN description LIKE '%$500%' THEN 'Property Crimes'
--         WHEN description LIKE '%OVER $500%' THEN 'Property Crimes'
--         WHEN description LIKE '%TO VEHICLE%' THEN 'Property Crimes'
--         WHEN description LIKE '%TO PROPERTY%' THEN 'Property Crimes'
--         WHEN description LIKE '%AUTOMOBILE%' THEN 'Property Crimes'
--         WHEN description LIKE '%FORCIBLE ENTRY%' THEN 'Property Crimes'
--         WHEN description LIKE '%UNLAWFUL ENTRY%' THEN 'Property Crimes'

--         -- Public Order Crimes
--         WHEN description LIKE '%POSS%' THEN 'Public Order Crimes'
--         WHEN description LIKE '%MANU/DELIVER%' THEN 'Public Order Crimes'
--         WHEN description LIKE '%UNLAWFUL POSS%' THEN 'Public Order Crimes'
--         WHEN description LIKE '%LIQUOR LICENSE%' THEN 'Public Order Crimes'

--         -- Unknown Category
--         WHEN description LIKE '%PREDATORY%' THEN 'Unknown Category'
--         WHEN description LIKE '%NON-AGGRAVATED%' THEN 'Unknown Category'
--         WHEN description LIKE '%BOGUS CHECK%' THEN 'Unknown Category'
--         WHEN description LIKE '%TRUCK%' THEN 'Unknown Category'

--         -- Weapon-Related Crimes
--         WHEN description LIKE '%HANDGUN%' THEN 'Weapon-Related Crimes'
--         WHEN description LIKE '%KNIFE%' THEN 'Weapon-Related Crimes'
--         WHEN description LIKE '%DANG WEAPON%' THEN 'Weapon-Related Crimes'

--         ELSE 'Unknown Category'
--     END AS crime_category,
--     population
-- FROM chicago_crimes_with_population;

-- -- check chicago_output table
-- select * from chicago_output LIMIT 300;


----------------------------------------------------------------
-- Jae version 
----------------------------------------------------------------

DROP TABLE IF EXISTS chi_anova;
CREATE TABLE chi_anova AS
SELECT
    c.c_year,
    CASE
       -- Crimes Against Persons
       WHEN description LIKE '%AGGRAVATED%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%SIMPLE%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%STRONGARM%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%SEX ASSLT%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%CRIMINAL SEXUAL%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%CHILD%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%PO HANDS%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%KNIFE%' THEN 'Crimes Against Persons'


       -- Financial & Fraud-Related Crimes
       WHEN description LIKE '%FINANCIAL%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%FRAUD%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%CREDIT CARD%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%EMBEZZLEMENT%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%FORGERY%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%THEFT OF LABOR%' THEN 'Financial & Fraud-Related Crimes'

       -- Miscellaneous
       WHEN description LIKE '%TELEPHONE%' THEN 'Miscellaneous'
       WHEN description LIKE '%HARASSMENT%' THEN 'Miscellaneous'
       WHEN description LIKE '%SOLICIT%' THEN 'Miscellaneous'
       WHEN description LIKE '%CONTRIBUTE%' THEN 'Miscellaneous'
       WHEN description LIKE '%VIOLATE ORDER%' THEN 'Miscellaneous'
       WHEN description LIKE '%OTHER CRIME%' THEN 'Miscellaneous'

       -- Property Crimes
       WHEN description LIKE '%$500%' THEN 'Property Crimes'
       WHEN description LIKE '%OVER $500%' THEN 'Property Crimes'
       WHEN description LIKE '%TO VEHICLE%' THEN 'Property Crimes'
       WHEN description LIKE '%TO PROPERTY%' THEN 'Property Crimes'
       WHEN description LIKE '%AUTOMOBILE%' THEN 'Property Crimes'
       WHEN description LIKE '%FORCIBLE ENTRY%' THEN 'Property Crimes'
       WHEN description LIKE '%UNLAWFUL ENTRY%' THEN 'Property Crimes'

       -- Public Order Crimes
       WHEN description LIKE '%POSS%' THEN 'Public Order Crimes'
       WHEN description LIKE '%MANU/DELIVER%' THEN 'Public Order Crimes'
       WHEN description LIKE '%UNLAWFUL POSS%' THEN 'Public Order Crimes'
       WHEN description LIKE '%LIQUOR LICENSE%' THEN 'Public Order Crimes'

       -- Unknown Category
       WHEN description LIKE '%PREDATORY%' THEN 'Unknown Category'
       WHEN description LIKE '%NON-AGGRAVATED%' THEN 'Unknown Category'
       WHEN description LIKE '%BOGUS CHECK%' THEN 'Unknown Category'
       WHEN description LIKE '%TRUCK%' THEN 'Unknown Category'

       -- Weapon-Related Crimes
       WHEN description LIKE '%HANDGUN%' THEN 'Weapon-Related Crimes'
       WHEN description LIKE '%KNIFE%' THEN 'Weapon-Related Crimes'
       WHEN description LIKE '%DANG WEAPON%' THEN 'Weapon-Related Crimes'

       ELSE 'Unknown Category'
   END AS category,
   c.population,
   count (DISTINCT case_number) AS cnt,
   c.city
FROM chicago_crimes_with_population c
where c.c_year between 2014 and 2023
Group by 
c.c_year,
c.population,
   CASE
       -- Crimes Against Persons
       WHEN description LIKE '%AGGRAVATED%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%SIMPLE%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%STRONGARM%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%SEX ASSLT%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%CRIMINAL SEXUAL%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%CHILD%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%PO HANDS%' THEN 'Crimes Against Persons'
       WHEN description LIKE '%KNIFE%' THEN 'Crimes Against Persons'


       -- Financial & Fraud-Related Crimes
       WHEN description LIKE '%FINANCIAL%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%FRAUD%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%CREDIT CARD%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%EMBEZZLEMENT%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%FORGERY%' THEN 'Financial & Fraud-Related Crimes'
       WHEN description LIKE '%THEFT OF LABOR%' THEN 'Financial & Fraud-Related Crimes'


       -- Miscellaneous
       WHEN description LIKE '%TELEPHONE%' THEN 'Miscellaneous'
       WHEN description LIKE '%HARASSMENT%' THEN 'Miscellaneous'
       WHEN description LIKE '%SOLICIT%' THEN 'Miscellaneous'
       WHEN description LIKE '%CONTRIBUTE%' THEN 'Miscellaneous'
       WHEN description LIKE '%VIOLATE ORDER%' THEN 'Miscellaneous'
       WHEN description LIKE '%OTHER CRIME%' THEN 'Miscellaneous'


       -- Property Crimes
       WHEN description LIKE '%$500%' THEN 'Property Crimes'
       WHEN description LIKE '%OVER $500%' THEN 'Property Crimes'
       WHEN description LIKE '%TO VEHICLE%' THEN 'Property Crimes'
       WHEN description LIKE '%TO PROPERTY%' THEN 'Property Crimes'
       WHEN description LIKE '%AUTOMOBILE%' THEN 'Property Crimes'
       WHEN description LIKE '%FORCIBLE ENTRY%' THEN 'Property Crimes'
       WHEN description LIKE '%UNLAWFUL ENTRY%' THEN 'Property Crimes'


       -- Public Order Crimes
       WHEN description LIKE '%POSS%' THEN 'Public Order Crimes'
       WHEN description LIKE '%MANU/DELIVER%' THEN 'Public Order Crimes'
       WHEN description LIKE '%UNLAWFUL POSS%' THEN 'Public Order Crimes'
       WHEN description LIKE '%LIQUOR LICENSE%' THEN 'Public Order Crimes'


       -- Unknown Category
       WHEN description LIKE '%PREDATORY%' THEN 'Unknown Category'
       WHEN description LIKE '%NON-AGGRAVATED%' THEN 'Unknown Category'
       WHEN description LIKE '%BOGUS CHECK%' THEN 'Unknown Category'
       WHEN description LIKE '%TRUCK%' THEN 'Unknown Category'


       -- Weapon-Related Crimes
       WHEN description LIKE '%HANDGUN%' THEN 'Weapon-Related Crimes'
       WHEN description LIKE '%KNIFE%' THEN 'Weapon-Related Crimes'
       WHEN description LIKE '%DANG WEAPON%' THEN 'Weapon-Related Crimes'


       ELSE 'Unknown Category'
   END,
   c.city;


-- check chicago_anova table
select * from chi_anova LIMIT 7;

----------------------------------------------------------------------------------
-- Dallas final table creation ---------------------------------------------------
----------------------------------------------------------------------------------
-- create clear table without null

----------------------------------------------------------------------------------
-- My version
----------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dallas_only_nessasary_columns;
-- CREATE TABLE dallas_only_nessasary_columns AS
-- SELECT 
--     Incident_Number,
--     CAST(date1_of_occurrence AS DATE) AS formatted_date,
--     type_of_incident
-- from dallas_crime where incident_number not in('DALLAS') and incident_number not LIKE '(%';


-- check if table exists
-- show tables;
-- select * from dallas_only_nessasary_columns LIMIT 10;

----------------------------------------------------------------------------------
-- Jae version
----------------------------------------------------------------------------------
-- create clear table without null
DROP TABLE IF EXISTS dal_crisim;
CREATE TABLE dal_crisim AS
SELECT
   Incident_Number,
   cast(Year_of_Incident AS int) AS c_year,
   type_of_incident
from dallas_crime where incident_number not in('DALLAS') and incident_number not LIKE '(%';

-- check if table exists
show tables;
select * from dal_crisim LIMIT 10;

-- select distinct type_of_incident
DROP TABLE IF EXISTS dal_cridesc;
CREATE TABLE dal_cridesc
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/dkim171/Project/dal_new' 
AS 
select distinct type_of_incident from dal_crisim;

-- remove previous file from the server 
rm 000000_0

-- check if file exists
hdfs dfs -ls Project/dal_new

-- move file to the server
hdfs dfs -get Project/dal_new/000000_0

-- move file to the local pc
scp dkim171@129.146.230.230:/home/dkim171/000000_0 ~/Desktop/

----------------------------------------------------------------
-- My version
----------------------------------------------------------------
-- create final table dallas_output
-- DROP TABLE IF EXISTS dallas_output;
-- CREATE TABLE dallas_output AS
-- SELECT 
--     incident_number, 
--     formatted_date, 
--     type_of_incident,
--     CASE 
--         -- Crimes Against Persons
--         WHEN type_of_incident LIKE '%ASSAULT%' 
--             OR type_of_incident LIKE '%HOMICIDE%' 
--             OR type_of_incident LIKE '%MURDER%' 
--             OR type_of_incident LIKE '%KIDNAPPING%' 
--             OR type_of_incident LIKE '%MANSLAUGHTER%' 
--             OR type_of_incident LIKE '%DEADLY CONDUCT%' 
--             OR type_of_incident LIKE '%SEXUAL%' 
--             OR type_of_incident LIKE '%TERRORISTIC THREAT%' 
--         THEN 'Crimes Against Persons'
        
--         -- Financial & Fraud-Related Crimes
--         WHEN type_of_incident LIKE '%FRAUD%' 
--             OR type_of_incident LIKE '%THEFT OF PROP%' 
--             OR type_of_incident LIKE '%FORGERY%' 
--             OR type_of_incident LIKE '%CREDIT CARD%' 
--             OR type_of_incident LIKE '%TRADEMARK%' 
--         THEN 'Financial & Fraud-Related Crimes'
        
--         -- Miscellaneous
--         WHEN type_of_incident LIKE '%MISCELLANEOUS%' 
--             OR type_of_incident LIKE '%LOST PROPERTY%' 
--             OR type_of_incident LIKE '%SUSPICIOUS%' 
--             OR type_of_incident LIKE '%RECOVERED%'
--         THEN 'Miscellaneous'
        
--         -- Property Crimes
--         WHEN type_of_incident LIKE '%BURGLARY%' 
--             OR type_of_incident LIKE '%THEFT%' 
--             OR type_of_incident LIKE '%ARSON%' 
--             OR type_of_incident LIKE '%CRIM MISCHIEF%' 
--         THEN 'Property Crimes'
        
--         -- Public Order Crimes
--         WHEN type_of_incident LIKE '%PUBLIC INTOX%' 
--             OR type_of_incident LIKE '%DISORDERLY CONDUCT%' 
--             OR type_of_incident LIKE '%GAMBLING%' 
--             OR type_of_incident LIKE '%HARASSMENT%' 
--             OR type_of_incident LIKE '%URINATING%' 
--         THEN 'Public Order Crimes'
        
--         -- Weapon-Related Crimes
--         WHEN type_of_incident LIKE '%UNLAWFUL CARRY%' 
--             OR type_of_incident LIKE '%WEAPON%' 
--             OR type_of_incident LIKE '%FIREARM%' 
--             OR type_of_incident LIKE '%KNIFE%' 
--         THEN 'Weapon-Related Crimes'
        
--         -- Unknown Category
--         ELSE 'Unknown Category'
--     END AS incident_category
-- FROM dallas_only_nessasary_columns;

-- -- check if table exists
-- show tables;
-- select * from dallas_output LIMIT 10;

----------------------------------------------------------------
-- Jae version full to the end of the file
----------------------------------------------------------------

-- create final table dallas_anova
DROP TABLE IF EXISTS dal_anova;
CREATE TABLE dal_anova AS
SELECT
  c_year,
  category,
  population,
  cnt,
  'Dallas' AS city
FROM (
  SELECT
    c.c_year,
    CASE
      -- Crimes Against Persons
      WHEN c.type_of_incident LIKE '%ASSAULT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%HOMICIDE%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%KIDNAPPING%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%DEADLY CONDUCT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%THREAT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%INJURY%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%MANSLAUGHTER%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%SEXUAL ASSAULT%' THEN 'Crimes Against Persons'
      
      -- Property Crimes
      WHEN c.type_of_incident LIKE '%THEFT%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%BURGLARY%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%ROBBERY%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%GRAFFITI%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%VANDALISM%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%CRIMINAL MISCHIEF%' THEN 'Property Crimes'
      
      -- Financial & Fraud-Related Crimes
      WHEN c.type_of_incident LIKE '%FRAUD%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%EMBEZZLEMENT%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%FORGERY%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%CREDIT CARD%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%FINANCIAL%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%COUNTERFEIT%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%TAMPER%' THEN 'Financial & Fraud-Related Crimes'
      
      -- Weapon-Related Crimes
      WHEN c.type_of_incident LIKE '%FIREARM%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%WEAPON%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%KNIFE%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%UNLAWFUL CARRY%' THEN 'Weapon-Related Crimes'
      
      -- Public Order Crimes
      WHEN c.type_of_incident LIKE '%DISORDERLY CONDUCT%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%PUBLIC INTOXICATION%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%LOITERING%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%RIOT%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%HARASSMENT%' THEN 'Public Order Crimes'
      
      -- Miscellaneous
      WHEN c.type_of_incident LIKE '%GAMBLING%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%DUMPING%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%UNAUTHORIZED USE%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%UNREGISTERED%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%UNLAWFUL%' THEN 'Miscellaneous'
      
      -- Unknown Category
      ELSE 'Unknown Category'
    END AS category,
    p.population,
    COUNT(DISTINCT c.incident_number) AS cnt
  FROM
    dal_crisim c
  LEFT JOIN
    dallas_population p ON c.c_year = p.year
  WHERE
    c.c_year IS NOT NULL
    AND c.c_year BETWEEN 2014 AND 2023
  GROUP BY
    c.c_year,
    CASE
      WHEN c.type_of_incident LIKE '%ASSAULT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%HOMICIDE%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%KIDNAPPING%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%DEADLY CONDUCT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%THREAT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%INJURY%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%MANSLAUGHTER%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%SEXUAL ASSAULT%' THEN 'Crimes Against Persons'
      WHEN c.type_of_incident LIKE '%THEFT%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%BURGLARY%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%ROBBERY%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%GRAFFITI%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%VANDALISM%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%CRIMINAL MISCHIEF%' THEN 'Property Crimes'
      WHEN c.type_of_incident LIKE '%FRAUD%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%EMBEZZLEMENT%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%FORGERY%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%CREDIT CARD%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%FINANCIAL%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%COUNTERFEIT%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%TAMPER%' THEN 'Financial & Fraud-Related Crimes'
      WHEN c.type_of_incident LIKE '%FIREARM%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%WEAPON%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%KNIFE%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%UNLAWFUL CARRY%' THEN 'Weapon-Related Crimes'
      WHEN c.type_of_incident LIKE '%DISORDERLY CONDUCT%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%PUBLIC INTOXICATION%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%LOITERING%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%RIOT%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%HARASSMENT%' THEN 'Public Order Crimes'
      WHEN c.type_of_incident LIKE '%GAMBLING%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%DUMPING%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%UNAUTHORIZED USE%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%UNREGISTERED%' THEN 'Miscellaneous'
      WHEN c.type_of_incident LIKE '%UNLAWFUL%' THEN 'Miscellaneous'
      ELSE 'Unknown Category'
    END,
    p.population
) subquery;

-- check if table exists
show tables;
select * from dal_anova LIMIT 10;
select DISTINCT c_year from dal_anova;


----------------------------------------------------------------------------------
-- Austin Anova table creation ---------------------------------------------------
----------------------------------------------------------------------------------
-- create clear table without null

DROP TABLE IF EXISTS aus_crisim;
CREATE TABLE aus_crisim AS
SELECT
    Incident_Number,
    Highest_Offense_Description,
    Highest_Offense_Code,
    CAST(SUBSTR(Occurred_Date, 7, 4) AS INT) AS c_year,
    'Austin' AS city
FROM
    austin_crime;

-- check table aus_crisim
select * from aus_crisim limit 5;

-- select distinct type_of_incident
DROP TABLE IF EXISTS aus_cridesc;
CREATE TABLE aus_cridesc
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/dkim171/Project/aus_new' 
AS 
select distinct Highest_Offense_Code, Highest_Offense_Description from aus_crisim;

--download file to prepare categories
ls
rm 000000_0
hdfs dfs -ls Project/aus_new
hdfs dfs -get Project/aus_new/000000_0
scp dkim171@129.146.230.230:/home/dkim171/000000_0 ~/Desktop/

-- create final table Austin_anova

DROP TABLE IF EXISTS aus_anova;
CREATE TABLE aus_anova AS 
SELECT 
    c.c_year,
CASE
  WHEN c.Highest_Offense_Code IN (100, 101, 102, 103, 104, 106, 108, 202, 206, 2105, 2109, 2801, 300, 302, 303, 305, 402, 403, 406, 407, 408, 411, 900, 901, 902, 903, 906, 909, 911, 2011, 2012, 2013, 405, 4111) THEN 'Violent Crimes'
  WHEN c.Highest_Offense_Code IN (200, 202, 204, 206, 208, 8905, 1600, 1601, 1602, 1603, 1604, 1700, 1701, 1706, 1707, 1709, 1710, 1712, 1715, 1718, 1719, 1722, 1724, 1725, 2014, 2600, 2601, 2602, 2603, 2605, 2609, 2610, 2611, 2612, 2613, 2614, 4199) THEN 'Sexual Crimes'
  WHEN c.Highest_Offense_Code IN (500, 501, 502, 504, 600, 601, 602, 603, 604, 605, 606, 608, 609, 611, 612, 613, 614, 615, 617, 618, 619, 620, 621, 622, 700, 800, 802, 803, 804, 8503, 1400, 1401, 1402, 3817, 3832, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1103, 1104, 1105, 1106, 1112, 1113, 1114, 1198, 1199, 1200, 1201, 1202, 1300, 4022, 4027) THEN 'Property Crimes'
  WHEN c.Highest_Offense_Code IN (1800, 1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815, 1818, 1819, 1820, 1821, 1822, 1823, 1824, 1825, 1826, 1827, 1828) THEN 'Drug Crimes'
  WHEN c.Highest_Offense_Code IN (2100, 2102, 2103, 2104, 2105, 2106, 2107, 2108, 2109, 2110, 2111, 2724, 2728, 3604) THEN 'Traffic Offenses'
  WHEN c.Highest_Offense_Code IN (2400, 2401, 2402, 2403, 2404, 2405, 2406, 2408, 2409, 2410, 2411, 2413, 2415, 2416, 2417, 2500, 2606, 2732, 2735, 2736, 3212, 3213, 3215, 3216, 3220, 3221, 3223, 3294, 3295, 3296, 3297, 3298, 3299, 3300, 3301, 3302, 3303, 3304, 3305, 3306, 3400, 3401, 3402, 3414, 3442, 3458, 3459, 3720, 3722, 3724, 3829) THEN 'Public Order Crimes'
  ELSE 'Other Crimes'
END AS Category,
p.population,
count(DISTINCT c.Incident_Number) as cnt,
c.city
from aus_crisim c
Left Join austin_population p on c.c_year = p.year
where c.c_year between 2014 and 2023
GROUP By 
    c.c_year,
CASE
  WHEN c.Highest_Offense_Code IN (100, 101, 102, 103, 104, 106, 108, 202, 206, 2105, 2109, 2801, 300, 302, 303, 305, 402, 403, 406, 407, 408, 411, 900, 901, 902, 903, 906, 909, 911, 2011, 2012, 2013, 405, 4111) THEN 'Violent Crimes'
  WHEN c.Highest_Offense_Code IN (200, 202, 204, 206, 208, 8905, 1600, 1601, 1602, 1603, 1604, 1700, 1701, 1706, 1707, 1709, 1710, 1712, 1715, 1718, 1719, 1722, 1724, 1725, 2014, 2600, 2601, 2602, 2603, 2605, 2609, 2610, 2611, 2612, 2613, 2614, 4199) THEN 'Sexual Crimes'
  WHEN c.Highest_Offense_Code IN (500, 501, 502, 504, 600, 601, 602, 603, 604, 605, 606, 608, 609, 611, 612, 613, 614, 615, 617, 618, 619, 620, 621, 622, 700, 800, 802, 803, 804, 8503, 1400, 1401, 1402, 3817, 3832, 1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1103, 1104, 1105, 1106, 1112, 1113, 1114, 1198, 1199, 1200, 1201, 1202, 1300, 4022, 4027) THEN 'Property Crimes'
  WHEN c.Highest_Offense_Code IN (1800, 1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815, 1818, 1819, 1820, 1821, 1822, 1823, 1824, 1825, 1826, 1827, 1828) THEN 'Drug Crimes'
  WHEN c.Highest_Offense_Code IN (2100, 2102, 2103, 2104, 2105, 2106, 2107, 2108, 2109, 2110, 2111, 2724, 2728, 3604) THEN 'Traffic Offenses'
  WHEN c.Highest_Offense_Code IN (2400, 2401, 2402, 2403, 2404, 2405, 2406, 2408, 2409, 2410, 2411, 2413, 2415, 2416, 2417, 2500, 2606, 2732, 2735, 2736, 3212, 3213, 3215, 3216, 3220, 3221, 3223, 3294, 3295, 3296, 3297, 3298, 3299, 3300, 3301, 3302, 3303, 3304, 3305, 3306, 3400, 3401, 3402, 3414, 3442, 3458, 3459, 3720, 3722, 3724, 3829) THEN 'Public Order Crimes'
  ELSE 'Other Crimes'
END,
p.population,
c.city;

SELECT * from aus_anova limit 5;
SELECT DISTINCT c_year from aus_anova;

--create combined_anova table
DROP TABLE IF EXISTS combined_anova;
CREATE TABLE combined_anova AS
SELECT * FROM la_anova
UNION ALL
SELECT * FROM chi_anova
UNION ALL
SELECT * FROM dal_anova
UNION ALL
SELECT * FROM aus_anova;

-- check table combined_anova
SELECT * FROM combined_anova;

--Export as csv file
DROP TABLE IF EXISTS com_anova;
CREATE TABLE com_anova
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
LOCATION '/user/dkim171/Project/anova' 
AS 
SELECT
  `year`,
  category,
  CAST(REGEXP_REPLACE(population, '[^0-9]', '') AS BIGINT) AS population,
  cnt,
  city
FROM 
  combined_anova;

-- check table com_anova;
SELECT * FROM com_anova;


rm 000000_0
hdfs dfs -ls Project/anova
hdfs dfs -get Project/anova/000000_0
scp dkim171@129.146.230.230:/home/dkim171/000000_0 ~/Desktop/