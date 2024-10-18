----------------------------------------------------------------------------------
-- 1. Upload data to Hadoop server
----------------------------------------------------------------------------------
scp "/Users/mityakim/Desktop/LA.zip" dkim171@129.146.230.230:/home/dkim171/
scp "/Users/mityakim/Desktop/Austin.zip" dkim171@129.146.230.230:/home/dkim171/
scp "/Users/mityakim/Desktop/Dallas.zip" dkim171@129.146.230.230:/home/dkim171/
scp "/Users/mityakim/Desktop/Chicago.zip" dkim171@129.146.230.230:/home/dkim171/

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
    Population STRING,
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
    Population STRING,
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
select * from chicago_population LIMIT 10;

--/-----------------------------------------------------------------------------------/-- 
--/----------------------------------Austin-------------------------------------------/-- 
--/-----------------------------------------------------------------------------------/-- 
-- create austin_crime table
DROP TABLE IF EXISTS austin_crime;
CREATE EXTERNAL TABLE IF NOT EXISTS austin_crime(
    Incident_Number INT,
    Incident_Type STRING,
    Council_District INT,
    Mental_Health_Flag STRING,
    Priority_Level STRING,
    Response_Datetime STRING,
    Response_Year STRING,
    Response_Month STRING,
    Response_Day_of_Week STRING,
    Response_Hour STRING,
    First_Unit_Arrived_Datetime STRING,
    Call_Closed_Datetime STRING,
    Sector STRING,
    Initial_Problem_Description STRING,
    Initial_Problem_Category STRING,
    Final_Problem_Description STRING,
    Final_Problem_Category STRING,
    Number_of_Units_Arrived STRING,
    Unit_Time_on_Scene STRING,
    Call_Disposition_Description STRING,
    Report_Written_Flag STRING,
    Response_Time STRING,
    Officer_Injured_Killed_Count STRING,
    Subject_Injured_Killed_Count STRING,
    Other_Injured_Killed_Count STRING,
    Geo_ID STRING,
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
    Population STRING,
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
    Population STRING,
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
SELECT DR_NO,
    Date_Rpt,
    DATE_OCC,
    TIME_OCC,
    AREA,
    AREA_NAME,
    RptDist_No,
    Part_1_2,
    Crm_Cd,
    Crm_Cd_Desc,
    Mocodes,
    Vict_Age,
    Vict_Sex,
    Vict_Descent,
    Premis_Cd,
    Premis_Desc,
    Weapon_Used_Cd,
    Weapon_Desc,
    Status,
    Status_Desc,
    Crm_Cd_1,
    Crm_Cd_2,
    Crm_Cd_3,
    Crm_Cd_4,
    LOCATION,
    Cross_Street,
    LAT,
    LON
FROM la_crime_10_19
UNION ALL
SELECT DR_NO,
    Date_Rpt,
    DATE_OCC,
    TIME_OCC,
    AREA,
    AREA_NAME,
    RptDist_No,
    Part_1_2,
    Crm_Cd,
    Crm_Cd_Desc,
    Mocodes,
    Vict_Age,
    Vict_Sex,
    Vict_Descent,
    Premis_Cd,
    Premis_Desc,
    Weapon_Used_Cd,
    Weapon_Desc,
    Status,
    Status_Desc,
    Crm_Cd_1,
    Crm_Cd_2,
    Crm_Cd_3,
    Crm_Cd_4,
    LOCATION,
    Cross_Street,
    LAT,
    LON
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
    CAST(CONCAT(SUBSTR(date_rpt, 7, 4), '-', SUBSTR(date_rpt, 1, 2), '-', SUBSTR(date_rpt, 4, 2)) AS DATE) AS full_date,
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
    c.full_date,
    c.crm_cd_desc,
    DR_NO,
    c.city,
    p.population,
    LAT,  
    LON,
    p.year AS population_year
FROM 
    la_only_nessasary_columns c
JOIN 
    la_population p
ON 
    YEAR(c.full_date) = p.year 
ORDER BY 
    YEAR(c.full_date);

-- check la_crimes_with_population
show tables;
select * from la_crimes_with_population LIMIT 100;

-- create final file
-- Create the new table la_output
DROP TABLE IF EXISTS la_output;
CREATE TABLE la_output AS
SELECT
    full_date,
    CASE
        -- Crimes Against Persons
        WHEN crm_cd_desc LIKE '%ASSAULT%' 
            OR crm_cd_desc LIKE '%HOMICIDE%' 
            OR crm_cd_desc LIKE '%BATTERY%' 
            OR crm_cd_desc LIKE '%KIDNAPPING%'
            OR crm_cd_desc LIKE '%SEXUAL%' 
            OR crm_cd_desc LIKE '%RAPE%' 
            OR crm_cd_desc LIKE '%CHILD ABUSE%' 
            OR crm_cd_desc LIKE '%INTIMATE PARTNER%' 
            OR crm_cd_desc LIKE '%EXTORTION%' 
            OR crm_cd_desc LIKE '%BRANDISH WEAPON%'
        THEN 'Crimes Against Persons'
        
        -- Financial & Fraud-Related Crimes
        WHEN crm_cd_desc LIKE '%THEFT OF IDENTITY%' 
            OR crm_cd_desc LIKE '%FORGERY%' 
            OR crm_cd_desc LIKE '%FRAUD%' 
            OR crm_cd_desc LIKE '%STOLEN%'
        THEN 'Financial & Fraud-Related Crimes'
        
        -- Property Crimes
        WHEN crm_cd_desc LIKE '%BURGLARY%' 
            OR crm_cd_desc LIKE '%SHOPLIFTING%' 
            OR crm_cd_desc LIKE '%THEFT%' 
            OR crm_cd_desc LIKE '%VANDALISM%' 
            OR crm_cd_desc LIKE '%TRESPASSING%' 
            OR crm_cd_desc LIKE '%VEHICLE%'
        THEN 'Property Crimes'
        
        -- Weapon-Related Crimes
        WHEN crm_cd_desc LIKE '%DEADLY WEAPON%' 
            OR crm_cd_desc LIKE '%BRANDISH%' 
            OR crm_cd_desc LIKE '%WEAPON%'
        THEN 'Weapon-Related Crimes'
        
        -- Public Order Crimes
        WHEN crm_cd_desc LIKE '%DISTURBING THE PEACE%' 
            OR crm_cd_desc LIKE '%TRESPASSING%'
        THEN 'Public Order Crimes'
        
        -- Miscellaneous Crimes
        WHEN crm_cd_desc LIKE '%CRUELTY%' 
            OR crm_cd_desc LIKE '%BOMB%' 
            OR crm_cd_desc LIKE '%OTHER%'
        THEN 'Miscellaneous'
        
        -- Unknown Category
        ELSE 'Unknown Category'
    END AS crime_category,
    crm_cd_desc,
    dr_no,
    city,
    LAT,  
    LON,
    population
FROM la_crimes_with_population
ORDER BY full_date;

-- check la_output
show tables;
select * from la_output LIMIT 300;


----------------------------------------------------------------------------------
-- Chicago final table creation --------------------------------------------------
----------------------------------------------------------------------------------
DROP TABLE IF EXISTS chicago_only_nessasary_columns;
CREATE TABLE chicago_only_nessasary_columns AS
SELECT
    ID,
    Case_Number,
    CAST(CONCAT(SUBSTR(`date`, 7, 4), '-', SUBSTR(`date`, 1, 2), '-', SUBSTR(`date`, 4, 2)) AS DATE) AS formatted_date,
    description
FROM chicago_crime;

select * from chicago_only_nessasary_columns LIMIT 100;

-- create chicago_crimes_with_population table
DROP TABLE IF EXISTS chicago_crimes_with_population;
CREATE TABLE chicago_crimes_with_population AS
SELECT
    c.ID,
    c.Case_Number,
    c.formatted_date,
    c.description,
    p.Population,
    p.Year AS population_year
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


-- create final table with categories chicago_output table

-- create final file
-- Create the new table la_output
DROP TABLE IF EXISTS chicago_output;
CREATE TABLE chicago_output AS
SELECT 
    id,
    case_number,
    formatted_date,
    description,
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
    END AS crime_category,
    population
FROM chicago_crimes_with_population;

-- check chicago_output table
select * from chicago_output LIMIT 300;

----------------------------------------------------------------------------------
-- Dallas final table creation ---------------------------------------------------
----------------------------------------------------------------------------------
-- create clear table without null
DROP TABLE IF EXISTS dallas_only_nessasary_columns;
CREATE TABLE dallas_only_nessasary_columns AS
SELECT 
    Incident_Number,
    CAST(date1_of_occurrence AS DATE) AS formatted_date,
    type_of_incident
from dallas_crime where incident_number not in('DALLAS') and incident_number not LIKE '(%';

-- check if table exists
show tables;
select * from dallas_only_nessasary_columns LIMIT 10;

-- create final table dallas_output
DROP TABLE IF EXISTS dallas_output;
CREATE TABLE dallas_output AS
SELECT 
    incident_number, 
    formatted_date, 
    type_of_incident,
    CASE 
        -- Crimes Against Persons
        WHEN type_of_incident LIKE '%ASSAULT%' 
            OR type_of_incident LIKE '%HOMICIDE%' 
            OR type_of_incident LIKE '%MURDER%' 
            OR type_of_incident LIKE '%KIDNAPPING%' 
            OR type_of_incident LIKE '%MANSLAUGHTER%' 
            OR type_of_incident LIKE '%DEADLY CONDUCT%' 
            OR type_of_incident LIKE '%SEXUAL%' 
            OR type_of_incident LIKE '%TERRORISTIC THREAT%' 
        THEN 'Crimes Against Persons'
        
        -- Financial & Fraud-Related Crimes
        WHEN type_of_incident LIKE '%FRAUD%' 
            OR type_of_incident LIKE '%THEFT OF PROP%' 
            OR type_of_incident LIKE '%FORGERY%' 
            OR type_of_incident LIKE '%CREDIT CARD%' 
            OR type_of_incident LIKE '%TRADEMARK%' 
        THEN 'Financial & Fraud-Related Crimes'
        
        -- Miscellaneous
        WHEN type_of_incident LIKE '%MISCELLANEOUS%' 
            OR type_of_incident LIKE '%LOST PROPERTY%' 
            OR type_of_incident LIKE '%SUSPICIOUS%' 
            OR type_of_incident LIKE '%RECOVERED%'
        THEN 'Miscellaneous'
        
        -- Property Crimes
        WHEN type_of_incident LIKE '%BURGLARY%' 
            OR type_of_incident LIKE '%THEFT%' 
            OR type_of_incident LIKE '%ARSON%' 
            OR type_of_incident LIKE '%CRIM MISCHIEF%' 
        THEN 'Property Crimes'
        
        -- Public Order Crimes
        WHEN type_of_incident LIKE '%PUBLIC INTOX%' 
            OR type_of_incident LIKE '%DISORDERLY CONDUCT%' 
            OR type_of_incident LIKE '%GAMBLING%' 
            OR type_of_incident LIKE '%HARASSMENT%' 
            OR type_of_incident LIKE '%URINATING%' 
        THEN 'Public Order Crimes'
        
        -- Weapon-Related Crimes
        WHEN type_of_incident LIKE '%UNLAWFUL CARRY%' 
            OR type_of_incident LIKE '%WEAPON%' 
            OR type_of_incident LIKE '%FIREARM%' 
            OR type_of_incident LIKE '%KNIFE%' 
        THEN 'Weapon-Related Crimes'
        
        -- Unknown Category
        ELSE 'Unknown Category'
    END AS incident_category
FROM dallas_only_nessasary_columns;

-- check if table exists
show tables;
select * from dallas_output LIMIT 10;








