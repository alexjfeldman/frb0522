drop table metadata_generation;
create external table metadata_generation (
Table_Name string,
Column_Name string,
Data_Type string,
Number_Of_Rows bigint,
Number_Of_Unique_Values bigint,
Percent_Missing bigint,
Minimum_Value string,
Maximum_Value string,
Minimum_Length integer,
Average_Length double,
Maximum_Length integer,
Update_User_Name string,
Update_Time_Stamp bigint)
stored as parquet
location 's3://metadatafrb/metadata_generation/';

