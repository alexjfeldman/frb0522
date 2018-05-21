drop table Table_Schema_Check;
create external table Table_Schema_Check (Table_Name string,
Column_Name string,
Column_Number integer,
[200~drop table Table_Schema_Check;
create external table Table_Schema_Check (Table_Name string,
Column_Name string,
Column_Number integer,
Data_Type string,
Update_User_Name string,
Update_Time_Stamp bigint
)
stored as parquet
location 's3://metadatafrb/Table_Schema_Check/';

