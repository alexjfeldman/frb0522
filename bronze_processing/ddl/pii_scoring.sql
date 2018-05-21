drop table PII_Scoring;
create external table PII_Scoring (
Table_Name string,
Column_Name string,
IP_Percentage bigint,
Time_Percentage bigint,
Phone_Percentage bigint,
Link_Percentage bigint,
Date_Percentage bigint,
Email_Percentage bigint,
PII_Flag string,
Review_Flag string,
Encryption_Type string,
Update_User_Name string,
Update_Time_Stamp bigint
)
stored as parquet
location 's3://metadatafrb/PII_Scoring/';

