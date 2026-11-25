# Databricks notebook source
import boto3
import os
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, concat_ws, substring, when, col, trim
from pyspark.sql import SparkSession 

import delta_table_creator, extract, transform, user_defined_functions
 
spark = SparkSession.builder.appName("example").getOrCreate()

# COMMAND ----------

# Takes the values d for dev, qa for test and p for prod depending in which workspace the init variables script is running
# env = os.environ["S3PREFIX"] 

# Import the module
import sys
sys.path.append("../aws_utils")
 
import aws_utils
# Get Environment Prefix
 
env = aws_utils.get_env_prefix()

# Takes the values of widgets
run_date = dbutils.widgets.get("run_date") 
unity = dbutils.widgets.get("unity") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Call the necessary functions

# COMMAND ----------

spark.udf.register("is_valid_tax_number_udf", user_defined_functions.is_valid_tax_number_udf)
spark.udf.register("string_cast_normalize_udf", user_defined_functions.string_cast_normalize_udf)
spark.udf.register("levenshtein_udf", user_defined_functions.levenshtein_udf)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create and Feed table DebtorUnifiedIdentical

# COMMAND ----------

table_name1 = 'DDebtorUnifiedIdentical'

# COMMAND ----------

schema1 = (
"ID string,"
"CDI string,"
"TaxRegistrationNumber string,"
"IsValidTaxFlag boolean,"
"InheritedTaxRegistrationNumber string,"
"InheritedCDI string,"
"PartyKey string,"
"SourceSystem string,"
"InheritedSourceSystem string,"
"PortfolioName string,"
"NationalNumber string,"
"InheritedNationalNumber string,"
"PassportNumber string,"
"InheritedPassportNumber string,"
"FirstNameENG string,"
"InheritedFirstNameENG string,"
"FIrstNameGR string,"
"InheritedFIrstNameGR string,"
"LastNameENG string,"
"InheritedLastNameENG string,"
"LastNameGR string,"
"InheritedLastNameGR string,"
"FirstFatherNameENG string,"
"InheritedFirstFatherNameENG string,"
"FirstFatherNameGR string,"
"InheritedFirstFatherNameGR string,"
"BirthDate string,"
"InheritedBirthDate string,"
"Gender string,"
"PostalCode string,"
"PhoneNumber1 string,"
"PhoneNumber2 string,"
"PhoneNumber3 string,"
"Rule string,"
"BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path1 = ""
 
# ================== Create Delta Table ==================
if unity == "True":
    derivative_table_path1 = f"`{env}-cdh`.derivativeactive.{table_name1}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path1):
        delta_table_creator.create_unity_delta_table(
            spark, derivative_table_path1, partition_columns, schema1
        )
else:
    derivative_table_path1 = f"s3://{env}-ec1-cepal-derivative-zone/{table_name1}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path1):
        delta_table_creator.create_delta_table(
            spark, derivative_table_path1, partition_columns, schema1
        )



# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path1,
    run_date,  # type: ignore
)

# COMMAND ----------

ValidDataset = spark.sql(f"""select * from (
    select  distinct p.TaxRegistrationNumber, p.CDI, p.SourceSystem
    , p.NationalNumber, string_cast_normalize_udf(p.NationalNumber) NormNationalNumber
    , p.PassportNumber, string_cast_normalize_udf(p.PassportNumber) NormPassportNumber
    , p.PartyKey, p.PortfolioName 
    , p.FirstNameENG, string_cast_normalize_udf(p.FirstNameENG) NormFirstNameENG
    , p.FirstNameGR, string_cast_normalize_udf(p.FirstNameGR) NormFirstNameGR
    , p.LastNameENG, string_cast_normalize_udf( p.LastNameENG) NormLastNameENG
    , p.LastNameGR, string_cast_normalize_udf(p.LastNameGR) NormLastNameGR
    , p.FirstFatherNameGR, string_cast_normalize_udf(p.FirstFatherNameGR) NormFirstFatherNameGR
    , p.FirstFatherNameENG, string_cast_normalize_udf(p.FirstFatherNameENG) NormFirstFatherNameENG
    , p.BirthDate, string_cast_normalize_udf(p.BirthDate) NormBirthDate
    , p.Gender, p.BusinessDate
    ,c.PostalCode, c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
    ,is_valid_tax_number_udf(p.TaxRegistrationNumber) as is_valid_tax_number
    ,row_number() OVER (PARTITION BY p.TaxRegistrationNumber ORDER BY 
            CASE    WHEN p.SourceSystem ='profits' THEN 1
                    WHEN p.SourceSystem ='qca' THEN 2
                    WHEN p.SourceSystem ='qc' THEN 3
                    WHEN p.SourceSystem ='arotron' THEN 4
                    ELSE 5 END
                    ) SystemRank

    from `{env}-cdh`.primary.party p
    left join `{env}-cdh`.primary.contactdetails c
    on p.BusinessDate=c.BusinessDate
    and p.CDI=c.CDI
    where   1=1
    and     p.BusinessDate={run_date!r}
    and     is_valid_tax_number_udf(p.TaxRegistrationNumber)=true
    )a
where SystemRank=1""")

ValidDataset.createOrReplaceTempView("ValidDatasetView")

# COMMAND ----------

ValidTax = spark.sql(f"""
    select ID,CDI,TaxRegistrationNumber,null as InheritedTaxRegistrationNumber,null as InheritedCDI,PartyKey,SourceSystem
    ,null as InheritedSourceSystem,PortfolioName,NationalNumber,null as InheritedNationalNumber,PassportNumber
    ,null as InheritedPassportNumber,FirstNameENG,null as InheritedFirstNameENG,FirstNameGR,null as InheritedFirstNameGR
    ,LastNameENG,null as InheritedLastNameENG,LastNameGR,null as InheritedLastNameGR,FirstFatherNameENG
    ,null as InheritedFirstFatherNameENG,FirstFatherNameGR,null as InheritedFirstFatherNameGR,BirthDate
    ,null as  InheritedBirthDate,Gender,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
    from (
    select distinct p.TaxRegistrationNumber AS ID,p.TaxRegistrationNumber,p.CDI,p.PartyKey,p.SourceSystem,p.PortfolioName,p.NationalNumber
    ,p.PassportNumber,p.FirstNameENG,p.FIrstNameGR,p.LastNameENG,p.LastNameGR,p.FirstFatherNameENG,p.FirstFatherNameGR,p.BirthDate,p.Gender,p.BusinessDate
    ,c.PostalCode, c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3, 'TaxRegistrationNumber' as Rule
    ,row_number() OVER (PARTITION BY p.TaxRegistrationNumber,p.CDI ORDER BY 
                CASE    WHEN p.SourceSystem ='profits' THEN 1
                        WHEN p.SourceSystem ='qca' THEN 2
                        WHEN p.SourceSystem ='qc' THEN 3
                        WHEN p.SourceSystem ='arotron' THEN 4
                        ELSE 5 END
                        ) SystemRank
    from `{env}-cdh`.primary.party p
    left join `{env}-cdh`.primary.contactdetails c
        on p.BusinessDate=c.BusinessDate
        and p.CDI=c.CDI
    where   1=1
    and     p.BusinessDate={run_date!r}
    and     is_valid_tax_number_udf(p.TaxRegistrationNumber)=true
    )a
    where SystemRank=1
    """)

ValidTax.createOrReplaceTempView("ValidTaxView")

# COMMAND ----------

# MAGIC %md
# MAGIC #####National Number

# COMMAND ----------

NationalNumber = spark.sql(f"""
select ID,CDI,TaxRegistrationNumber
,InheritedTaxRegistrationNumber,InheritedCDI
,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber
,FirstNameENG,InheritedFirstNameENG,FirstNameGR,InheritedFirstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (
select distinct coalesce(tax.TaxRegistrationNumber,cdi.NationalNumber) as ID,cdi.TaxRegistrationNumber,cdi.CDI
,tax.TaxRegistrationNumber as InheritedTaxRegistrationNumber,tax.CDI as InheritedCDI
,cdi.PartyKey,cdi.SourceSystem,tax.SourceSystem as InheritedSourceSystem,cdi.PortfolioName
,cdi.NationalNumber,tax.NationalNumber as InheritedNationalNumber,cdi.PassportNumber,tax.PassportNumber as InheritedPassportNumber
,cdi.FirstNameENG,tax.FirstNameENG as InheritedFirstNameENG,cdi.FirstNameGR,tax.FirstNameGR as InheritedFirstNameGR,cdi.LastNameENG,tax.LastNameENG as InheritedLastNameENG
,cdi.LastNameGR,tax.LastNameGR as InheritedLastNameGR,cdi.FirstFatherNameENG,tax.FirstFatherNameENG as InheritedFirstFatherNameENG,cdi.FirstFatherNameGR,tax.FirstFatherNameGR as InheritedFirstFatherNameGR
,cdi.BirthDate,tax.BirthDate as InheritedBirthDate,cdi.Gender,cdi.BusinessDate
,c.PostalCode,c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
, 'NationalNumber' as Rule
,row_number() OVER (PARTITION BY tax.TaxRegistrationNumber,cdi.CDI,cdi.NationalNumber ORDER BY 
            CASE    WHEN cdi.SourceSystem ='profits' THEN 1
                    WHEN cdi.SourceSystem ='qca' THEN 2
                    WHEN cdi.SourceSystem ='qc' THEN 3
                    WHEN cdi.SourceSystem ='arotron' THEN 4
                    ELSE 5 END
                    ) SystemRank
from `{env}-cdh`.primary.party cdi
left join `{env}-cdh`.primary.contactdetails c
    on cdi.BusinessDate=c.BusinessDate
    and cdi.CDI=c.CDI
inner join ValidDatasetView tax
on COALESCE(NULLIF(string_cast_normalize_udf(cdi.NationalNumber), ''),'-1')=COALESCE(NULLIF((tax.NormNationalNumber), ''),'-2')
and coalesce(cdi.BirthDate,'0')=coalesce(tax.BirthDate,'0') 
and tax.BusinessDate =cdi.BusinessDate

left join ValidTaxView ValidTaxView
on cdi.BusinessDate=ValidTaxView.BusinessDate
and cdi.CDI=ValidTaxView.CDI

where   1=1
and     cdi.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(cdi.TaxRegistrationNumber)=false or is_valid_tax_number_udf(cdi.TaxRegistrationNumber) is null)
and     cdi.NationalNumber is not null and cdi.NationalNumber<>'000000000'
and     cdi.FirstNameGR not like '%XXX%'
and     ValidTaxView.CDI is null -- if found already in other scenarios, ignore
)a
where SystemRank=1
""")

NationalNumber.createOrReplaceTempView("NationalNumberView")

# COMMAND ----------

# MAGIC %md
# MAGIC #####PassportNumber

# COMMAND ----------

PassportNumber = spark.sql(f"""
select ID,CDI,TaxRegistrationNumber
,InheritedTaxRegistrationNumber,InheritedCDI
,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber
,FirstNameENG,InheritedFirstNameENG,FirstNameGR,InheritedFirstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (
select distinct coalesce(tax.TaxRegistrationNumber,cdi.PassportNumber) as ID,cdi.TaxRegistrationNumber,cdi.CDI
,tax.TaxRegistrationNumber as InheritedTaxRegistrationNumber,tax.CDI as InheritedCDI
,cdi.PartyKey,cdi.SourceSystem,tax.SourceSystem as InheritedSourceSystem,cdi.PortfolioName
,cdi.NationalNumber,tax.NationalNumber as InheritedNationalNumber,cdi.PassportNumber,tax.PassportNumber as InheritedPassportNumber
,cdi.FirstNameENG,tax.FirstNameENG as InheritedFirstNameENG,cdi.FirstNameGR,tax.FirstNameGR as InheritedFirstNameGR,cdi.LastNameENG,tax.LastNameENG as InheritedLastNameENG
,cdi.LastNameGR,tax.LastNameGR as InheritedLastNameGR,cdi.FirstFatherNameENG,tax.FirstFatherNameENG as InheritedFirstFatherNameENG,cdi.FirstFatherNameGR,tax.FirstFatherNameGR as InheritedFirstFatherNameGR
,cdi.BirthDate,tax.BirthDate as InheritedBirthDate,cdi.Gender,cdi.BusinessDate
,c.PostalCode,c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
, 'PassportNumber' as Rule
,row_number() OVER (PARTITION BY tax.TaxRegistrationNumber,cdi.CDI,cdi.PassportNumber ORDER BY 
            CASE    WHEN cdi.SourceSystem ='profits' THEN 1
                    WHEN cdi.SourceSystem ='qc' THEN 2
                    WHEN cdi.SourceSystem ='qca' THEN 3
                    WHEN cdi.SourceSystem ='arotron' THEN 4
                    ELSE 5 END) SystemRank
from `{env}-cdh`.primary.party cdi
left join `{env}-cdh`.primary.contactdetails c
    on cdi.BusinessDate=c.BusinessDate
    and cdi.CDI=c.CDI
inner join ValidDatasetView tax
on COALESCE(NULLIF(string_cast_normalize_udf(cdi.PassportNumber), ''),'-1')=COALESCE(NULLIF((tax.NormPassportNumber), ''),'-2')
and tax.BusinessDate =cdi.BusinessDate

left join ValidTaxView ValidTaxView
on cdi.BusinessDate=ValidTaxView.BusinessDate
and cdi.CDI=ValidTaxView.CDI

left join NationalNumberView NationalNumberView
on  NationalNumberView.CDI = cdi.CDI
and NationalNumberView.BusinessDate=cdi.BusinessDate

where   1=1
and     cdi.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(cdi.TaxRegistrationNumber)=false or is_valid_tax_number_udf(cdi.TaxRegistrationNumber) is null)
and     cdi.PassportNumber is not null
and     cdi.FirstNameGR not like '%XXX%'
and     ValidTaxView.CDI is null -- if found already in other scenarios, ignore
and     NationalNumberView.CDI is null -- if found already in other scenarios, ignore
)a
where SystemRank=1
""")

PassportNumber.createOrReplaceTempView("PassportNumberView")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FirstNameGR, LastNameGR, FirstFatherNameGR, BirthDate

# COMMAND ----------

FullNameGR = spark.sql(f"""
select ID,CDI,TaxRegistrationNumber
,InheritedTaxRegistrationNumber,InheritedCDI
,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber
,FirstNameENG,InheritedFirstNameENG,FirstNameGR,InheritedFirstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (
select distinct coalesce(tax.TaxRegistrationNumber,p.LastNameGR) as ID, p.TaxRegistrationNumber,p.CDI
,tax.TaxRegistrationNumber as InheritedTaxRegistrationNumber,tax.CDI as InheritedCDI
,p.PartyKey,p.SourceSystem,tax.SourceSystem as InheritedSourceSystem,p.PortfolioName
,p.NationalNumber,tax.NationalNumber as InheritedNationalNumber,p.PassportNumber,tax.PassportNumber as InheritedPassportNumber
,p.FirstNameENG,tax.FirstNameENG as InheritedFirstNameENG,p.FirstNameGR,tax.FirstNameGR as InheritedFirstNameGR,p.LastNameENG,tax.LastNameENG as InheritedLastNameENG
,p.LastNameGR,tax.LastNameGR as InheritedLastNameGR,p.FirstFatherNameENG,tax.FirstFatherNameENG as InheritedFirstFatherNameENG,p.FirstFatherNameGR,tax.FirstFatherNameGR as InheritedFirstFatherNameGR
,p.BirthDate,tax.BirthDate as InheritedBirthDate,p.Gender,p.BusinessDate
,c.PostalCode,c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
, 'FirstNameGR,LastNameGR,FirstFatherNameGR,BirthDate' as Rule
,row_number() OVER (PARTITION BY tax.TaxRegistrationNumber,p.CDI,p.FirstNameGR,p.LastNameGR ORDER BY 
            coalesce(p.FirstFatherNameGR, 'ΩΩΩ') ASC
            ,CASE    WHEN p.SourceSystem ='profits' THEN 1
                    WHEN p.SourceSystem ='qc' THEN 2
                    WHEN p.SourceSystem ='qca' THEN 3
                    WHEN p.SourceSystem ='arotron' THEN 4
                    ELSE 5 END) SystemRank
from `{env}-cdh`.primary.party p
left join `{env}-cdh`.primary.contactdetails c
    on p.BusinessDate=c.BusinessDate
    and p.CDI=c.CDI
inner join ValidDatasetView tax
on tax.BusinessDate =p.BusinessDate

and COALESCE(NULLIF(string_cast_normalize_udf(p.FirstNameGR), ''),'-1')=COALESCE(NULLIF((tax.NormFirstNameGR), ''),'-2')
and COALESCE(NULLIF(string_cast_normalize_udf(p.LastNameGR), ''),'-1')=COALESCE(NULLIF((tax.NormLastNameGR), ''),'-2')
and COALESCE(NULLIF(string_cast_normalize_udf(p.FirstFatherNameGR), ''),'-1')=COALESCE(NULLIF((tax.NormFirstFatherNameGR), ''),'-2')
and coalesce(p.BirthDate,'0')=coalesce(tax.BirthDate,'0')

left join ValidTaxView ValidTaxView
on ValidTaxView.BusinessDate=p.BusinessDate
and ValidTaxView.CDI=p.CDI

left join NationalNumberView NationalNumberView
on  NationalNumberView.CDI = p.CDI
and NationalNumberView.BusinessDate=p.BusinessDate

left join PassportNumberView PassportNumberView
on PassportNumberView.CDI=p.CDI
and PassportNumberView.BusinessDate=p.BusinessDate

where   1=1
and     p.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(p.TaxRegistrationNumber)=false or is_valid_tax_number_udf(p.TaxRegistrationNumber) is null)
and     p.FirstNameGR is not null and p.FirstNameGR<>''
and     p.LastNameGR is not null  and p.LastNameGR<>''
and     p.FirstFatherNameGR is not null and p.FirstFatherNameGR<>''
and     p.FirstNameGR not like '%XXX%'
and     ValidTaxView.CDI is null -- if found already in other scenarios, ignore
and     NationalNumberView.CDI is null -- if found already in other scenarios, ignore
and     PassportNumberView.CDI is null -- if found already in other scenarios, ignore
)a
where SystemRank=1

""")

FullNameGR.createOrReplaceTempView("FullNameGRView")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FirstNameENG, LastNameENG, FirstFatherNameENG, BirthDate

# COMMAND ----------

FullNameENG = spark.sql(f"""
select ID,CDI,TaxRegistrationNumber
,InheritedTaxRegistrationNumber,InheritedCDI
,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber
,FirstNameENG,InheritedFirstNameENG,FirstNameGR,InheritedFirstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (
select distinct coalesce(tax.TaxRegistrationNumber,p.LastNameENG) as ID, p.TaxRegistrationNumber,p.CDI
,tax.TaxRegistrationNumber as InheritedTaxRegistrationNumber,tax.CDI as InheritedCDI
,p.PartyKey,p.SourceSystem,tax.SourceSystem as InheritedSourceSystem,p.PortfolioName
,p.NationalNumber,tax.NationalNumber as InheritedNationalNumber,p.PassportNumber,tax.PassportNumber as InheritedPassportNumber
,p.FirstNameENG,tax.FirstNameENG as InheritedFirstNameENG,p.FirstNameGR,tax.FirstNameGR as InheritedFirstNameGR,p.LastNameENG,tax.LastNameENG as InheritedLastNameENG
,p.LastNameGR,tax.LastNameGR as InheritedLastNameGR,p.FirstFatherNameENG,tax.FirstFatherNameENG as InheritedFirstFatherNameENG,p.FirstFatherNameGR,tax.FirstFatherNameGR as InheritedFirstFatherNameGR
,p.BirthDate,tax.BirthDate as InheritedBirthDate,p.Gender,p.BusinessDate
,c.PostalCode,c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
,'FirstNameENG,LastNameENG,FirstFatherNameENG,BirthDate' as Rule
,row_number() OVER (PARTITION BY tax.TaxRegistrationNumber,p.CDI,p.FirstNameENG,p.LastNameENG ORDER BY 
            coalesce(p.FirstFatherNameENG, 'ZZZ') ASC,
            CASE    WHEN p.SourceSystem ='profits' THEN 1
                    WHEN p.SourceSystem ='qc' THEN 2
                    WHEN p.SourceSystem ='qca' THEN 3
                    WHEN p.SourceSystem ='arotron' THEN 4
                    ELSE 5 END) SystemRank
from `{env}-cdh`.primary.party p
left join `{env}-cdh`.primary.contactdetails c
    on p.BusinessDate=c.BusinessDate
    and p.CDI=c.CDI
inner join ValidDatasetView tax
on  tax.BusinessDate =p.BusinessDate
and COALESCE(NULLIF(string_cast_normalize_udf(p.FirstNameENG), ''),'-1')=COALESCE(NULLIF((tax.NormFirstNameENG), ''),'-2')
and COALESCE(NULLIF(string_cast_normalize_udf(p.LastNameENG), ''),'-1')=COALESCE(NULLIF((tax.NormLastNameENG), ''),'-2')
and COALESCE(NULLIF(string_cast_normalize_udf(p.FirstFatherNameENG), ''),'-1')=COALESCE(NULLIF((tax.NormFirstFatherNameENG), ''),'-2')
and coalesce(p.BirthDate,'0')=coalesce(tax.BirthDate,'0')

left join ValidTaxView ValidTaxView
on ValidTaxView.BusinessDate=p.BusinessDate
and ValidTaxView.CDI=p.CDI

left join NationalNumberView NationalNumberView
on  NationalNumberView.CDI = p.CDI
and NationalNumberView.BusinessDate=p.BusinessDate

left join PassportNumberView PassportNumberView
on PassportNumberView.CDI=p.CDI
and PassportNumberView.BusinessDate=p.BusinessDate

left join FullNameGRView FullNameGRView
on FullNameGRView.CDI=p.CDI
and FullNameGRView.BusinessDate=p.BusinessDate

where   1=1
and     p.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(p.TaxRegistrationNumber)=false or is_valid_tax_number_udf(p.TaxRegistrationNumber) is null)
and     p.LastNameENG<>'DEFAULT'
and     p.FirstNameENG<>'DEFAULT'
and     p.LastNameENG is not null and p.LastNameENG<>''
and     p.FirstNameENG is not null and p.FirstNameENG<>''
and     p.FirstFatherNameENG is not null and p.FirstFatherNameENG<>''
and     p.FirstNameENG not like '%XXX%'
and     ValidTaxView.CDI is null -- if found already in other scenarios, ignore
and     NationalNumberView.CDI is null -- if found already in other scenarios, ignore
and     PassportNumberView.CDI is null -- if found already in other scenarios, ignore
and     FullNameGRView.CDI is null -- if found already in other scenarios, ignore
)a
where SystemRank=1
""")

FullNameENG.createOrReplaceTempView("FullNameENGView")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FirstNameGR, LastNameGR, PhoneNumber, BirthDate

# COMMAND ----------

PhoneNumberGR = spark.sql(f"""
select ID,CDI,TaxRegistrationNumber
,InheritedTaxRegistrationNumber,InheritedCDI
,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber
,FirstNameENG,InheritedFirstNameENG,FIrstNameGR,InheritedFIrstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (

select distinct coalesce(tax.TaxRegistrationNumber,p.LastNameGR) as ID, p.TaxRegistrationNumber,p.CDI
,tax.TaxRegistrationNumber as InheritedTaxRegistrationNumber,tax.CDI as InheritedCDI
,p.PartyKey,p.SourceSystem,tax.SourceSystem as InheritedSourceSystem,p.PortfolioName
,p.NationalNumber,tax.NationalNumber as InheritedNationalNumber,p.PassportNumber,tax.PassportNumber as InheritedPassportNumber
,p.FirstNameENG,tax.FirstNameENG as InheritedFirstNameENG,p.FirstNameGR,tax.FirstNameGR as InheritedFirstNameGR,p.LastNameENG,tax.LastNameENG as InheritedLastNameENG
,p.LastNameGR,tax.LastNameGR as InheritedLastNameGR,p.FirstFatherNameENG,tax.FirstFatherNameENG as InheritedFirstFatherNameENG,p.FirstFatherNameGR,tax.FirstFatherNameGR as InheritedFirstFatherNameGR
,p.BirthDate,tax.BirthDate as InheritedBirthDate,p.Gender,p.BusinessDate
,c.PostalCode,c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
,'FirstNameGR, LastNameGR, PhoneNumber, BirthDate' as Rule
,row_number() OVER (PARTITION BY tax.TaxRegistrationNumber,p.CDI,p.LastNameGR,p.FirstNameGR ORDER BY 
            coalesce(p.FirstFatherNameGR, 'ΩΩΩ') ASC,
            CASE    WHEN p.SourceSystem ='profits' THEN 1
                    WHEN p.SourceSystem ='qc' THEN 2
                    WHEN p.SourceSystem ='qca' THEN 3
                    WHEN p.SourceSystem ='arotron' THEN 4
                    ELSE 5 END) SystemRank
from `{env}-cdh`.primary.party p

inner join `{env}-cdh`.primary.contactdetails c
on p.BusinessDate=c.BusinessDate
and p.CDI=c.CDI

inner join ValidDatasetView tax
on tax.BusinessDate = p.BusinessDate
and COALESCE(NULLIF(string_cast_normalize_udf(p.FirstNameGR), ''),'-1')=COALESCE(NULLIF((tax.NormFirstNameGR), ''),'-2')
and COALESCE(NULLIF(string_cast_normalize_udf(p.LastNameGR), ''),'-1')=COALESCE(NULLIF((tax.NormLastNameGR), ''),'-2')
and COALESCE(NULLIF(c.PhoneNumber1, ''), NULLIF(c.PhoneNumber2, ''), NULLIF(c.PhoneNumber3, ''),-1) = 
COALESCE(NULLIF(tax.PhoneNumber1, ''), NULLIF(tax.PhoneNumber2, ''), NULLIF(tax.PhoneNumber3, ''),-2)
and coalesce(p.BirthDate,'0')=coalesce(tax.BirthDate,'0')

left join ValidTaxView ValidTaxView
on ValidTaxView.BusinessDate=p.BusinessDate
and ValidTaxView.CDI=p.CDI

left join NationalNumberView NationalNumberView
on  NationalNumberView.CDI = p.CDI
and NationalNumberView.BusinessDate=p.BusinessDate

left join PassportNumberView PassportNumberView
on PassportNumberView.CDI=p.CDI
and PassportNumberView.BusinessDate=p.BusinessDate

left join FullNameGRView FullNameGRView
on FullNameGRView.CDI=p.CDI
and FullNameGRView.BusinessDate=p.BusinessDate

left join FullNameENGView FullNameENGView
on FullNameENGView.CDI=p.CDI
and FullNameENGView.BusinessDate=p.BusinessDate

where   1=1
and     p.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(p.TaxRegistrationNumber)=false or is_valid_tax_number_udf(p.TaxRegistrationNumber) is null)
and     p.LastNameGR is not null and p.LastNameGR<>''
and     p.FirstNameGR not like '%XXX%'
and     p.FirstNameGR is not null and p.FirstNameGR<>''
and     COALESCE(NULLIF(c.PhoneNumber1, ''), NULLIF(c.PhoneNumber2, ''), NULLIF(c.PhoneNumber3, '')) is not null
and     COALESCE(NULLIF(tax.PhoneNumber1, ''), NULLIF(tax.PhoneNumber2, ''), NULLIF(tax.PhoneNumber3, '')) is not null
AND     (CHAR_LENGTH(c.PhoneNumber1) >= 10 OR CHAR_LENGTH(c.PhoneNumber2) >= 10 OR CHAR_LENGTH(c.PhoneNumber3) >= 10)

and     ValidTaxView.CDI is null -- if found already in other scenarios, ignore
and     NationalNumberView.CDI is null -- if found already in other scenarios, ignore
and     PassportNumberView.CDI is null -- if found already in other scenarios, ignore
and     FullNameGRView.CDI is null -- if found already in other scenarios, ignore
and     FullNameENGView.CDI is null -- if found already in other scenarios, ignore
)a
where SystemRank=1
""")

PhoneNumberGR.createOrReplaceTempView("PhoneNumberGRView")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FirstNameENG, LastNameENG, PhoneNumber1, BirthDate

# COMMAND ----------

PhoneNumberENG = spark.sql(f"""
 -- English Name,PhoneNumber
select ID,CDI,TaxRegistrationNumber
,InheritedTaxRegistrationNumber,InheritedCDI
,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber
,FirstNameENG,InheritedFirstNameENG,FirstNameGR,InheritedFirstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (

select distinct coalesce(tax.TaxRegistrationNumber,p.LastNameENG) as ID, p.TaxRegistrationNumber,p.CDI
,tax.TaxRegistrationNumber as InheritedTaxRegistrationNumber,tax.CDI as InheritedCDI
,p.PartyKey,p.SourceSystem,tax.SourceSystem as InheritedSourceSystem,p.PortfolioName
,p.NationalNumber,tax.NationalNumber as InheritedNationalNumber,p.PassportNumber,tax.PassportNumber as InheritedPassportNumber
,p.FirstNameENG,tax.FirstNameENG as InheritedFirstNameENG,p.FirstNameGR,tax.FirstNameGR as InheritedFirstNameGR,p.LastNameENG,tax.LastNameENG as InheritedLastNameENG
,p.LastNameGR,tax.LastNameGR as InheritedLastNameGR,p.FirstFatherNameENG,tax.FirstFatherNameENG as InheritedFirstFatherNameENG,p.FirstFatherNameGR,tax.FirstFatherNameGR as InheritedFirstFatherNameGR
,p.BirthDate,tax.BirthDate as InheritedBirthDate,p.Gender,p.BusinessDate
,c.PostalCode,c.PhoneNumber1,c.PhoneNumber2,c.PhoneNumber3
,'FirstNameENG, LastNameENG, PhoneNumber, BirthDate' as Rule
,row_number() OVER (PARTITION BY tax.TaxRegistrationNumber,p.CDI,p.LastNameENG,p.FirstNameENG ORDER BY
            coalesce(p.FirstFatherNameENG, 'ZZZ') ASC,    
            CASE    WHEN p.SourceSystem ='profits' THEN 1
                    WHEN p.SourceSystem ='qc' THEN 2
                    WHEN p.SourceSystem ='qca' THEN 3
                    WHEN p.SourceSystem ='arotron' THEN 4
                    ELSE 5 END) SystemRank
from `{env}-cdh`.primary.party p

inner join `{env}-cdh`.primary.contactdetails c
on p.BusinessDate=c.BusinessDate
and p.CDI=c.CDI

inner join ValidDatasetView tax
on tax.BusinessDate = p.BusinessDate
and COALESCE(NULLIF(string_cast_normalize_udf(p.FirstNameENG), ''),'-1')=COALESCE(NULLIF((tax.NormFirstNameENG), ''),'-2')
and COALESCE(NULLIF(string_cast_normalize_udf(p.LastNameENG), ''),'-1')=COALESCE(NULLIF((tax.NormLastNameENG), ''),'-2')

and COALESCE(NULLIF(c.PhoneNumber1, ''), NULLIF(c.PhoneNumber2, ''), NULLIF(c.PhoneNumber3, ''),-1) = 
COALESCE(NULLIF(tax.PhoneNumber1, ''), NULLIF(tax.PhoneNumber2, ''), NULLIF(tax.PhoneNumber3, ''),-2)
and coalesce(p.BirthDate,'0')=coalesce(tax.BirthDate,'0')

left join ValidTaxView ValidTaxView
on ValidTaxView.BusinessDate=p.BusinessDate
and ValidTaxView.CDI=p.CDI

left join NationalNumberView NationalNumberView
on  NationalNumberView.CDI = p.CDI
and NationalNumberView.BusinessDate=p.BusinessDate

left join PassportNumberView PassportNumberView
on PassportNumberView.CDI=p.CDI
and PassportNumberView.BusinessDate=p.BusinessDate

left join FullNameGRView FullNameGRView
on FullNameGRView.CDI=p.CDI
and FullNameGRView.BusinessDate=p.BusinessDate

left join FullNameENGView FullNameENGView
on FullNameENGView.CDI=p.CDI
and FullNameENGView.BusinessDate=p.BusinessDate

left join PhoneNumberGRView PhoneNumberGRView
on PhoneNumberGRView.CDI=p.CDI
and PhoneNumberGRView.BusinessDate=p.BusinessDate

where   1=1
and     p.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(p.TaxRegistrationNumber)=false or is_valid_tax_number_udf(p.TaxRegistrationNumber) is null)
and     COALESCE(NULLIF(c.PhoneNumber1, ''), NULLIF(c.PhoneNumber2, ''), NULLIF(c.PhoneNumber3, '')) is not null
and     COALESCE(NULLIF(tax.PhoneNumber1, ''), NULLIF(tax.PhoneNumber2, ''), NULLIF(tax.PhoneNumber3, '')) is not null
and     p.LastNameENG is not null and p.LastNameENG<>''
and     p.FirstNameENG is not null and p.FirstNameENG<>''
and     p.FirstNameENG not like '%XXX%'
and     p.LastNameENG<>'DEFAULT'
AND (CHAR_LENGTH(c.PhoneNumber1) >= 10 OR CHAR_LENGTH(c.PhoneNumber2) >= 10 OR CHAR_LENGTH(c.PhoneNumber3) >= 10)
and     ValidTaxView.CDI is null -- if found already in other scenarios, ignore
and     NationalNumberView.CDI is null -- if found already in other scenarios,ignore
and     PassportNumberView.CDI is null -- if found already in other scenarios, ignore
and     FullNameGRView.CDI is null -- if found already in other scenarios, ignore 
and     FullNameENGView.CDI is null -- if found already in other scenarios, ignore
and     PhoneNumberGRView.CDI is null -- if found already in other scenarios, ignore
)a
where SystemRank=1                         
""")

PhoneNumberENG.createOrReplaceTempView("PhoneNumberENGView")

# COMMAND ----------

final_df1 = spark.sql("""
select ID,CDI,TaxRegistrationNumber,is_valid_tax_number_udf(TaxRegistrationNumber) as IsValidTaxFlag
,InheritedTaxRegistrationNumber,InheritedCDI,PartyKey,SourceSystem,InheritedSourceSystem,PortfolioName
,NationalNumber,InheritedNationalNumber,PassportNumber,InheritedPassportNumber,FirstNameENG
,InheritedFirstNameENG,FirstNameGR,InheritedFIrstNameGR,LastNameENG,InheritedLastNameENG,LastNameGR,InheritedLastNameGR
,FirstFatherNameENG,InheritedFirstFatherNameENG,FirstFatherNameGR,InheritedFirstFatherNameGR,BirthDate,InheritedBirthDate,Gender
,PostalCode, PhoneNumber1,PhoneNumber2,PhoneNumber3,Rule,BusinessDate
from (
select * from ValidTaxView
union 
select * from NationalNumberView
union 
select * from PassportNumberView
union 
select * from FullNameGRView
union 
select * from FullNameENGView
union 
select * from PhoneNumberGRView
union 
select * from PhoneNumberENGView
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write DataFrame to Delta Lake based on 'unity' value
# MAGIC

# COMMAND ----------

if unity == "True":
    final_df1.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path1) # in order to write in delta_lake
else:
    final_df1.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path1) # in order to write in delta_lake

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating the Matching Dataset for the Similarity Algorithm

# COMMAND ----------

table_name2 = 'DDebtorUnifiedMatched'

# COMMAND ----------

schema2= (
"SourceTaxCDI string,"
"InheritedTaxRegistrationNumber string,"
"MatchedCDI string,"
"SelfMatchFlag int,"
"TaxRegistrationNumber string,"
"PartyCDI string,"
"CDI string,"
"PartyPortfolioName string,"
"PortfolioName string,"
"PartySourceSystem string,"
"SourceSystem string,"
"PartyPartyKey string,"
"PartyKey string,"
"PartyPartyRepresentKey string,"
"PartyRepresentKey string,"
"NormalizedPartyFirstLastNameGR string,"
"NormalizedFirstLastNameGR string,"
"NormalizedPartyFirstFatherNameGR string,"
"NormalizedFirstFatherNameGR string,"
"NormalizedPartyBirthDate string,"
"NormalizedBirthDate string,"
"NormalizedPartyNationalNumber string,"
"NormalizedNationalNumber string,"
"NormalizedPartyPassportNumber string,"
"NormalizedPassportNumber string, "
"NormalizedPartyPhoneNumberGR string,"
"NormalizedPhoneNumberGR string,"
"PhoneNumbers string,"
"LevenshteinNationalNumber string,"
"LevenshteinPassportNumber string,"
"LevenshteinNameGR string,"
"LevenshteinFirstFatherNameGR string,"
"LevenshteinBirthDateGR string,"
"LevenshteinPhoneNumberGR string,"
"WeightedAverage double,"
"BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path2 = ""
 
# ================== Create Delta Table ==================
if unity == "True":
    derivative_table_path2 = f"`{env}-cdh`.derivativeactive.{table_name2}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path2):
        delta_table_creator.create_unity_delta_table(
            spark, derivative_table_path2, partition_columns, schema2
        )
else:
    derivative_table_path2 = f"s3://{env}-ec1-cepal-derivative-zone/{table_name2}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path2):
        delta_table_creator.create_delta_table(
            spark, derivative_table_path2, partition_columns, schema2
        )
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path2,
    run_date,  # type: ignore
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Superset
# MAGIC Based on the Substring of the first two letters of the FirstNameGR and LastNameGR

# COMMAND ----------

start_df = spark.sql(f"""
with test_table as (
select distinct p.CDI, p.PartyKey,p.PortfolioName,p.SourceSystem,p.PartyRepresentKey,p.BusinessDate
,string_cast_normalize_udf(p.FirstNameGR) as NormalizedFirstNameGR
,string_cast_normalize_udf(p.LastNameGR) as NormalizedLastNameGR
,string_cast_normalize_udf(concat(coalesce(p.FirstNameGR,''),coalesce(p.LastNameGR,''))) as NormalizedFirstLastNameGR
,string_cast_normalize_udf(p.FirstFatherNameGR) as NormalizedFirstFatherNameGR
,string_cast_normalize_udf(cast(p.BirthDate as date))  as NormalizedBirthDate
,string_cast_normalize_udf(p.NationalNumber) as NormalizedNationalNumber
,string_cast_normalize_udf(p.PassportNumber) as NormalizedPassportNumber
,string_cast_normalize_udf(concat(coalesce(p.FirstNameGR,''),coalesce(p.LastNameGR,''),coalesce(p.FirstFatherNameGR,''),coalesce(cast(p.BirthDate as date),''))) as NormalizedBirthDateGR
,string_cast_normalize_udf(concat(coalesce(p.LastNameGR,''),coalesce(c.PhoneNumber1,''),coalesce(c.PhoneNumber2,''),coalesce(c.PhoneNumber3,''))) as NormalizedPhoneNumberGR
,string_cast_normalize_udf(concat(coalesce(c.PhoneNumber1,''),coalesce(c.PhoneNumber2,''),coalesce(c.PhoneNumber3,''))) as NormalizedPhoneNumber


from `{env}-cdh`.primary.party p
left join `{env}-cdh`.primary.contactdetails c
    on p.BusinessDate=c.BusinessDate
    and p.CDI=c.CDI
left join `{env}-cdh`.derivative.ddebtorunifiedidentical d
on d.BusinessDate=p.BusinessDate
and d.CDI=p.CDI
where   p.BusinessDate={run_date!r}
and     (is_valid_tax_number_udf(p.TaxRegistrationNumber)=false or is_valid_tax_number_udf(p.TaxRegistrationNumber) is null)
and     (p.FirstNameGR not like '%XXX%' OR p.FirstNameGR IS NULL)
and d.CDI is null --  exclude identical matched cdis
and p.CDI is not null and p.CDI <>''
) 

select *

from 
( select distinct d.TaxRegistrationNumber,d.CDI as PartyCDI,d.PartyKey as PartyPartyKey,d.PortfolioName as PartyPortfolioName,d.SourceSystem as PartySourceSystem,d.PartyRepresentKey as PartyPartyRepresentKey,d.BusinessDate as PartyBusinessDate
,string_cast_normalize_udf(FirstNameGR) as NormalizedPartyFirstNameGR
,string_cast_normalize_udf(LastNameGR) as NormalizedPartyLastNameGR
,string_cast_normalize_udf(concat(coalesce(d.FirstNameGR,''),coalesce(d.LastNameGR,''))) as NormalizedPartyFirstLastNameGR
,string_cast_normalize_udf(d.FirstFatherNameGR) as NormalizedPartyFirstFatherNameGR
,string_cast_normalize_udf(cast(d.BirthDate as date)) as NormalizedPartyBirthDate
,string_cast_normalize_udf(d.NationalNumber) as NormalizedPartyNationalNumber
,string_cast_normalize_udf(d.PassportNumber) as NormalizedPartyPassportNumber
,string_cast_normalize_udf(concat(coalesce(d.FIrstNameGR,''),coalesce(d.LastNameGR,''),coalesce(d.FirstFatherNameGR,''))) as NormalizedPartyNameGR
,string_cast_normalize_udf(concat(coalesce(d.FIrstNameGR,''),coalesce(d.LastNameGR,''),coalesce(d.FirstFatherNameGR,''),coalesce(cast(d.BirthDate as date),''))) as NormalizedPartyBirthDateGR
,string_cast_normalize_udf(concat(coalesce(d.LastNameGR,''),coalesce(c.PhoneNumber1,''),coalesce(c.PhoneNumber2,''),coalesce(c.PhoneNumber3,''))) as NormalizedPartyPhoneNumberGR
,string_cast_normalize_udf(concat(coalesce(c.PhoneNumber1,''),coalesce(c.PhoneNumber2,''),coalesce(c.PhoneNumber3,''))) as NormalizedPartyPhoneNumber
 
 from 
`{env}-cdh`.primary.party d

left join `{env}-cdh`.primary.contactdetails c 
    on d.BusinessDate=c.BusinessDate
    and d.CDI=c.CDI
and     d.BusinessDate={run_date!r}
and     (d.FirstNameGR not like '%XXX%' OR d.FirstNameGR IS NULL)
) p

inner join test_table t
on t.BusinessDate=p.PartyBusinessDate 
and 
    ( COALESCE(NULLIF(SUBSTRING(t.NormalizedFirstNameGR, 1, 2), ''),1)= COALESCE(NULLIF(SUBSTRING(p.NormalizedPartyFirstNameGR, 1, 2), ''),1) 
    and COALESCE(NULLIF(SUBSTRING(t.NormalizedLastNameGR, 1, 2), ''),1)= COALESCE(NULLIF(SUBSTRING(p.NormalizedPartyLastNameGR, 1, 2), ''),1) )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Apply similarity algorithm levenshtein distance
# MAGIC When having null values or empty strings we reset the distance

# COMMAND ----------

from pyspark.sql.functions import levenshtein, length, greatest, when, col


df = start_df

dummy_values_date = ["17530101", "18990101","19000101", "19010101", "19001231"]

dummy_values_phonenumber = ["000000","0000000000","0","2100000000","00","000000000000000000000000000000","1111111111","00000","0000"] 


df = df.withColumn("LevenshteinNationalNumber",         
    when(
        ((df["NormalizedPartyNationalNumber"].isNull()) | (df["NormalizedNationalNumber"].isNull())) |
        ((df["NormalizedPartyNationalNumber"] == "") | (df["NormalizedNationalNumber"] == "")),0
    ).otherwise(
        1- levenshtein(df["NormalizedPartyNationalNumber"], df["NormalizedNationalNumber"]) 
        / greatest(length(df["NormalizedPartyNationalNumber"]), length(df["NormalizedNationalNumber"]))
    )
)\
.withColumn("LevenshteinPassportNumber",         
    when(
        ((df["NormalizedPartyPassportNumber"].isNull()) | (df["NormalizedPassportNumber"].isNull())) |
        ((df["NormalizedPartyPassportNumber"] == "") | (df["NormalizedPassportNumber"] == "")),0
    ).otherwise(
        1- levenshtein(df["NormalizedPartyPassportNumber"], df["NormalizedPassportNumber"]) 
        / greatest(length(df["NormalizedPartyPassportNumber"]), length(df["NormalizedPassportNumber"]))
    )
)\
.withColumn("LevenshteinNameGR",         
    when(
        ((df["NormalizedPartyFirstLastNameGR"].isNull()) | (df["NormalizedFirstLastNameGR"].isNull())) |
        ((df["NormalizedPartyFirstLastNameGR"] == "") | (df["NormalizedFirstLastNameGR"] == "")),0
    ).otherwise(
        1- levenshtein(df["NormalizedPartyFirstLastNameGR"], df["NormalizedFirstLastNameGR"]) 
        / greatest(length(df["NormalizedPartyFirstLastNameGR"]), length(df["NormalizedFirstLastNameGR"]))
    )
)\
.withColumn("LevenshteinFirstFatherNameGR",         
    when(
        ((df["NormalizedPartyFirstFatherNameGR"].isNull()) | (df["NormalizedFirstFatherNameGR"].isNull())) |
        ((df["NormalizedPartyFirstFatherNameGR"] == "") | (df["NormalizedFirstFatherNameGR"] == "")),0
    ).otherwise(
        1- levenshtein(df["NormalizedPartyFirstFatherNameGR"], df["NormalizedFirstFatherNameGR"]) 
        / greatest(length(df["NormalizedPartyFirstFatherNameGR"]), length(df["NormalizedFirstFatherNameGR"]))
    )
)\
.withColumn("LevenshteinBirthDateGR",         
    when(
            (
            (df["NormalizedPartyBirthDate"].isNull()) |
            (df["NormalizedBirthDate"].isNull()) |
            (df["NormalizedPartyBirthDate"] == "") |
            (df["NormalizedBirthDate"] == "") |
            (df["NormalizedPartyBirthDate"].isin(dummy_values_date)) |
            (df["NormalizedBirthDate"].isin(dummy_values_date))
        ), 0
    ).otherwise(
        1-levenshtein(df["NormalizedPartyBirthDate"], df["NormalizedBirthDate"]) 
        / greatest(length(df["NormalizedPartyBirthDate"]), length(df["NormalizedBirthDate"]))
    )   
)\
.withColumn("LevenshteinPhoneNumberGR",         
    when(
        (
        (df["NormalizedPartyPhoneNumber"].isNull()) | 
        (df["NormalizedPhoneNumber"].isNull()) |
        (df["NormalizedPartyPhoneNumber"] == "") | 
        (df["NormalizedPhoneNumber"] == "") |
        (df["NormalizedPartyPhoneNumber"].isin(dummy_values_phonenumber)) |
        (df["NormalizedPhoneNumber"].isin(dummy_values_phonenumber)) 
        ),0
    ).otherwise(
        1-levenshtein(df["NormalizedPartyPhoneNumberGR"], df["NormalizedPhoneNumberGR"]) 
        / greatest(length(df["NormalizedPartyPhoneNumberGR"]), length(df["NormalizedPhoneNumberGR"]))
    )
)

# COMMAND ----------

df=df.withColumn("WeightedAverage",(( 0.15*df["LevenshteinNationalNumber"] + 0.05*df["LevenshteinPassportNumber"] + 0.3*df["LevenshteinNameGR"] + 0.2*df["LevenshteinFirstFatherNameGR"]+0.2*df["LevenshteinBirthDateGR"]+0.1*df["LevenshteinPhoneNumberGR"])) / 1)

df.createOrReplaceTempView("MatchedDatasetRaw")

# COMMAND ----------

MatchedDataset = spark.sql(f"""
select distinct
    case when SourceSystem=PartySourceSystem and PartyKey=PartyPartyKey then 1 else 0 end as SelfMatchFlag
    ,TaxRegistrationNumber
    ,PartyCDI
    ,m.CDI
    ,PartyPortfolioName
    ,PortfolioName
    ,PartySourceSystem
    ,SourceSystem
    ,PartyPartyKey
    ,PartyKey
    ,PartyPartyRepresentKey
    ,PartyRepresentKey
    ,cast(NormalizedPartyFirstLastNameGR as string)
    ,cast(NormalizedFirstLastNameGR as string)
    ,cast(NormalizedPartyFirstFatherNameGR as string)
    ,cast(NormalizedFirstFatherNameGR as string)
    ,cast(NormalizedPartyBirthDate as string)
    ,cast(NormalizedBirthDate as string)
    ,cast(NormalizedPartyNationalNumber as string)
    ,cast(NormalizedNationalNumber as string)
    ,cast(NormalizedPartyPassportNumber as string)
    ,cast(NormalizedPassportNumber as string)
    ,cast(NormalizedPartyPhoneNumberGR as string)
    ,cast(NormalizedPhoneNumberGR as string)
    ,cast(PhoneNumbers as string)
    ,cast(LevenshteinNationalNumber as string)
    ,cast(LevenshteinPassportNumber as string)
    ,cast(LevenshteinNameGR as string)
    ,cast(LevenshteinFirstFatherNameGR as string)
    ,cast(LevenshteinBirthDateGR as string)
    ,cast(LevenshteinPhoneNumberGR   as string)
    ,WeightedAverage
    ,m.BusinessDate
from MatchedDatasetRaw m

left join (
select distinct CDI,BusinessDate,concat_ws(', ', collect_set(coalesce(nullif(PhoneNumber1,''),nullif(PhoneNumber2,''),nullif(PhoneNumber3,'')))) as PhoneNumbers
from  `{env}-cdh`.primary.contactdetails
group by CDI,BusinessDate
) Contact 
on m.CDI=Contact.CDI
and m.BusinessDate=Contact.BusinessDate
""")

MatchedDataset.createOrReplaceTempView("MatchedDataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a View with debtors that only appear in one record

# COMMAND ----------

SelfMatchedDataset = spark.sql("""
SELECT distinct s1.PartyKey,s1.SourceSystem,s1.CDI 
FROM MatchedDataset as s1
left join 
(   SELECT distinct PartyKey,SourceSystem 
     FROM MatchedDataset
     WHERE SelfMatchFlag <> 1
) s2
on s1.SourceSystem =s2.SourceSystem
AND s1.PartyKey = s2.PartyKey
where s2.PartyKey is null
AND s2.SourceSystem is null
""")
SelfMatchedDataset.createOrReplaceTempView("SelfMatchedDataset")

# COMMAND ----------

DebtorUnifiedMatched = spark.sql("""
select a.*
,row_number () over (partition by PartyPartyKey,PartySourceSystem,PartyKey,SourceSystem order by WeightedAverage desc,coalesce(nullif(InheritedTaxRegistrationNumber,''),'0') desc) as RnMatchedPair
from (
select 
case when t1.WeightedAverage >= 0.75 then Tax.PartyCDI else null end as SourceTaxCDI
,CASE WHEN (is_valid_tax_number_udf(t1.TaxRegistrationNumber) IS NULL OR is_valid_tax_number_udf(t1.TaxRegistrationNumber) = false)
    AND t1.WeightedAverage >= 0.75 
    THEN Tax.TaxRegistrationNumber
    ELSE t1.TaxRegistrationNumber
END as InheritedTaxRegistrationNumber
,MatchedCDI
,t1.*   
from MatchedDataset t1
left join 
    (
    SELECT TaxRegistrationNumber,PartyCDI,CDI,BusinessDate,PartyKey,SourceSystem,
    ROW_NUMBER() OVER(PARTITION BY CDI ORDER BY WeightedAverage DESC) as row_num
    FROM MatchedDataset AS t3
    where t3.WeightedAverage >= 0.75
    and is_valid_tax_number_udf(t3.TaxRegistrationNumber)=true
    ) Tax
    on Tax.CDI = t1.CDI
    AND Tax.row_num = 1
    and t1.WeightedAverage >= 0.75
left join (
select distinct CDI,concat_ws(', ', collect_set(PartyCDI)) as MatchedCDI
from MatchedDataset
where WeightedAverage >= 0.75
group by CDI
) MatchedCDI 
on MatchedCDI.CDI=t1.CDI
and t1.WeightedAverage >= 0.75

WHERE concat(t1.SourceSystem,t1.PartyKey)<>concat(t1.PartySourceSystem,t1.PartyPartyKey)


UNION 
-- union records that appear only once in table party
SELECT 
null as SourceTaxCDI
,null as InheritedTaxRegistrationNumber
,null as MatchedCDI
,SelfMatchFlag,TaxRegistrationNumber,PartyCDI,t1.CDI,PartyPortfolioName,PortfolioName,PartySourceSystem,t1.SourceSystem
,PartyPartyKey,t1.PartyKey,PartyPartyRepresentKey,PartyRepresentKey,NormalizedPartyFirstLastNameGR
,NormalizedFirstLastNameGR,NormalizedPartyFirstFatherNameGR,NormalizedFirstFatherNameGR
,NormalizedPartyBirthDate,NormalizedBirthDate,NormalizedPartyNationalNumber,NormalizedNationalNumber
,NormalizedPartyPassportNumber,NormalizedPassportNumber,NormalizedPartyPhoneNumberGR,NormalizedPhoneNumberGR,PhoneNumbers
,LevenshteinNationalNumber,LevenshteinPassportNumber,LevenshteinNameGR,LevenshteinFirstFatherNameGR
,LevenshteinBirthDateGR,LevenshteinPhoneNumberGR, null as WeightedAverage,BusinessDate
from MatchedDataset t1
INNER join SelfMatchedDataset s
on s.PartyKey=t1.PartyKey
AND s.PartyKey=t1.PartyPartyKey
AND s.SourceSystem=t1.SourceSystem
AND s.SourceSystem=t1.PartySourceSystem
)a

""")

DebtorUnifiedMatched.createOrReplaceTempView("DebtorUnifiedMatched")

# COMMAND ----------

final_df2 = spark.sql("""
select SourceTaxCDI ,InheritedTaxRegistrationNumber, MatchedCDI, SelfMatchFlag,TaxRegistrationNumber,
PartyCDI, CDI, PartyPortfolioName, PortfolioName, PartySourceSystem, SourceSystem,
PartyPartyKey, PartyKey,PartyPartyRepresentKey, PartyRepresentKey,
NormalizedPartyFirstLastNameGR, NormalizedFirstLastNameGR, NormalizedPartyFirstFatherNameGR, 
NormalizedFirstFatherNameGR, NormalizedPartyBirthDate, NormalizedBirthDate, NormalizedPartyNationalNumber,
NormalizedNationalNumber, NormalizedPartyPassportNumber, NormalizedPassportNumber,
NormalizedPartyPhoneNumberGR,NormalizedPhoneNumberGR, PhoneNumbers,
LevenshteinNationalNumber, LevenshteinPassportNumber, LevenshteinNameGR, LevenshteinFirstFatherNameGR, 
LevenshteinBirthDateGR, LevenshteinPhoneNumberGR, WeightedAverage,BusinessDate
from DebtorUnifiedMatched 
where RnMatchedPair =1
""")

final_df2.createOrReplaceTempView("final_df2")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write DataFrame to Delta Lake based on 'unity' value
# MAGIC

# COMMAND ----------

if unity == "True":
    final_df2.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path2) # in order to write in delta_lake
else:
    final_df2.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path2) # in order to write in delta_lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create and Feed table DebtorUnifiedPartyKey

# COMMAND ----------

table_name3 = 'DDebtorUnifiedPartyKey'

# COMMAND ----------

schema3 = (
"CDI string,"
"TaxRegistrationNumber string,"
"PartyKey string,"
"SourceSystem string,"
"BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path3 = ""
 
# ================== Create Delta Table ==================
if unity == "True":
    derivative_table_path3 = f"`{env}-cdh`.derivativeactive.{table_name3}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path3):
        delta_table_creator.create_unity_delta_table(
            spark, derivative_table_path3, partition_columns, schema3
        )
else:
    derivative_table_path3 = f"s3://{env}-ec1-cepal-derivative-zone/{table_name3}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path3):
        delta_table_creator.create_delta_table(
            spark, derivative_table_path3, partition_columns, schema3
        )
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path3,
    run_date,  # type: ignore
)

# COMMAND ----------

final_df3 = spark.sql(f"""
select distinct d.CDI,d.TaxRegistrationNumber,d.PartyKey,d.SourceSystem,d.BusinessDate
from `{env}-cdh`.derivative.ddebtorunifiedidentical d
where d.Rule='TaxRegistrationNumber'
and d.BusinessDate={run_date!r}
union
select distinct d2.CDI,d2.InheritedTaxRegistrationNumber as TaxRegistrationNumber,d2.PartyKey,d2.SourceSystem,d2.BusinessDate
from `{env}-cdh`.derivative.ddebtorunifiedidentical d2
where d2.Rule<>'TaxRegistrationNumber'
and d2.BusinessDate={run_date!r}
union
select distinct c.CDI,c.InheritedTaxRegistrationNumber  as TaxRegistrationNumber,c.PartyKey,c.SourceSystem,c.BusinessDate
from `{env}-cdh`.derivative.DDebtorUnifiedMatched c
where c.WeightedAverage >=0.75
and c.BusinessDate={run_date!r}
union
select distinct p.PartyCDI as CDI,p.InheritedTaxRegistrationNumber  as TaxRegistrationNumber,p.PartyPartyKey as PartyKey,p.PartySourceSystem as SourceSystem,p.BusinessDate
from `{env}-cdh`.derivative.DDebtorUnifiedMatched p
left join `{env}-cdh`.derivative.ddebtorunifiedidentical i
on i.CDI=p.PartyCDI
and i.BusinessDate=p.BusinessDate
where p.BusinessDate={run_date!r}
and p.WeightedAverage>=0.75
and i.CDI is null
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write DataFrame to Delta Lake based on 'unity' value
# MAGIC

# COMMAND ----------

if unity == "True":
    final_df3.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path3) # in order to write in delta_lake
else:
    final_df3.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path3) # in order to write in delta_lake

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create and Feed Table DebtorUnified

# COMMAND ----------

table_name4 = 'DDebtorUnified'

# COMMAND ----------

schema4 = (
"PartyTypeCode string,"
"PartyType string,"
"OCWStatus string,"
"DeceasedFlag boolean,"
"FirstNameGR string,"
"FirstNameENG string,"
"LastNameGR string,"
"LastNameENG string,"
"FirstFatherNameGR string,"
"FirstFatherNameENG  string,"
"LastFatherNameGR string,"
"LastFatherNameENG string,"
"LastMotherNameGR string,"
"LastMotherNameENG string,"
"LegalNameGR string,"
"LegalNameENG string,"
"PrimaryCommercialGR string,"
"PrimaryCommercialENG  string,"
"GenderCode string,"
"Gender string,"
"MaritalStatusCode string,"
"MaritalStatus string,"
"BirthDate string,"
"BirthPlace string,"
"DeathDate date,"
"TaxRegistrationNumber string,"
"PassportNumber string,"
"PassportIssuingAuthority  string,"
"PassportIssueDate date,"
"PassportExpiredDate date,"
"NationalNumber string,"
"NationalNumberIssuingAuthority  string,"
"NationalNumberIssueDate date,"
"NationalNumberExpiredDate date,"
"MilitaryNumber string,"
"MilitaryAuthority string,"
"MilitaryIssueDate date,"
"MilitaryExpiredDate date,"
"SocialSecurityNumber string,"
"EnterpriseStartDate string,"
"EnterpriseEndDate date,"
"EconomicActivityCode int,"
"EconomicActivity string,"
"GeneralCommercialRegister string,"
"TotalIncome string,"
"BalanceSheetTotalAmount string,"
"AnnualTurnoverAmount string,"
"GroupCode string,"
"GroupName string,"
"LegalFormCode int,"
"LegalForm string,"
"BusinessDate date"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create - Delete Delta Table

# COMMAND ----------

partition_columns = ["BusinessDate"]
derivative_table_path4 = ""
 
# ================== Create Delta Table ==================
if unity == "True":
    derivative_table_path4 = f"`{env}-cdh`.derivativeactive.{table_name4}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path4):
        delta_table_creator.create_unity_delta_table(
            spark, derivative_table_path4, partition_columns, schema4
        )
else:
    derivative_table_path4 = f"s3://{env}-ec1-cepal-derivative-zone/{table_name4}"
    if not DeltaTable.isDeltaTable(spark, derivative_table_path4):
        delta_table_creator.create_delta_table(
            spark, derivative_table_path4, partition_columns, schema4
        )
 
 
# ================== Delete From Delta Table ==================
 
transform.delete_from_unity_table(
    spark,
    derivative_table_path4,
    run_date,  # type: ignore
)

# COMMAND ----------

final_df4 = spark.sql(f"""
select distinct PartyTypeCode,PartyType,OCWStatus,DeceasedFlag,FirstNameGR,FirstNameENG,LastNameGR,LastNameENG,FirstFatherNameGR,FirstFatherNameENG,LastFatherNameGR,LastFatherNameENG,LastMotherNameGR,LastMotherNameENG,LegalNameGR,LegalNameENG,PrimaryCommercialGR,PrimaryCommercialENG,GenderCode,Gender,MaritalStatusCode,MaritalStatus,BirthDate,BirthPlace,DeathDate,TaxRegistrationNumber,PassportNumber,PassportIssuingAuthority,PassportIssueDate,PassportExpiredDate,NationalNumber,NationalNumberIssuingAuthority,NationalNumberIssueDate,NationalNumberExpiredDate,MilitaryNumber,MilitaryAuthority,MilitaryIssueDate,MilitaryExpiredDate,SocialSecurityNumber,EnterpriseStartDate,EnterpriseEndDate,EconomicActivityCode,EconomicActivity,GeneralCommercialRegister,TotalIncome,BalanceSheetTotalAmount,AnnualTurnoverAmount,GroupCode,GroupName,LegalFormCode,LegalForm,BusinessDate
from (                          
select distinct SourceSystem,PartyTypeCode,PartyType,OCWStatus,DeceasedFlag,FirstNameGR,FirstNameENG,LastNameGR,LastNameENG,FirstFatherNameGR,FirstFatherNameENG,LastFatherNameGR,LastFatherNameENG,LastMotherNameGR,LastMotherNameENG,LegalNameGR,LegalNameENG,PrimaryCommercialGR,PrimaryCommercialENG,GenderCode,Gender,MaritalStatusCode,MaritalStatus,BirthDate,BirthPlace,DeathDate,p.TaxRegistrationNumber
,PassportNumber,PassportIssuingAuthority,PassportIssueDate,PassportExpiredDate,NationalNumber,NationalNumberIssuingAuthority,NationalNumberIssueDate,NationalNumberExpiredDate,MilitaryNumber,MilitaryAuthority,MilitaryIssueDate,MilitaryExpiredDate,SocialSecurityNumber,EnterpriseStartDate,EnterpriseEndDate,EconomicActivityCode,EconomicActivity,GeneralCommercialRegister,TotalIncome,BalanceSheetTotalAmount,AnnualTurnoverAmount,GroupCode,GroupName,LegalFormCode,LegalForm, p.BusinessDate
,row_number() OVER (PARTITION BY p.TaxRegistrationNumber ORDER BY 
            CASE    WHEN SourceSystem ='profits' THEN 1
                    WHEN SourceSystem ='qca' THEN 2
                    WHEN SourceSystem ='qc' THEN 3
                    WHEN SourceSystem ='arotron' THEN 4
                    ELSE 5 END
                    ) RN
from `{env}-cdh`.primary.party p
inner join
(       select distinct CDI,BusinessDate
        from `{env}-cdh`.derivative.ddebtorunifiedidentical d
        where 1=1
        and d.BusinessDate={run_date!r}
        and d.CDI is not null
        union
        select distinct CDI,BusinessDate
        from `{env}-cdh`.derivative.DDebtorUnifiedMatched d
        where 1=1
        and d.BusinessDate={run_date!r}
        and d.CDI is not null
        and WeightedAverage >=0.75
        union 
        select distinct PartyCDI as CDI,BusinessDate
        from `{env}-cdh`.derivative.DDebtorUnifiedMatched d
        where 1=1
        and d.BusinessDate={run_date!r}
        and d.PartyCDI is not null
        and WeightedAverage >=0.75
        and PartyCDI<>SourceTaxCDI
) cdi
on p.BusinessDate=cdi.BusinessDate
and p.CDI=cdi.CDI
and p.BusinessDate={run_date!r}
and cdi.CDI is not null

)a
where RN=1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write DataFrame to Delta Lake based on 'unity' value
# MAGIC

# COMMAND ----------

if unity == "True":
    final_df4.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(derivative_table_path4) # in order to write in delta_lake
else:
    final_df4.write \
        .format("delta") \
        .mode("append") \
        .save(derivative_table_path4) # in order to write in delta_lake
