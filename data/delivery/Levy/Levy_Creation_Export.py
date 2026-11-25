# Databricks notebook source
from typing import Any

from delta import DeltaTable  # type: ignore
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    from_utc_timestamp,
    lit,
    sha2,
    substring,
    trim,
    when,
    months_between,
    dateadd,
    datediff,
)
 
from io import BytesIO
import pandas as pd
import json
 
from datetime import (datetime, date)
from pytz import timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
sys.path.append("../aws_utils")
import aws_utils
# Get Environment Prefix

# COMMAND ----------

env = aws_utils.get_env_prefix()

# COMMAND ----------

import os

# COMMAND ----------

print(env)

# COMMAND ----------

try:
    add_hoc_date = dbutils.widgets.get("date")
    dbutils.jobs.taskValues.set(key = "running_date", value = "add_hoc_date")

except Exception as e:
    # You can set a default or handle as needed
    add_hoc_date = None


# COMMAND ----------

spark.sql(f"""
          CREATE VOLUME IF NOT EXISTS `{env}-cdh`.`delivery-deprecated`.levy_export
          """)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{env}-cdh`.`delivery-deprecated`.levyreports (
        StartDT TIMESTAMP,
        portfolioName STRING,
        portfolioid STRING,
        AccountNumber STRING,
        AssetSegment STRING,
        Currency STRING,
        LoanStatus STRING,
        AccountStatus STRING,
        Starttotalbalance DECIMAL(18,2),
        StartURL STRING,
        EndDT TIMESTAMP,
        EndAccountStatus STRING,
        Endtotalbalance DECIMAL(18,2),
        EndURL STRING,
        PaymentURL STRING,
        LevyRate decimal(19,6)
    )
""")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{env}-cdh`.`delivery-deprecated`.levyreporthistory (
        ReportDate DATE,
        StartDT TIMESTAMP,
        portfolioName STRING,
        portfolioid STRING,
        AccountNumber STRING,
        AssetSegment STRING,
        Currency STRING,
        LoanStatus STRING,
        AccountStatus STRING,
        Starttotalbalance DECIMAL(18,2),
        StartURL STRING,
        EndDT TIMESTAMP,
        EndAccountStatus STRING,
        Endtotalbalance DECIMAL(18,2),
        EndURL STRING,
        PaymentURL STRING,
        LevyRate decimal(19,6)
    )
""")

# COMMAND ----------

from datetime import date

# Get the current date
today = date.today()

# Print the result
print(today)

# COMMAND ----------

output_path_date = today.strftime("%Y%m%d")
print(output_path_date)

# COMMAND ----------

run_date = spark.sql(f"""select min(CalendarDate) as RunDate
                     from `{env}-cdh`.reference.ref_calendar
                     where DayName = 'Tuesday' 
                     and IsBusinessDay = 1
                     and BusinessDayOfMonth > 2
                     and month(CalendarDate) = month(current_date())
                     and year(CalendarDate) = year(current_date())""").collect()[0]["RunDate"]
print(run_date)


# COMMAND ----------

if add_hoc_date != None:

    # Parse it into a datetime object
    date_obj = datetime.strptime(add_hoc_date, "%Y%m%d")

    # Format it to the desired format
    formatted_date = date_obj.strftime("%Y-%m-%d")

    print(formatted_date)
    today = formatted_date
   

# COMMAND ----------

if today != run_date:
    print("no run")
    dbutils.jobs.taskValues.set(key = "continue_run", value = "false")
    dbutils.notebook.exit("Stopping notebook execution")

# COMMAND ----------

# Get start of the month and end of the month
param_dates = spark.sql(f"""
SELECT 
    cal1.PreviousBusinessDay AS prv_eom_month,
    ADD_MONTHS(DATE_ADD(c.PreviousMonthLastDate, 1), -1) AS som_month,
    c.PreviousMonthLastWorkingDate  AS eom_month
FROM `{env}-cdh`.reference.ref_calendar c
JOIN `{env}-cdh`.reference.ref_calendar cal1 
    ON cal1.CalendarDate = ADD_MONTHS(DATE_ADD(c.PreviousMonthLastDate, 1), -1)
WHERE c.CalendarDate = CURRENT_DATE()""")

# Show the result
display(param_dates)

param_dates.createOrReplaceTempView("param_dates")


# COMMAND ----------

param_dates_eom_month = param_dates.collect()[0]["eom_month"]
print(param_dates_eom_month)

# COMMAND ----------

has_data = spark.sql(f"""
    SELECT COUNT(1) as has_data
    FROM `{env}-cdh`.`delivery-deprecated`.levyreports
    WHERE enddt = '{param_dates_eom_month}'
""").collect()[0]["has_data"]

display(has_data)

if has_data != 0:
    print("Don't run again")
    dbutils.jobs.taskValues.set(key = "continue_run", value = "false")
    dbutils.notebook.exit("Stopping notebook execution")
    dbutils.notebook.exit(f"For date '{param_dates_eom_month}' the notebook has run again. Stopping execution from within a workflow.")

# COMMAND ----------

# Get start of the month and end of the month
param_dt = spark.sql(f"""
SELECT 
    cal1.PreviousBusinessDay AS prv_eom_month,
    ADD_MONTHS(DATE_ADD(c.PreviousMonthLastDate, 1), -1) AS som_month,
    c.PreviousMonthLastWorkingDate  AS eom_month
FROM `{env}-cdh`.reference.ref_calendar c
JOIN `{env}-cdh`.reference.ref_calendar cal1 
    ON cal1.CalendarDate = ADD_MONTHS(DATE_ADD(c.PreviousMonthLastDate, 1), -1)
WHERE c.CalendarDate = CURRENT_DATE()""")

# Show the result
display(param_dt)

param_dt.createOrReplaceTempView("param_dt")


# COMMAND ----------

# Get start of the month and end of the month
param_dt = spark.sql(f"""
SELECT 
    cal1.PreviousBusinessDay AS prv_eom_month,
    ADD_MONTHS(DATE_ADD(c.PreviousMonthLastDate, 1), -1) AS som_month,
    c.PreviousMonthLastWorkingDate  AS eom_month
FROM `{env}-cdh`.reference.ref_calendar c
JOIN `{env}-cdh`.reference.ref_calendar cal1 
    ON cal1.CalendarDate = ADD_MONTHS(DATE_ADD(c.PreviousMonthLastDate, 1), -1)
WHERE c.CalendarDate = CURRENT_DATE()""")

# Show the result
display(param_dt)

param_dt.createOrReplaceTempView("param_dt")


# COMMAND ----------

end_of_month = spark.sql("""SELECT eom_month
                         FROM param_dt""").collect()[0][0]

print(f'End of month: {end_of_month}')

prv_eom_month = spark.sql("""SELECT prv_eom_month
                         FROM param_dt""").collect()[0][0]

print(f'End of previous month: {prv_eom_month}')


# COMMAND ----------

selected_source_data = spark.sql(f""" SELECT *
                                 FROM `{env}-cdh`.derivative.dexposure
                                 WHERE SourceSystem = 'profits' 
                                 """)
                                 
#business date
selected_source_data.createOrReplaceTempView("filtered_exposure")

# COMMAND ----------

selected_portfolios = spark.sql(f""" SELECT *
                                 FROM `{env}-cdh`.primary.portfolio
                                 """)

selected_portfolios.createOrReplaceTempView("filtered_portfolios")

# COMMAND ----------

new_portfolios = spark.sql(f"""
SELECT 
    p.PortfolioId, 
    p.PortfolioName,  
    COALESCE(p.unit_code_profits, 0) AS unit_code_profits, 
    MIN(f.BusinessDate) AS prv_eom_month, 
    MIN(f.BusinessDate) AS som_month, 
    d.eom_month
FROM filtered_exposure f
JOIN filtered_portfolios p ON p.PortfolioName = f.PortfolioName 
CROSS JOIN param_dt d
WHERE p.AssignmentDate BETWEEN d.som_month AND d.eom_month
AND COALESCE(p.unit_code_profits, 0) > 0
AND f.BusinessDate > d.prv_eom_month
AND f.BusinessDate <= d.eom_month
GROUP BY 
    p.PortfolioId, 
    p.PortfolioName,  
    p.unit_code_profits, 
    d.eom_month
""")

new_portfolios.createOrReplaceTempView("new_portfolios")


# COMMAND ----------

portfolio_final = spark.sql(f"""
SELECT DISTINCT * 
FROM (
    SELECT *, 
           RANK() OVER(PARTITION BY PortfolioId ORDER BY prv_eom_month DESC, som_month DESC) AS rnk
    FROM (
        SELECT 
            p.PortfolioId, 
            p.PortfolioName,  
            COALESCE(p.unit_code_profits, 0) AS unit_code_profits, 
            dd.prv_eom_month, 
            dd.som_month, 
            dd.eom_month
        FROM filtered_portfolios p
        CROSS JOIN param_dt dd
        WHERE COALESCE(p.unit_code_profits, 0) > 0
        UNION ALL
        SELECT 
            np.PortfolioId, 
            np.PortfolioName, 
            np.unit_code_profits, 
            np.prv_eom_month, 
            np.som_month, 
            np.eom_month
        FROM new_portfolios np
    ) d
) ranked_data
WHERE rnk = 1
""")

portfolio_final.createOrReplaceTempView("portfolio_final")

# COMMAND ----------


prv = spark.sql(
    """SELECT 
        f.ExposureKey AS loanid,
        f.BusinessDate,
        p.PortfolioId AS portfolioid,
        f.ExposureStatus,
        f.OutStandingTotalBalance,
        (f.UnrealizedInterest + f.PenaltyUnrealizedinterest + f.DueUnrealizedinterest) AS URLAmount
    FROM filtered_exposure AS f
    INNER JOIN portfolio_final AS p
        ON f.PortfolioName = p.PortfolioName 
    WHERE f.BusinessDate = p.prv_eom_month
    """
)
prv.createOrReplaceTempView("prv")


# COMMAND ----------

nxt = spark.sql("""
    SELECT 
        f.ExposureKey AS loanid,
        f.BusinessDate AS ReferenceDate,
        p.portfolioid,
        f.ExposureStatus AS loanstatus,
        f.OutStandingTotalBalance,
        (f.UnrealizedInterest + f.PenaltyUnrealizedinterest + f.DueUnrealizedinterest) AS URLAmount
    FROM filtered_exposure AS f
    INNER JOIN portfolio_final AS p 
        ON f.PortfolioName = p.PortfolioName
   WHERE f.BusinessDate = p.eom_month
    """
)
nxt.createOrReplaceTempView("nxt")

# COMMAND ----------

dexposure_current = spark.sql(f"""
                              
                              select *
                              from (
                              SELECT *,row_number() over (partition by ExposureKey order by BusinessDate desc) AS rownum
                              FROM `{env}-cdh`.derivative.dexposure
                              where sourcesystem = 'profits'
                            --  and PortfolioName = 'EUCLID'
                              )
                              where rownum = 1
                              """)
dexposure_current.createOrReplaceTempView("dexposure_current")


# COMMAND ----------

basic_data = spark.sql("""
    SELECT 
        prv.BusinessDate AS StartDT,
        p.portfolioName,
        p.portfolioid,
        la.ExposureKey AS ACCOUNTNUMBER,
        AssetSegment AS AssetSegment,
        la.Currency,
        case when prv.ExposureStatus ='Normal' then 'Normal' when prv.ExposureStatus ='Overdue' then 'Overdue' when prv.ExposureStatus ='Define Delay' then 'Defined Delay' else 'Write-off' end LoanStatus,

     --   prv.ExposureStatus AS LoanStatus,
        'Active' AS AccountStatus,
        prv.OutStandingTotalBalance AS StartTotalBalance,
        prv.URLAmount AS StartURL,
        COALESCE(nxt.ReferenceDate, p.eom_month) AS EndDT,
        CASE 
            WHEN la.IsActiveFlag != TRUE THEN 'Closed' -- na dw oti paizei 
            ELSE 'Active'
        END AS EndAccountStatus,
        COALESCE(nxt.OutStandingTotalBalance, 0) AS EndTotalBalance,
        COALESCE(nxt.URLAmount, 0) AS EndURL,
        CAST(0 AS DECIMAL(19, 2)) AS PaymentURL,
        la.N128Rate
    FROM dexposure_current AS la 
    INNER JOIN portfolio_final AS p ON p.PortfolioName = la.PortfolioName
    INNER JOIN prv ON prv.loanid = la.ExposureKey 
    LEFT JOIN nxt ON nxt.loanid = la.ExposureKey
""")

basic_data.createOrReplaceTempView("basic_data")


# COMMAND ----------

max_dates_account_transactions = spark.sql(f"""
                            Select ExposureKey, max(BusinessDate) as max_date
                            FROM `{env}-cdh`.primary.accounttransaction
                            where BusinessDate <= '{end_of_month}' 
                            and JustificationCode IN (74431, 41001, 43001)
                            group by ExposureKey
                              """)

max_dates_account_transactions.createOrReplaceTempView("max_dates_account_transactions")


# COMMAND ----------

account_transaction_data = spark.sql(f"""
                                    SELECT a.*
                                    FROM `{env}-cdh`.primary.accounttransaction a
                                    JOIN portfolio_final p ON p.PortfolioName = a.PortfolioName
                                    inner join max_dates_account_transactions t
                                    CROSS JOIN param_dt d
                                    on a.ExposureKey = t.ExposureKey 
                                    and a.BusinessDate = t.max_date
                                    WHERE  a.JustificationCode IN (74431, 41001, 43001)
                                    and a.TransactionDate between d.som_month AND d.eom_month
                                    """)

account_transaction_data.createOrReplaceTempView("account_transaction_data")

# COMMAND ----------

source_basic_data = spark.sql(f"""SELECT 
                    a.ExposureKey, 
                    SUM(a.UnrealizedInterestAmount + a.UnrealizedPenaltyInterestAmount) AS urlPayAmount
                FROM account_transaction_data a
                JOIN portfolio_final p ON p.PortfolioName = a.PortfolioName
                GROUP BY a.ExposureKey
                HAVING SUM(a.UnrealizedInterestAmount + a.UnrealizedPenaltyInterestAmount) > 0""")

source_basic_data.createOrReplaceTempView("source_basic_data")

# COMMAND ----------

updated_data = spark.sql(f"""SELECT 
                            t.StartDT,
                            t.portfolioName,
                            t.portfolioid,
                            t.ACCOUNTNUMBER,
                            t.AssetSegment,
                            t.LoanStatus,
                            t.Currency,
                            t.AccountStatus,
                            t.StartTotalBalance,
                            t.StartURL,
                            t.EndDT,
                            t.EndAccountStatus,
                            t.EndTotalBalance,
                            t.EndURL,
                            COALESCE(s.urlPayAmount, 0) as PaymentURL,
                            t.N128Rate
                            FROM basic_data t
                            LEFT JOIN source_basic_data s
                            ON s.ExposureKey = t.ACCOUNTNUMBER;
                        """)

updated_data.createOrReplaceTempView("basic_data")

# COMMAND ----------

levy_data = spark.sql(f"""SELECT DISTINCT 
                            StartDT
                            ,portfolioName
                            ,portfolioid  
                            , AccountNumber
                            ,AssetSegment
                            ,Currency
                            ,LoanStatus 
                            ,AccountStatus
                            ,Starttotalbalance
                            ,StartURL
                            ,EndDT
                            ,EndAccountStatus
                            ,Endtotalbalance
                            ,EndURL
                            ,PaymentURL
                            ,N128Rate
                            from basic_data
                            where PaymentURL<>0
                        """)
levy_data.createOrReplaceTempView("levy_data")

# COMMAND ----------

deleted_rows = spark.sql(f"""DELETE FROM `{env}-cdh`.`delivery-deprecated`.levyreports""")
display(deleted_rows)


# COMMAND ----------

spark.sql(f"""INSERT INTO  `{env}-cdh`.`delivery-deprecated`.levyreports
    SELECT * FROM levy_data
   """)


# COMMAND ----------



# COMMAND ----------

deleted_history_rows = spark.sql(f"""DELETE FROM `{env}-cdh`.`delivery-deprecated`.levyreporthistory
                         where ReportDate = '{end_of_month}'""")
                         
display(deleted_history_rows)

# COMMAND ----------

spark.sql(f"""INSERT INTO  `{env}-cdh`.`delivery-deprecated`.levyreporthistory
    SELECT EndDT AS reportDate, * FROM `{env}-cdh`.`delivery-deprecated`.levyreports
   """)


# COMMAND ----------

levy_export_data = spark.sql(f"""SELECT  CAST(StartDT AS DATE),
        portfolioName,
        AccountNumber,
        AssetSegment,
        Currency,
        LoanStatus,
        AccountStatus,
        Starttotalbalance,
        StartURL,
        CAST(EndDT AS DATE),
        EndAccountStatus,
        Endtotalbalance,
        EndURL,
        PaymentURL,
        LevyRate FROM `{env}-cdh`.`delivery-deprecated`.levyreports""")

# COMMAND ----------

unique_values = [row['portfolioName'] for row in levy_export_data.select('portfolioName').distinct().collect()]


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import os
from io import BytesIO
from datetime import datetime


# COMMAND ----------

# Convert to datetime object
date_obj = datetime.strptime(run_date, "%Y-%m-%d")

# Reformat to desired format (YYYYMMDD)
formatted_date = date_obj.strftime("%Y%m%d")

# COMMAND ----------

output_path = f"/Volumes/{env}-cdh/delivery-deprecated/levy_export/{formatted_date}/"
full_export_output = os.path.join(output_path, f"LevyReport_{formatted_date}.xlsx")

print(full_export_output)



# COMMAND ----------

# Ensure the directory exists, create it if it doesn't
os.makedirs(os.path.dirname(full_export_output), exist_ok=True)


# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Export

# COMMAND ----------

# Create an Excel writer object
output_buffer = BytesIO()
with pd.ExcelWriter(output_buffer) as writer:
    for value in unique_values:
        # Filter the DataFrame for the current value
        filtered_df = levy_export_data.filter(col('portfolioName') == value)
        
        # Save the subset to a temporary CSV file
        temp_file = os.path.join(output_path, f"{value}.csv")
        filtered_df.coalesce(1).write.csv(temp_file, header=True, mode="overwrite")
        
        # Read the CSV back into Pandas
        csv_files = [f for f in os.listdir(temp_file) if f.endswith(".csv")]
        if csv_files:
            pandas_df = pd.read_csv(os.path.join(temp_file, csv_files[0]))
            
            # Write to an Excel sheet
            sheet_name = str(value)[:31]  # Excel sheet names must be <= 31 characters
            pandas_df.to_excel(writer, sheet_name=sheet_name, index=False)
        for file in os.listdir(temp_file):
            os.remove(os.path.join(temp_file, file))

with open(full_export_output, "wb") as f:
    f.write(output_buffer.getvalue())

