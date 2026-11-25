# Databricks notebook source
# MAGIC %run ./02-DS_utils/WAL_NPV_Calculation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.Portfolioid as `PortfolioId`,
# MAGIC        a.CDI AS `key`,
# MAGIC        a.businessDate AS `Date`,
# MAGIC        SUM(a.GrossRecoveries) AS `Amount`
# MAGIC FROM `qa-cdh`.derivative.DBPCepalRecoveriesParty a
# MAGIC LEFT JOIN (
# MAGIC     SELECT a.BusinessDate,
# MAGIC            CDI,
# MAGIC            MAX(DATEDIFF(a.Month, a.BusinessDate)) AS Duration
# MAGIC     FROM `qa-cdh`.derivative.DBPCepalRecoveriesParty a
# MAGIC     WHERE keytype = 'CDI'
# MAGIC       AND a.PortfolioName IN ('Symbol', 'Neptune')
# MAGIC     GROUP BY a.BusinessDate, CDI
# MAGIC ) b ON a.CDI = b.CDI AND a.BusinessDate = b.BusinessDate
# MAGIC LEFT JOIN (
# MAGIC     SELECT a.BusinessDate,
# MAGIC            a.PortfolioName,
# MAGIC            a.cdi,
# MAGIC            SUM(a.gbv) AS GBV
# MAGIC     FROM `qa-cdh`.derivative.dexposure a
# MAGIC     GROUP BY a.BusinessDate, a.PortfolioName, a.cdi
# MAGIC ) c ON a.CDI = c.CDI AND a.BusinessDate = c.BusinessDate
# MAGIC LEFT JOIN (
# MAGIC     SELECT *,
# MAGIC            CASE WHEN d.CollectionType = 'Payments' THEN d.Amount END AS PaymentAmountPreonboarding,
# MAGIC            CASE WHEN d.CollectionType = 'Liquidations' THEN d.Amount END AS LiquidationAmountPreonboarding,
# MAGIC            CASE WHEN d.CollectionType = 'Disbursements' THEN d.Amount END AS DisbursementAmountPreonboarding,
# MAGIC            CASE WHEN d.CollectionType = 'Payment from AB' THEN d.Amount END AS PaymentAmountFromABPreonboarding
# MAGIC     FROM `qa-cdh`.reference.collections_preonboarding d
# MAGIC ) d ON a.cdi = d.DebtorCode AND REPLACE(d.DebtorCode, 'NULL', NULL) IS NOT NULL
# MAGIC left join  `qa-cdh`.reference.portfolio p on           p.Prefix= substring(a.CDI,1,3)
# MAGIC WHERE keytype = 'CDI'
# MAGIC   AND a.PortfolioName IN ('Symbol', 'Neptune')
# MAGIC --  AND a.BusinessDate = '2024-07-02'
# MAGIC GROUP BY a.PortfolioName, a.CDI, a.businessDate, p.Portfolioid;

# COMMAND ----------

data_per_portfolio = _sqldf

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `qa-cdh`.reference.portfolio_discount_rate

# COMMAND ----------

irr = _sqldf

# COMMAND ----------

output_table = calculate_WAL_NPV(data_per_portfolio, irr)

# COMMAND ----------

display(output_table[16])

# COMMAND ----------


