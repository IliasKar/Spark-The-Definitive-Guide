# Databricks notebook source


# COMMAND ----------

# ##Initialization

# COMMAND ----------

dbutils.widgets.text("run_date", "", "Run Date")
dbutils.widgets.text("portfolio", "", "Portfolio")
dbutils.widgets.text("portfolio_exact_match", "false", "Exact Match")

# COMMAND ----------

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

import os
import sys
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace" + notebook_path.rsplit("/MegaDataTape/", 1)[0]
    sys.path.insert(0, os.path.join(repo_root, "aws_utils"))
    sys.path.insert(0, os.path.join(repo_root, "01-utils"))
except Exception:
    sys.path.append("../../aws_utils")
    sys.path.append("../../01-utils")

from aws_utils import get_env_prefix, get_env_fullname
from helpers import get_validated_eom_run_date, build_portfolio_filter, delete_from_unity_table

env = get_env_prefix()
env_name = get_env_fullname()

partition_columns = ["ReportingDate"]
run_date = get_validated_eom_run_date(spark, env_name, dbutils.widgets.get("run_date"))
portfolio_filter, portfolio_param, portfolio_display = build_portfolio_filter(dbutils.widgets.get("portfolio"), portfolio_exact_match=dbutils.widgets.get("portfolio_exact_match").strip().lower() == "true")
target_area = f"`{env}-cdh-delivery`.`mega-datatape`"

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
print(f"Executing: {notebook_name} | run_date: {run_date} | portfolio: {portfolio_display}")

# COMMAND ----------

# ##Import Tables

# COMMAND ----------

dexposure = spark.sql(f"""
                      SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.derivative.get_all_exposures_by_business_date(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                      """)

dexposure.createOrReplaceTempView("dexposure")

dexposure_full = spark.sql(f"""
                      select * 
                      from `{env}-cdh`.derivative.dexposure
                      where BusinessDate = {run_date!r}
                      """)

dexposure_full.createOrReplaceTempView("dexposure_full")

dparty = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r} and sourcesystem='profits'
                   AND {portfolio_filter}
                      """)

dparty.createOrReplaceTempView("dparty")

dparty_full = spark.sql(f"""
                   select * 
                   from `{env}-cdh`.derivative.dparty
                   where BusinessDate = {run_date!r} 
                   AND {portfolio_filter}
                      """)

dparty_full.createOrReplaceTempView("dparty_full")

dunderlaw = spark.sql(f"""
                      select *
                      from `{env}-cdh`.derivative.dunderlaw
                      where BusinessDate = {run_date!r} 
                      """)

dunderlaw.createOrReplaceTempView("dunderlaw")

portfolio = spark.sql(f"""
                       select * 
                       from `{env}-cdh`.primary.portfolio
                       where BusinessDate = {run_date!r} 
                       """)

portfolio.createOrReplaceTempView("portfolio")                       

paymentschedule = spark.sql(f"""
                            select * 
                            from `{env}-cdh`.derivative.dpaymentschedule
                            where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                            AND {portfolio_filter}
                            """)

paymentschedule.createOrReplaceTempView("paymentschedule")

interaction = spark.sql(f"""
                        select *
                        from `{env}-cdh`.primary.interaction
                        --where BusinessDate = {run_date!r}  -- EK 20260309 update for interaction table historical data
                        where Businessdate = (select max(Businessdate) from `{env}-cdh`.primary.interaction)
                        AND {portfolio_filter}
                        """)

interaction.createOrReplaceTempView("interaction")

dlienanalysisltv = spark.sql(f"""
                             select * 
                             from `{env}-cdh`.derivative.dlienanalysisltv 
                             where BusinessDate = {run_date!r}
                             """)

dlienanalysisltv.createOrReplaceTempView("dlienanalysisltv")

danalyticsmetrics = spark.sql(f"""
                              select *
                              from `{env}-cdh`.derivative.danalyticsmetrics
                              where BusinessDate = {run_date!r}
                              """)

danalyticsmetrics.createOrReplaceTempView("danalyticsmetrics")

danalyticsmetrics_som = spark.sql(f"""          -- EK 2026715
                              select *
                              from `{env}-cdh`.derivative.danalyticsmetrics
                              WHERE BusinessDate=(CASE WHEN Portfolioname='ASTRAEA PORTFOLIO RETAIL' AND {run_date!r}<='2026-03-31' THEN '2026-03-31'
                              ELSE (SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) END) 
                              """)

danalyticsmetrics_som.createOrReplaceTempView("danalyticsmetrics_som")

danalyticsmetrics_soq = spark.sql(f"""         -- EK 2026715
                              select *
                              from `{env}-cdh`.derivative.danalyticsmetrics
                              WHERE BusinessDate=(CASE WHEN Portfolioname='ASTRAEA PORTFOLIO RETAIL' AND {run_date!r}<='2026-03-31' THEN '2026-03-31'
                              ELSE (SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate=(SELECT DISTINCT FirstDateOfQuarter FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r})) END ) 
                              """)

danalyticsmetrics_soq.createOrReplaceTempView("danalyticsmetrics_soq")

agreement = spark.sql(f"""
                      select * 
                      from `{env}-cdh`.primary.agreement
                      where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                      """)

agreement.createOrReplaceTempView("agreement")

restructuring = spark.sql(f"""
                          select * 
                          from `{env}-cdh`.primary.restructuring
                          where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                          """)

restructuring.createOrReplaceTempView("restructuring")

product = spark.sql(f"""
                    select * 
                    from `{env}-cdh`.primary.product
                    where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                    """)

product.createOrReplaceTempView("product")

exposure = spark.sql(f"""
                    SELECT *,
                      {run_date!r} AS RunDate
                      FROM `{env}-cdh`.primary.get_all_exposures(DATE({run_date!r}))
                      WHERE sourcesystem = 'profits'
                      AND {portfolio_filter}
                    """)

exposure.createOrReplaceTempView("exposure")

syndicatedfacility = spark.sql(f"""
                    select * 
                    from `{env}-cdh`.primary.syndicatedfacility
                    where BusinessDate = {run_date!r} and SourceSystem = 'profits'
                    """)

syndicatedfacility.createOrReplaceTempView("syndicatedfacility")

debastatusalgorithm = spark.sql(f"""
                    select ebastatus, exposurekey, cdi, NPEStatusDate,
                    case when IsBusinessDay = 1 then calendardate
                    else CurrentMonthLastWorkingDate end as businessdate
                    from `{env}-cdh`.derivative.debastatusalgorithm deba
                    left join `{env}-cdh`.reference.ref_calendar cal
                    on deba.ReportingDate = cal.calendardate
                    where ReportingDate =last_day({run_date!r})
                    """)

debastatusalgorithm.createOrReplaceTempView("debastatusalgorithm")

dlegalprocessstatusagreement = spark.sql(f"""
                    select * 
                    from `{env}-cdh`.derivative.dlegalprocessstatusagreement
                    where BusinessDate = {run_date!r} 
                    AND {portfolio_filter}
                    """)

dlegalprocessstatusagreement.createOrReplaceTempView("dlegalprocessstatusagreement")

dagreement = spark.sql(f"""
                                 select * 
                                 from `{env}-cdh`.derivative.dagreement
                                 where BusinessDate = {run_date!r}
                                 """)

dagreement.createOrReplaceTempView("dagreement")

#defines secured perimeter for monza
servicerreportsecuredperimeter = spark.sql(f"""
                                WITH latest_servicer_report_securedperimeter as (
                                  SELECT max(ReferenceDate) as MaxReferenceDate
                                  FROM `{env}-reportinputfiles`.investor.servicer_report_securedperimeter
                                  WHERE ReferenceDate <= {run_date!r}
                              )
                                      SELECT
                                          DebtorCDI,
                                          CASE 
                                              WHEN SecUnsec = 'Unsecured D' THEN FALSE
                                              WHEN SecUnsec = 'Secured D' THEN TRUE
                                          END AS IsSecuredFlag,
                                          ReferenceDate
                                      FROM `{env}-reportinputfiles`.investor.servicer_report_securedperimeter sec_per
                                      JOIN latest_servicer_report_securedperimeter lsr 
                                          ON sec_per.ReferenceDate = lsr.MaxReferenceDate
                                      WHERE PortfolioName ILIKE '%monza%'
""")

servicerreportsecuredperimeter.createOrReplaceTempView("servicerreportsecuredperimeter")

collection_mtd = spark.sql(f"""     -- EK 20260715
                       SELECT *
                       , CASE WHEN cal.IsBusinessDay = 0 then cal.PreviousBusinessDay else col.ActualDate end as ActualDate_calc 
                       FROM `{env}-cdh`.derivative.get_collection_data(DATE({run_date!r})) col
                       LEFT JOIN `{env}-cdh`.reference.ref_calendar cal ON col.actualdate = cal.CalendarDate 
                       WHERE YEAR(ActualDate) =  YEAR({run_date!r}) AND MONTH(ActualDate) =  MONTH({run_date!r}) and {portfolio_filter}
                                          """)
 
collection_mtd.createOrReplaceTempView("collection_mtd")

dexposure_mtd_active = spark.sql(f"""    -- EK 20260715
                      select * 
                      from `{env}-cdh`.derivative.dexposure
                      where BusinessDate <= {run_date!r}
                      AND BusinessDate >= (SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r})
                      AND IsActiveFlag = true
                      and {portfolio_filter}
                      """)

dexposure_mtd_active.createOrReplaceTempView("dexposure_mtd_active")

L3869ProductKeys = spark.sql(f"""   -- EK 20260715
SELECT distinct
      ProductCode
    , SubProductCode
    , ProductKey
    , ProductDescription
    , SubProduct
    , L3869Flag
    , CASE WHEN (ProductDescription ILIKE '%3869%' OR SubProduct ILIKE '%3869%' OR L3869Flag IS TRUE)
         AND NOT (ProductDescription ILIKE '%ΔΙΑΤΡΑΠΕΖΙΚΗ%' OR SubProduct ILIKE '%ΔΙΑΤΡΑΠΕΖΙΚΗ%' OR ProductDescription ILIKE '%C4.C5%' OR Subproduct ILIKE '%C4.C5%') THEN 1 ELSE 0 END AS L3869_Product_Ind  
    , CASE WHEN SubProductCode IN (3127, 3121, 3120) THEN 1 ELSE 0 END L3869_Suspended_Interest
FROM `{env}-cdh`.primary.product 
WHERE  BusinessDate <= {run_date!r}   
AND SourceSystem = 'profits'
AND (ProductDescription ILIKE '%3869%' OR Subproduct ILIKE '%3869%' OR L3869Flag IS TRUE)
AND CASE WHEN (ProductDescription ILIKE '%3869%' OR SubProduct ILIKE '%3869%' OR L3869Flag IS TRUE)
AND NOT(ProductDescription ILIKE '%ΔΙΑΤΡΑΠΕΖΙΚΗ%' OR SubProduct ILIKE '%ΔΙΑΤΡΑΠΕΖΙΚΗ%' OR ProductDescription ILIKE '%C4.C5%' OR Subproduct ILIKE '%C4.C5%' or SubProductCode IN (3127, 3121, 3120)) THEN 1 ELSE 0 END = 1
""")

L3869ProductKeys.createOrReplaceTempView('L3869ProductKeys')

# COMMAND ----------

# ##Loan

# COMMAND ----------

Haircut = spark.sql(f"""
select dex.agreementkey,
sum(dex.InitialAssignmentAmount) / NULLIF(sum(dex2.InitialAssignmentAmount), 0) as Haircut
from restructuring re
left join dexposure dex
on dex.exposurekey = re.WriteOffExposureKey
left join dexposure dex2
on dex2.exposurekey = re.NewExposureKey
where re.WriteOffExposureKey is not null
group by all
""")

Haircut.createOrReplaceTempView("Haircut")

BaloonDate = spark.sql(f"""
select dex.exposurekey,
max(case when BaloonAmount>0 then dp.InstallmentDate else null end) BaloonDate
from dexposure dex
left join paymentschedule dp
on dp.exposurekey = dex.exposurekey
group by all
""")

BaloonDate.createOrReplaceTempView("BaloonDate")

Bullet = spark.sql(f"""
select dex.exposurekey,
max(case when dex.IsBulletFlag = true then dp.InstallmentDate else null end) as BulletDate,
max(case when dex.IsBulletFlag = true then dp.InstallmentAmount else null end) as BulletAmount
from dexposure dex
left join paymentschedule dp
on dp.exposurekey = dex.exposurekey
group by dex.exposurekey
""")

Bullet.createOrReplaceTempView("Bullet")

arotron_prioritization = spark.sql(f"""
SELECT
        AgreementKey,
        OriginSourcesystem,
        OperationalLegalStatusLevel3, 
        LegalProcessStatusLastChangeDate,
        Portfolioname,
        DENSE_RANK() OVER (PARTITION BY AgreementKey ORDER BY CASE WHEN OriginSourcesystem = 'arotron' THEN 1 ELSE 2 END) AS rnk
    FROM
        dlegalprocessstatusagreement
""")

arotron_prioritization.createOrReplaceTempView("arotron_prioritization")

person_exposurekey = spark.sql(f"""
SELECT Exposurekey,
-- SUM(CASE WHEN IsRPCflag = true THEN 1 ELSE 0 END) AS RightPersonContacts,
SUM(CASE WHEN IsRPCflagCalc = true THEN 1 ELSE 0 END) AS RightPersonContacts, -- EK 20260309
SUM(CASE WHEN IsPositiveFlag = true THEN 1 ELSE 0 END) AS PositiveRightPersonContacts,
SUM(CASE WHEN IsPromiseFlag = true THEN 1 ELSE 0 END) AS NumberofPromises,
SUM(CASE WHEN ContactType in ('RIGHT PARTY CONTACT','Wrong Person Contact','No Contact','Right Party Contact', 'Right Person Contact') THEN 1 ELSE 0 END) AS NumberofContactActivities -- EK 20260309
FROM interaction
WHERE date_format(InteractionDate, 'yyyy-MM') = date_format(CAST('{run_date}' AS DATE), 'yyyy-MM')
GROUP BY Exposurekey 
""")

person_exposurekey.createOrReplaceTempView("person_exposurekey")

allocREV_View = spark.sql(f"""
select agreementkey,
sum(coalesce(dan.AllocREVBase,0)) as AgreementREV,
sum(coalesce(dan.cREVBase,0)) as AgreementCREV,
sum(coalesce(dan.cREVBaseTLC,0)) as AgreementCREVTLC
from danalyticsmetrics dan
group by agreementkey
""")

allocREV_View.createOrReplaceTempView('allocREV_View')

DebtorCREV_som = spark.sql(f"""    -- EK 20260715
select PrimaryPartyCDI,
sum(coalesce(dan.cREVBase,0)) as DebtorCREV
from danalyticsmetrics_som dan
group by PrimaryPartyCDI
""")

DebtorCREV_som.createOrReplaceTempView('DebtorCREV_som')

DebtorCREV_soq = spark.sql(f"""    -- EK 20260715
select PrimaryPartyCDI,
sum(coalesce(dan.cREVBase,0)) as DebtorCREV
from danalyticsmetrics_soq dan
group by PrimaryPartyCDI
""")

DebtorCREV_soq.createOrReplaceTempView('DebtorCREV_soq')

DebtorCollection = spark.sql(f"""    -- EK 20260715
select PrimaryOwnerCDI,
min(ActualDate_calc) as ActualDate 
from collection_mtd C
group by PrimaryOwnerCDI
""")

DebtorCollection.createOrReplaceTempView('DebtorCollection')

dexposure3869_mtd_active = spark.sql(f"""     --EK 20260715
                      select * 
                      from dexposure_mtd_active dex
                      inner join L3869ProductKeys l3869 on l3869.ProductKey = dex.ProductKey
                      where dex.exposurestatus <> 'Writen Off'
                      """)

dexposure3869_mtd_active.createOrReplaceTempView("dexposure3869_mtd_active")

DebtorPayerAtCollection_3869ProductKey = spark.sql(f"""    --EK 20260715
select e.cdi, CASE WHEN max(e.DaysPastDueFinal) > 30 THEN 'Non Payer' ELSE 'Payer' END as PayerAtCollection 
from dexposure3869_mtd_active e
inner join DebtorCollection as c on e.cdi =c.PrimaryOwnerCDI and e.BusinessDate = c.ActualDate
group by e.cdi
""")

DebtorPayerAtCollection_3869ProductKey.createOrReplaceTempView('DebtorPayerAtCollection_3869ProductKey')

DebtorPayerSoM_3869ProductKey = spark.sql(f"""   -- EK 20260715
select e.cdi,  
CASE WHEN max(e.DaysPastDueFinal) > 30 THEN 'Non Payer' ELSE 'Payer' END as PayerSoM 
from dexposure3869_mtd_active e
WHERE BusinessDate = (CASE WHEN Portfolioname='ASTRAEA PORTFOLIO RETAIL' AND {run_date!r}<='2026-03-31' THEN '2026-03-09'   --at migration
                      ELSE (SELECT DISTINCT PreviousMonthLastWorkingDate FROM `{env}-cdh`.reference.ref_calendar WHERE CalendarDate={run_date!r}) END ) 
group by e.cdi
""")

DebtorPayerSoM_3869ProductKey.createOrReplaceTempView('DebtorPayerSoM_3869ProductKey')


Loan_pre = spark.sql(f"""
SELECT distinct
cast(dex.RunDate as date) AS ReportingDate,
dex.PortfolioName,
dex.CDI,
(case when dex.PortfolioName like 'Hermes%' then dp.GroupCode
when dex.PortfolioName='Cetus' then  dp.GroupCodePortfolio end) as GroupID,
dex.AgreementKey,
dex.ExposureKey,
dex.Currency,
dex.AssetSegment,
(case when dex.PortfolioName ilike '%monza%' then srp.IsSecuredFlag
else dex2.IsSecuredFlag end) as IsSecuredFlag,
por.originBank AS BankOfOrigin, 
por.originBank AS ESMABankOfOrigin, 
dex.OpenDate,
agr.IssueDate,
dex.OutstandingTotalBalance,
dex.TotalPrincipal,
dex.TotalInterest, 
dex.TotalAccruals,
dex.TotalExpense,
CAST(dex.Commission + dex.DueCommission AS DECIMAL(19,2)) AS TotalCommissions,
dex.GBV,
dex.MaturityDate,
CASE  
WHEN dex.ExposureStatusCode IN ('3','4') THEN 0
WHEN dex.MaturityDate < {run_date!r} THEN 0
WHEN dex.MechanismCode = '6'  THEN 0 		         
ELSE coalesce(p.InterestFrequency,0) END AS RepaymentFrequency,
dex.InterestRateType,
dex.Baserate,
dex.SpreadRate,
dex.Coupon,
dex.N128Rate,
dex.PenaltyRate,
case when dex.DaysPastDueFinal<0 
then 0 
else dex.DaysPastDueFinal 
end AS DaysPastDueFinal,
deba.Ebastatus as EBAStatus,
case when ranked.PlatformName is not null then 'Y' else 'N' end AS PlatformNameFlag,
dag.NumberOfRestructurings as NoOfRestructures,
dag.NumberOfRestructuresESMA as NoOfRestructuresESMA,
dex.ActiveModificationProgramStatus,
dex.LastModificationActionType,
dex.LastModificationDate, 
dl.OperationalLegalStatusLevel3, 
dl.LegalProcessStatusLastChangeDate,
dex.DenouncedDate,
case 
when dex.DenouncementIndicator is true then "Y"
else "N" end DenouncementIndicator, 
dex.ExtrajudicialLetterEverFlag,
dex.ExtrajudicialLetterLastDate, 
case when dex.PaymentOrderFlag = true then 'Y' else 'N' end AS PaymentOrderFlag, 
dex.PaymentOrderDate,
dex.CodeofConductStage,
case
when dex.L3869Flag =1 then 'Y' 
else 'N' end L3869Flag, 
dex.L3869Status,
dex.L3869SubmissionDate,
dex.L3869ProvisionalCourtDecisionDate,
dex.L3869CourtDate,
ranked.PlatformName,
deba.NPEStatusDate as NPEDate,
dex.PrincipalAtDefault,
dex.InterestAtDefault,
dex.AmountDueAtDefault,
case 
    when deba.NPEStatusDate > por.AssignmentDate THEN NULL 
    else deba.NPEStatusDate 
end as ABNPEDate,
case 
    when deba.NPEStatusDate > por.AssignmentDate THEN NULL 
    else dex.OutstandingTotalBalance
end as ABNPEAmount,
per.RightPersonContacts,
per.PositiveRightPersonContacts,
per.NumberofPromises,
per.NumberofContactActivities,
round(dli.BaseLTV,2) AS BaseLTV, 
round(rev.AgreementCREV,2) AS AgreementCREV, 
round(rev.AgreementREV,2) AS AgreementREV, 
round(rev.AgreementCREVTLC,2) AS AgreementCREVTLC,
dex.IsActiveFlag,
dex.CourtRegion,
dex.DueAmount,
case when sy.Exposurekey is not null then 1 else 0 end as SyndicationFlag,
sy.PartyPercentage as SyndicationPercentage,
case when dex.portfolioname='EUCLID' and dex.SubServicer ='EUCLID DV01 - ASSIGN FOR SERVICING' then 'DV01'
when dex.portfolioname='EUCLID' and dex.SubServicer is null then 'Cepal'
else null end as SubServicerFlag,
dex.IsHighBalanceFlag as HighBalanceFlag,
dex.BaloonAmount,
bal.BaloonDate,
bul.BulletDate,
bul.BulletAmount,
cast(hair.Haircut as decimal(19,2)) AS Haircut,
dex.ProductKey, -- EK 20260715
dex.SubProductDescription,  -- EK 20260715
case when l3869.L3869_Product_Ind = 1  
    then dex.GBV else 0 end as L3869GBV, -- EK 20260715
CASE WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' and CREVsom.DebtorCREV > 10000 THEN true 
     WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' THEN false 
     ELSE NULL end AS SecuredDebtorSoM,  -- EK 20260715 
CASE WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' and CREVsoq.DebtorCREV > 10000 THEN true 
     WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' THEN false 
     ELSE NULL end AS SecuredDebtorSoQ, -- EK 20260715
CASE WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' and PC1.CDI IS NOT NULL THEN PC1.PayerAtCollection  
     WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' and PS1.CDI is not null THEN  PS1.PayerSoM  
     WHEN dex.PortfolioName = 'ASTRAEA PORTFOLIO RETAIL' then 'Non L3869 Product'
     else null
     end AS DebtorPayer  -- EK 20260715
FROM dexposure dex
LEFT JOIN (
  SELECT ExposureKey, PlatformName
  FROM (
    SELECT
      dex.ExposureKey,
      d.PlatformName,
      ROW_NUMBER() OVER (
          PARTITION BY dex.ExposureKey
          ORDER BY d.PlatformName IS NULL,
                  dex.businessdate DESC,   
                  d.PlatformName  
      ) AS rn
    FROM dexposure dex
    LEFT JOIN exposure exp
      ON dex.ExposureKey = exp.ExposureKey
    LEFT JOIN (
      SELECT
        CDI,
        PlatformName
      FROM (
        SELECT
          CDI,
          PlatformName,
          ROW_NUMBER() OVER (
            PARTITION BY CDI
            ORDER BY PlatformName IS NULL, PlatformName
          ) AS rn
        FROM dunderlaw
        WHERE  PlatformName IN ('Bridge2', 'Bridge1', 'L4605')
          AND InLawFlag = 1
      ) t
      WHERE rn = 1
    ) d
      ON exp.CDI = d.CDI       
  ) tmp
  WHERE rn = 1
) ranked
ON dex.ExposureKey = ranked.ExposureKey
LEFT JOIN dlienanalysisltv dli
ON dli.AgreementKey = dex.AgreementKey
LEFT JOIN agreement agr
ON agr.AgreementKey = dex.AgreementKey
LEFT JOIN allocREV_View rev
ON rev.Agreementkey = dex.AgreementKey
left join product p
on p.ProductKey = dex.ProductKey 
left join syndicatedfacility sy
on sy.Exposurekey = dex.ExposureKey
and sy.LenderPartyKey  ='429562'
left join dparty dp
on dp.CDI = dex.CDI 
LEFT JOIN person_exposurekey per
ON coalesce(dex.ExposureKeyQCAlpha,dex.Exposurekey) = per.ExposureKey
left join portfolio por
on LEFT(dex.exposurekeyprofits,3) = por.prefix
LEFT JOIN arotron_prioritization dl
ON dex.AgreementKey = dl.AgreementKey
and dex.Portfolioname = dl.Portfolioname
and rnk=1
left join debastatusalgorithm deba
on dex.exposurekey = deba.exposurekey
and dex.cdi = deba.cdi
left join dagreement dag
on dex.agreementkey = dag.agreementkey
left join BaloonDate bal 
on dex.exposurekey = bal.exposurekey
left join Bullet bul
on bul.exposurekey = dex.exposurekey
left join Haircut hair
on dex.agreementkey = hair.agreementkey
left join dexposure_full dex2 
on dex2.exposurekey=dex.exposurekey 
and dex2.cdi=dex.cdi 
and dex2.businessdate={run_date!r}
left join servicerreportsecuredperimeter srp
on dex.CDI = srp.DebtorCDI
left join DebtorCREV_som CREVsom    -- EK 20260715
on dex.CDI = CREVsom.PrimaryPartyCDI
left join DebtorCREV_soq CREVsoq    -- EK 20260715
on dex.CDI = CREVsoq.PrimaryPartyCDI
LEFT JOIN L3869ProductKeys l3869  -- EK 20260715
ON l3869.ProductKey = dex.ProductKey
LEFT JOIN DebtorPayerAtCollection_3869ProductKey AS PC1 -- EK 20260715
on dex.CDI = PC1.CDI
LEFT JOIN DebtorPayerSoM_3869ProductKey AS PS1 -- EK 20260715
on dex.CDI = PS1.CDI
""")

Loan_pre.createOrReplaceTempView("Loan_pre")

# COMMAND ----------

ActivePaymentAgreement = spark.sql(f"""
SELECT distinct
dp.GroupCodePortfolio,
dex.CDI,
dex.AgreementKey,
cast(dex.RunDate as date) AS ReportingDate,
dex.PortfolioName,
concat(res.NewAgreementKey,' | ',res.restructuringdate) AS PaymentAgreementID,
dex.ExposureKey,
dex.ActiveExposureRestructureStartDate,
dex.MonthsOfCleanPayHistory,
dex.GBV,
dex.OutstandingTotalBalance,
dex.TotalPaymentAgreementPaid,
dex.TotalPaymentAgreementDue,
case when ExposureRestructureType <> 'Conditional Woff' and ExposureRestructureStatus =  'Active'
then  ExposureRestructureOrigination END AS PaymentArrangementOrigination,
col.LastPaymentDate,
col.allocationamount AS Allocationamount
FROM dexposure dex
LEFT JOIN dparty dp
ON dex.CDI = dp.CDI
LEFT JOIN (
select Exposurekey, sum(amount) as allocationamount ,lastpaymentdate
from(
SELECT Exposurekey, Amount,
case when (ReportingDate>=DATE_FORMAT(date_trunc('MONTH',ReportingDate),'yyyy-MM-dd'))
and (ReportingDate<=DATE_FORMAT(last_day(ReportingDate),'yyy-MM-dd')) then max(ActualDate) end as LastPaymentDate,
ROW_NUMBER() OVER (PARTITION BY Exposurekey ORDER BY max(ActualDate) DESC) AS row_num
from `{env}-cdh-delivery`.`mega-datatape`.collections
group by Exposurekey, ReportingDate, Amount)
where row_num = 1
group by all) col
on col.Exposurekey = dex.ExposureKey
left join dparty_full dpf
on dpf.CDI = dex.CDI
left join restructuring res
on res.NewExposurekey = dex.ExposureKey
""")

ActivePaymentAgreement.createOrReplaceTempView("ActivePaymentAgreement")

RestructuringHistory = spark.sql(f"""
SELECT distinct 
res.RestructuringDate,
dex.PortfolioName,
cast(null as string) AS PaymentAgreementID,
dex.ExposureKey,
dex.ActiveExposureRestructureStartDate, 
dex.ExposureRestructureStatus,
dex.ExposureRestructureType,
prod.ProductDescription,
dex.ExposureRestructureOrigination,
cast (null as string) as Haircut,
dexfull.ConditionalWriteOffAmount,
dex.Tenor,
dex.InterestRate
FROM dexposure dex
LEFT JOIN restructuring res
ON res.NewExposureKey = dex.ExposureKey
LEFT JOIN  product prod
ON prod.ProductKey = dex.ProductKey --KS Change DATA2-421
LEFT JOIN dexposure_full dexfull
ON res.NewExposureKey = dexfull.ExposureKey
""")

RestructuringHistory.createOrReplaceTempView("RestructuringHistory")

# COMMAND ----------

#### Play addition into Loan

SplitBalance=spark.sql(f'''
                       SELECT * 
                       FROM `{env}-cdh`.primary.SplitBalance
                       WHERE BusinessDate = {run_date!r} and SourceSystem = "profits"
                       ''')
SplitBalance.createOrReplaceTempView("splitbalance")



exposure_adjustmentstepup=spark.sql(f'''
                    SELECT * 
                    FROM `{env}-cdh`.primary.exposureadjustmentstepup
                    WHERE BusinessDate = {run_date!r} and SourceSystem = "profits"

''')
exposure_adjustmentstepup.createOrReplaceTempView("exposure_adjustmentstepup")

exposure_adjustmentperiod = spark.sql(f'''
                    SELECT * 
                    FROM `{env}-cdh`.primary.ExposureAdjustmentPeriod
                    WHERE BusinessDate = {run_date!r} and SourceSystem = "profits"

''')
exposure_adjustmentperiod.createOrReplaceTempView("exposure_adjustmentperiod")



nextInterest=spark.sql(f'''
                       with nextInterest as (
        select 
        ex.exposurekey as exposurekey2,
        coalesce(stepup.fixinterest,'0') fixcode,
        coalesce(stepup.floatinterest,'0') floatcode,
        stepup.validdate,
        ex.currency,
        CASE WHEN FixCode <> '0'  THEN 'Future Fixed'  ELSE 'Future Variable' END AS INTEREST_TYPE,      
	    CASE WHEN FixCode <> '0'  THEN FixCode	ELSE floatCode END AS INTER_CODE,
        rank() over(partition by ex.exposurekey order by stepup.validdate ) rnk,
        stepup.*
        from dexposure ex
        inner join  exposure_adjustmentstepup stepup
        on ex.exposurekey=stepup.exposurekey  and ex.IsActiveFlag=true and ex.PrimaryPartyFlag=True
        and stepup.ValidDate>stepup.BusinessDate 
        and stepup.validdate < "2090-01-01"
        ) select nxtint.*,
                idt.DescriptionGR,
                idt.DescriptionEN
        from nextInterest nxtint 
        left join (select code, type, DescriptionGR,  descriptionEN, rank()over(partition By code order by type desc)  rnk  from `{env}-cdh`.reference.refratedescription  where type='interest') 
        idt on try_cast(idt.code as string) =nxtint.INTER_CODE and idt.rnk=1
        where (nxtint.fixcode <> '0' or nxtint.floatcode <> '0')
''')
nextInterest.createOrReplaceTempView("nextInterest")



ExposureInterestFutureSpread=spark.sql(f'''
                    SELECT * 
                    FROM `{env}-cdh`.primary.ExposureInterestFutureSpread
                    WHERE BusinessDate = {run_date!r} and SourceSystem = "profits"
                                       ''')
ExposureInterestFutureSpread.createOrReplaceTempView("exposureinterestfuturespread")

FutureSpread=spark.sql(f'''
                with futurespread as(
                   with t as (select ex.exposurekey as ExposureKeyFutureSpread ,ex.businessdate as businessdate1,(fs1.DebitSpreadRate + fs1.CreditSpread ) as FutureMargin,
            rank() over(partition by fs1.exposurekey order by fs1.FutureMarginApplyDate) rnk
            ,fs1.FutureMarginApplyDate
            ,ex.IsActiveFlag
            ,ex.Maturitydate
            ,ex.PrimaryPartyFlag
            from dexposure ex
            inner join exposureinterestfuturespread fs1
            on fs1.exposurekey=ex.exposurekey 
            )
            select * from t
            where  t.IsActiveFlag=true  and t.futuremarginapplydate < t.maturitydate and t.PrimaryPartyFlag=True)
            select * ,rank() over(partition by exposurekeyfuturespread order by rnk desc ) rrr
            from futurespread

            ''')

FutureSpread.createOrReplaceTempView("futurespread")


collateral=spark.sql(f'''select AgreementKey,
                                NonRealEstateKey
                FROM `{env}-cdh`.primary.collateral
                WHERE BusinessDate = {run_date!r} 
                 ''')
collateral.createOrReplaceTempView("collateral")


NonRECollateral=spark.sql(f'''
                          select distinct  ex.exposurekey
                                 from dexposure ex
                                 inner join collateral c 
                                 on ex.agreementkey=c.agreementkey 
                                 and c.nonrealestatekey is not null
                          ''')
NonRECollateral.createOrReplaceTempView("nonrecollateral")

interview=spark.sql(f'''
                    SELECT  CDI,
                            DeclaredIncome,
                            StatedIncome
                FROM `{env}-cdh`.primary.interview
                WHERE BusinessDate = {run_date!r} 
                    ''')
interview.createOrReplaceTempView("interview")

gracePeriod=spark.sql(f'''
                      select   ex.exposurekey
                        , ex.OpenDate
                        , ex.ExposureStatus  
                        , ex.Mechanism 
                        , ex.GracePeriodEndDate 
                        , datediff(MONTH,ex.OpenDate, ex.GracePeriodEndDate) GPinMonths
                        , ex.GracePeriodInterestPayableFlag
                        , ex.GracePeriodType
                        , ex.GracePeriodNextInterestDate
                        , ex.GracePeriodInterestFrequency
                        from dexposure ex
                        inner join product prd on prd.ProductKey=ex.Productkey
                        and ex.IsActiveFlag=true
                        and ex.OpenDate< ex.GracePeriodEndDate
                        and  ex.GracePeriodEndDate >ex.BusinessDate
                        and ex.MechanismCode="4"
                        and ex.ExposureStatusCode  in ('1','2')
                        and prd.ProductDescription not like '%ΑΙΡΕΣΗ%';
                      ''')

gracePeriod.createOrReplaceTempView("gracePeriod")

numOfDebtors=spark.sql(f'''
            select ex.exposurekey,count(distinct p.cdi) as NumberOfDebtors from exposure ex 
            inner join dparty p
            on p.cdi=ex.cdi 
            group by ex.exposurekey
''')
numOfDebtors.createOrReplaceTempView("numOfDebtors")

income=spark.sql(f'''
                 select p.cdi,
                        i.declaredincome,
                        i.statedincome
                        from dparty p
                        inner join (select distinct cdi,max(StatedIncome) as statedincome,max(DeclaredIncome) as declaredincome
                        from interview
                        where (DeclaredIncome is not null or statedincome is not null)
                        group by cdi) i on  i.cdi=p.cdi
                 ''')
income.createOrReplaceTempView("income")

borrowerBirth = spark.sql(f''' 
    SELECT distinct
        ex.exposurekey,
        p.CDI,
        YEAR(p.birthdate) AS birth_year,
        rank() over(partition by ex.exposurekey order by p.CDI  desc) AS rnk
    FROM 
       exposure ex
    INNER JOIN 
        dparty p
    ON 
        p.cdi = ex.cdi
''')
borrowerBirth.createOrReplaceTempView("borrowerBirth")

debtor=spark.sql(f'''
                 select ex.exposurekey,
                        p.nationality,
                        year(p.birthdate) as primarybirthdate,
                        p.deceasedflag,
                        p.occupation,
                        p.partytype,
                        p.partypostalcode
                from  dexposure ex 
                inner join (select distinct cdi,
                            sourcesystem,
                            nationality,
                            deceasedflag,
                            occupation,
                            partytype,
                            birthdate,
                            partypostalcode
                from dparty ) p 
                on ex.cdi=p.cdi 
''')
debtor.createOrReplaceTempView("debtor")



residency=spark.sql(f'''
               select distinct ex.exposurekey from dexposure ex
               inner join dparty p  
               on p.cdi=ex.cdi 
               where p.partypostalcode is not null                   

 ''')
residency.createOrReplaceTempView("residency")

perimeter3869=spark.sql(f'''
                  select distinct ex.exposurekey,
                  ex.CDI,
                  p.ProductDescription,
                  p.ProductKey,
                  ex.exposurestatus,
                  ex.mechanism,
                  ex.BaseRate,
                  ex.N128Rate,
                  ex.SpreadRate,
                  ex.businessdate as businessdate
                  from dexposure ex
                  inner join  product p 
                  on p.ProductKey = ex.ProductKey 
                  inner join  dunderlaw law
                  on law.CDI = ex.CDI and law.InLawFlag=1 and law.UnderLawType in ("N3869")
                  where ex.IsActiveFlag=true and ex.PrimaryPartyFlag=True  and ex.ExposureStatusCode <> "4"
                  and p.ProductKey in ('40567;4016' ,'40567;4015' ,'40568;4015' ,'40567;6079' ,'40567;6078' ,'40568;4016' ,'40517;7362' ,'40517;7363' ,
                  '40568;6079' ,'40568;6078' ,'40568;7363' ,'40324;4015' ,'40568;40000','40568;40001','713')
                          ''')

perimeter3869.createOrReplaceTempView("perimeter3869")
n3869interest=spark.sql(f'''
                          select distinct p.exposurekey,productdescription,productkey,exposurestatus,N128Rate,BaseRate,SpreadRate,
                          case when p.N128Rate + p.BaseRate + p.SpreadRate =0 then 1 else 0 end HasZeroInterestToday
                          from perimeter3869 p
                          left join nextInterest nextInt on nextInt.exposurekey2 = p.exposurekey and nextInt.rnk=1
                          left join FutureSpread fs on fs.ExposureKeyFutureSpread = p.exposurekey and fs.rnk=1 
                          and fs.FutureMarginApplyDate > p.businessdate and fs.FutureMargin > 0
                          where p.baserate + p.n128rate + p.spreadrate = 0

                          ''')
n3869interest.createOrReplaceTempView("n3869interest")

NextPaymentDue=spark.sql(f'''
                         with t as (
                        select exposurekey, InstallmentAmount, InstallmentDate, rank() over(partition by exposurekey order by InstallmentDate)   rnk
                        from  paymentschedule
                        where InstallmentDate>{run_date!r}
                        and InstallmentDate>dateadd(MONTH,12, {run_date!r})
                        and abs(InstallmentAmount)>0
            )  select exposurekey, InstallmentAmount, InstallmentDate from t
            ---group by exposurekey  
            ---where rnk=1
''')
NextPaymentDue.createOrReplaceTempView("nextpaymentdue")

# COMMAND ----------

arrearsAgo=spark.sql(f'''     
                with ddtt as (
with tt as (
                select  PreviousMonthLastWorkingDate eomdate, ROW_NUMBER()over(order by PreviousMonthLastWorkingDate desc) MonthAgo 
                from (
                select distinct PreviousMonthLastWorkingDate 
                from `{env}-cdh`.reference.ref_calendar
                where Calendardate>dateadd(MONTH,-2, {run_date!r})
                and Calendardate<{run_date!r}
                and PreviousMonthLastDate <>{run_date!r}
                ) as d
)select * from tt limit 2
)
                select dt.eomdate, a.exposurekey, a.DueAmount, 1 mago
                from `{env}-cdh`.derivative.dexposure a
                cross join ddtt  dt
                where  dt.MonthAgo=1 and a.businessdate=dt.eomdate and a.sourcesystem="profits"
                union 
                select dt.eomdate, a.exposurekey, a.DueAmount, 2 mago
                from `{env}-cdh`.derivative.dexposure a
                cross join ddtt  dt
                where  dt.MonthAgo=2 and a.businessdate=dt.eomdate and a.sourcesystem="profits"
''')
arrearsAgo.createOrReplaceTempView("arrearsAgo")

OCWLoan=spark.sql(f'''
                  with ocwloan as (
            select ex.exposurekey,
            u.underlawtype,
            u.applicationstatus,
            case when u.applicationstatus="99 - Ολοκλήρωση αίτησης" then u.CompletionReason else null end as NewOCWSubStatus,
            case when u.applicationstatus="99 - Ολοκλήρωση αίτησης" then u.ApplicationStatusDate else null end as NewOCWSubStatusDate,
            ex.businessdate
            from dexposure ex
            inner join dunderlaw u
            on u.CDI=ex.CDI  and u.Underlawtype ="N4738/2020" and lawsystem="Εξωδικαστικός μηχανισμός")
            select distinct * from ocwloan
                  ''')
OCWLoan.createOrReplaceTempView("OCWLoan")


## LoanInfo 
LoanInfo=spark.sql(f'''
                   select distinct ex.exposurekey,
                           case when ex.isactiveflag=true then "Yes" else "No" End OpenFlag,
                           ex.PurposeReason,
                           ex.InstalmentFrequency,
                           ex.ContractSigningDate as AgreementSigningDate,
                           tr.FieldValueENG as Purpose,
                           ex.ExposureStatusCode,
                           case 
								when ex.exposurestatuscode="1" then 'Normal' 
								 when ex.exposurestatuscode="2" then 'Overdue'
								 when exposurestatuscode="3" then 'Define Delay' 
								 when exposurestatuscode="4" then 'Writen Off' 
							END	AS AccountStatus,
                           ex.Overduedate,
                           ex.Mechanism,
                           ex.RepaymentMethod, 
                           ex.LastModificationDate,
                           ex.LastModificationActionType,
                          ex.HasSplitBalanceFlag as SplitLoanFlag,
                          splt.FirstPeriodAmount,    
                          cast(date_format(to_date(splt.FirstPeriodStartDate,'dd-MM-yyyy'),'yyyy-MM-dd') as date) as FirstPeriodStartDate,
                          cast(date_format(to_date(splt.FirstPeriodEndDate,'dd-MM-yyyy'),'yyyy-MM-dd') as date) as FirstPeriodEndDate,
                          splt.SecondPeriodAmount,
                          cast(date_format(to_date(splt.SecondPeriodStartDate,'dd-MM-yyyy'),'yyyy-MM-dd') as date) as SecondPeriodStartDate,
                          cast(date_format(to_date(splt.SecondPeriodEndDate,'dd-MM-yyyy'),'yyyy-MM-dd') as date) as SecondPeriodEndDate,
                          ex.OCWFlag,
                          ex.OCWApplicationStatus,
                          ocw.NewOCWSubStatusDate as OCWStatusDate,
                          ex.tenor as Tenor,
                          ex.L3869InterestFreePeriodEndDate,
                          f3869.HasZeroInterestToday as OnL3869InterestFreePeriod,
                          gp.GPInMonths as PrincipalGracePeriod,
                          fs1.FutureMargin as RevisionMargin1,
                          fs1.FutureMarginApplyDate as InterestRevisionDate1,
                          fs2.FutureMargin as RevisionMargin2,
                          fs2.FutureMarginApplyDate as InterestRevisionDate2,
                          fs3.FutureMargin as RevisionMargin3,
                          fs3.FutureMarginApplyDate as InterestRevisionDate3,
                          fsx.FutureMargin as FinalMargin,
                          fsx.FutureMarginApplyDate as FinalMarginStepDate,
                          nxtInt.DescriptionEN as RevisedInterestRateIndex,
                          coalesce(nxtIntCnt.cnt,0) as NumberOfFutureBaseRateChanges,
                          nxtInt.ValidDate as FutureBaseRateChangeDate,
                          -------nxtInt.ReferenceRate asFutureBaseRate
                          nxtIntFixed.DescriptionEN as FutureFixedSchedule,
                          nxtIntFixed.ValidDate as FutureFixedScheduleDate,
                          CASE WHEN ex.lastmodificationdate IS NOT NULL THEN "Yes" ELSE "No" END RestructuringArrangement,
                                CASE 
                                    WHEN ex.IsActiveFlag = false OR ex.ExposureStatusCode IN ('3', '4') OR ex.SlaveFlag = True THEN 'ND'
                                    WHEN ex.IsBulletFlag = True THEN 'Bullet'
                                    WHEN ex.MechanismCode IN ('4', '5') THEN 
                                        CASE 
                                            WHEN ex.InstalmentFrequency = '1' THEN 'Monthly'
                                            WHEN ex.InstalmentFrequency = '3' THEN 'Quarterly'
                                            WHEN ex.InstalmentFrequency = '6' THEN 'Semi annually'
                                            WHEN ex.InstalmentFrequency = '12' THEN 'Annual'
                                            ELSE 'Other'
                                        END
                                    ELSE 'Other'
                                END AS PaymentFrequency,
                                ex.businessdate as BusinessDate
                          ------nxtIntFixed.ReferenceRate as FutureFixedInterestRate
                          from dexposure ex
                          left join splitbalance splt on splt.exposurekey=ex.exposurekey
                          left join gracePeriod gp on gp.exposurekey=ex.exposurekey
                          LEFT JOIN OCWLoan ocw on ocw.exposurekey=ex.exposurekey
                          left join futurespread fs1 on fs1.exposurekeyfuturespread=ex.exposurekey and fs1.rnk=1
                          left join futurespread fs2 on fs2.exposurekeyfuturespread=ex.exposurekey and fs2.rnk=1
                          left join futurespread fs3 on fs3.exposurekeyfuturespread=ex.exposurekey and fs3.rnk=1
                          left join futurespread fsx on fsx.exposurekeyfuturespread=ex.exposurekey and fsx.rrr=1
                          left join n3869interest f3869 on f3869.exposurekey=ex.exposurekey
                          left join nextInterest nxtInt on nxtInt.exposurekey2=ex.exposurekey and nxtInt.rnk=1
                          left join `{env}-cdh`.reference.translationmapping tr on tr.fieldvalue=ex.purposereason
                          and tr.category="Purpose"
                          left join (select *, rank()over(partition by exposurekey2 order by rnk) rr from nextInterest where nextInterest.INTEREST_TYPE= 'Future Fixed') nxtIntFixed on nxtIntFixed.exposurekey2=ex.exposurekey and nxtIntFixed.rr=1 
                          left join (select exposurekey2, count(1) cnt from nextInterest group by exposurekey2) nxtIntCnt 
                          on nxtIntCnt.exposurekey2=ex.exposurekey
                          where ex.isactiveflag= true
                   ''')
LoanInfo.createOrReplaceTempView("LoanInfo")
 
 
LoanInfo=spark.sql(f'''
                   SELECT *,
                          CASE
                             WHEN PaymentFrequency="ND" then "ND"
                             ELSE cast(InstalmentFrequency as string)
                             END PaymentFrequencyTaxonomy
                    FROM LoanInfo
                   ''')
 
LoanInfo=LoanInfo.withColumnRenamed("exposurekey","exposurekey_LI")

# COMMAND ----------

## LoanRates

LoanRates=spark.sql(f'''
                    SELECT distinct
                    ex.exposurekey,
                    ex.HasStepUpInstallmentFlag,
                    coalesce(idt.DescriptionEN,ex.interestDescription) as CurrentInterestRateDescription,
                    idt.Taxonomy as CurrentInterestRateIndex,
                    coalesce(bdt.DescriptionEN, ex.BaseDescription) as CurrentBaseInterestRateDescription,
                    bdt.Taxonomy as CurrentBaseInterestIndex,
                    ex.InterestRateResetInterval as InterestRateResetInterval,
                    ex.InterestRateResetInterval as BaseRateResetFrequency,
                    ex.BaseRate as RefRate,
                    ex.N128Rate as Levy,
                    ex.PenaltyRate as PenaltyInterestRate,
                    ex.SpreadRate as CurrentInterestRateMargin,
                    ex.BaseRate as CurrentInterestRate,
                    --- LTV from danalyticmetrics????
                    case when ex.InterestRateType='Fixed' and ex.InterestDuration!=999 then 'Fixed Periodic' else ex.InterestRateType end InterestRateType,
                    case when ex.InterestRateType='Fixed' and ex.InterestDuration!=999 then '4' else case when ex.InterestRateType='Fixed' then '3' else '1' end end InterestRateTypeTaxonomy
                    FROM dexposure ex
                    LEFT JOIN nextInterest nxtInt 
                    ON nxtInt.exposurekey = ex.exposurekey and nxtInt.rnk=1
                    LEFT JOIN (select code, type, descriptionEN, taxonomy,   rank()over(partition By code order by type desc)  rnk  
                    from `{env}-cdh`.reference.refratedescription where type='interest' ) idt on idt.code =ex.interestCode and idt.rnk=1
                    LEFT JOIN (select code, type, descriptionEN, taxonomy, rank()over(partition By code order by type desc) rnk  
                    from `{env}-cdh`.reference.refratedescription where type='Base') bdt on  bdt.code =ex.BaseCode and bdt.rnk=1
                    where ex.isactiveflag=true
                    ''')
LoanRates=LoanRates.withColumnRenamed("exposurekey","exposurekey_LR")

## LoanAnalytic
LoanAnalytic=spark.sql(f'''
                   select distinct ex.exposurekey,
                          ex.CDI as CustomerId,         
                          ex.portfoliokey as PortfolioKey,        
                          ex.portfolioname as PortfolioName,
                          ex.mechanism as AccountType, 
                          ex.mechanism as PaymentType,
                          ex.Bucket as SourceBucket, 
                          ex.DateLastInArrears as DateLastInArrears, 
                          ex.DateLastCurrent as DateLastCurrent,     
                          b2.birth_year as SecondBorrowerBirthDate, 
                          b3.birth_year as ThirdBorrowerBirthDate, 
                          b4.birth_year as FourthBorrowerBirthDate, 
                          b5.birth_year as FifthBorrowerBirthDate, 
                          b2.CDI as SecondBorrowerID,
                          ex.DaysPastDue as DaysPastDue,                  
                          nd.NumberOfDebtors as NumberOfDebtors,           
                          inc.DeclaredIncome as PrimaryIncome,          
                          inc.StatedIncome as SecondaryIncome,          
                          deb.partytype as BorrowerType,
                          deb.primarybirthdate as BorrowerYearOfBirth,                   
                          deb.nationality as Nationality,                 
                          deb.deceasedflag as DeceasedFlag,                
                          deb.Occupation as EmploymentStatus, 
                          deb.PartyPostalCode as BorrowerPostCode,
                          case when r.exposurekey is not null then "Yes" else "No" end IsResident,
                          mago1.DueAmount as Arrears1MonthAgo,            
                          mago2.DueAmount as Arrears2MonthsAgo,           
                          nxt.Installmentamount as NextPaymentDue,
                          nxt.InstallmentDate as NextPaymentDueDate,
                          CASE WHEN nre.exposurekey IS NOT NULL THEN "Yes" ELSE "No" END AdditionalCollateralFlag,
                          CASE WHEN COALESCE(inc.declaredincome, 0) > 0 THEN ROUND(CAST(ex.outstandingtotalbalance AS DECIMAL(19,2)) / NULLIF(CAST(inc.declaredincome AS DECIMAL(19,2)), 0), 2) 
                          ELSE
                            NULL 
                          END DebtToIncome
                          from dexposure ex
                          left join borrowerBirth b2 on ex.exposurekey = b2.exposurekey and b2.rnk=2  
                          left join borrowerBirth b3 on ex.exposurekey = b3.exposurekey and b3.rnk=3  
                          left join borrowerBirth b4 on ex.exposurekey = b4.exposurekey and b4.rnk=4  
                          left join borrowerBirth b5 on ex.exposurekey = b5.exposurekey and b5.rnk=5  
                          left join numOfDebtors nd on nd.exposurekey=ex.exposurekey                  
                          left join income inc on inc.cdi=ex.cdi                                      
                          left join debtor deb on deb.exposurekey=ex.exposurekey       
                          left join residency r on r.exposurekey=ex.exposurekey               
                          left join nonrecollateral nre on nre.exposurekey=ex.exposurekey             
                          left join ArrearsAgo mago1 on mago1.exposurekey=ex.exposurekey and mago1.mago=1 
                          left join ArrearsAgo mago2 on mago2.exposurekey=ex.exposurekey  and mago2.mago=2 
                          left join (select exposurekey, sum(InstallmentAmount) as InstallmentAmount, max(installmentDate) as InstallmentDate from nextpaymentdue group by exposurekey) nxt on nxt.exposurekey=ex.exposurekey
                          where ex.isactiveflag=true
                   ''')


LoanAnalytic=LoanAnalytic.withColumnRenamed("exposurekey","exposurekey_LA")

### Joins
target_df=LoanInfo.join(
    LoanRates,
    (LoanRates["exposurekey_LR"] == LoanInfo["exposurekey_LI"]),
    "left",
).join(
    LoanAnalytic,
    LoanAnalytic["exposurekey_LA"] == LoanInfo["exposurekey_LI"],
    "left",
)

## Play dataframe

play_df=target_df.select(
    target_df.BusinessDate.alias("PoolCutOffDate"),
    LoanInfo.exposurekey_LI.alias("LoanIdentifier"),
    target_df.CustomerId.alias("CustomerID"), 
    target_df.BorrowerPostCode,
    target_df.IsResident,
    target_df.BorrowerType.alias("BorrowerType"),
    target_df.Nationality.alias("ForeignNational"),
    target_df.DeceasedFlag.alias("DeceasedFlag"),  
    target_df.BorrowerYearOfBirth,
    target_df.NumberOfDebtors.cast("int"),
    target_df.EmploymentStatus.alias("BorrowerEmploymentStatus"),
    target_df.OCWFlag,
    target_df.OCWApplicationStatus,
    target_df.OCWStatusDate,
    target_df.Purpose,
    target_df.PurposeReason.alias("PurposeGR"),
    target_df.PaymentFrequency,
    target_df.PaymentFrequencyTaxonomy,
    target_df.AccountStatus,
    target_df.ExposureStatusCode.alias("LoanStatusTaxonomy"),
    target_df.RevisionMargin1,
    target_df.InterestRevisionDate1,
    target_df.RevisionMargin2,
    target_df.InterestRevisionDate2,
    target_df.RevisionMargin3,
    target_df.InterestRevisionDate3,
    target_df.OnL3869InterestFreePeriod,
    target_df.L3869InterestFreePeriodEndDate.alias("L3869InterestFreePeriodEndDate"),
    target_df.RevisedInterestRateIndex,
    target_df.NumberOfFutureBaseRateChanges.cast("int"),
    target_df.FutureBaseRateChangeDate,
    target_df.SplitLoanFlag,            
    target_df.FirstPeriodAmount.alias("SplitLoan1Balance"),    
    target_df.FirstPeriodStartDate.alias("SplitLoan1StartDate"),
    target_df.FirstPeriodEndDate.alias("SplitLoan1EndDate"),
    target_df.SecondPeriodAmount.alias("SplitLoan2Balance"),
    target_df.SecondPeriodStartDate.alias("SplitLoan2StartDate"),
    target_df.SecondPeriodEndDate.alias("SplitLoan2EndDate"),
    target_df.PrincipalGracePeriod.cast("int"),
    target_df.FinalMargin,
    target_df.FinalMarginStepDate.alias("FinalStepDate"),
    target_df.Tenor.alias("LoanTerm"),
    target_df.FutureFixedSchedule,
    target_df.FutureFixedScheduleDate,
    target_df.RepaymentMethod, 
    target_df.SecondBorrowerBirthDate.alias("SecondBorrowerYearOfBirth"), 
    target_df.ThirdBorrowerBirthDate.alias("ThirdBorrowerYearOfBirth"), 
    target_df.FourthBorrowerBirthDate.alias("FourthBorrowerYearOfBirth"), 
    target_df.FifthBorrowerBirthDate.alias("FifthBorrowerYearOfBirth"),
    target_df.NextPaymentDue.alias("PaymentDue"),
    target_df.Overduedate.alias("OverdueDate"),
    target_df.AccountType,
    target_df.SourceBucket,
    target_df.Arrears1MonthAgo,            
    target_df.Arrears2MonthsAgo,
    target_df.DateLastCurrent,
    target_df.DateLastInArrears, 
    target_df.PaymentType,
    target_df.PrimaryIncome,
    target_df.SecondBorrowerID,
    target_df.SecondaryIncome, 
    target_df.RestructuringArrangement,
    target_df.LastModificationActionType,
    target_df.LastModificationDate,
    target_df.AdditionalCollateralFlag.alias("AdditionalCollateral"),
    target_df.SourceBucket.cast("int").alias("NumberMonthsinArrears"),
    target_df.HasStepUpInstallmentFlag.alias("StepupFlag"),
    target_df.CurrentInterestRateDescription,
    target_df.CurrentInterestRateIndex,
    target_df.CurrentBaseInterestRateDescription,
    target_df.CurrentBaseInterestIndex,
    target_df.BaseRateResetFrequency,
    target_df.InterestRateResetInterval,
    target_df.CurrentInterestRate, 
    target_df.CurrentInterestRateMargin, 
    target_df.InterestRateTypeTaxonomy,    
    target_df.AgreementSigningDate,                                                                                
    target_df.DebtToIncome.cast("decimal(19,2)"),
    target_df.PortfolioKey.alias("PortfolioID"),
    target_df.BusinessDate
)

play_df=play_df.distinct()

play_df.createOrReplaceTempView("play_df")

Loan = spark.sql(f"""SELECT 
                 ln.ReportingDate,
                 res.RestructuringDate,
                 ln.PortfolioName,	
                 ln.CDI,	
                 ln.GroupID,	
                 ln.AgreementKey,	
                 ln.ExposureKey,	
                 ln.Currency,	
                 ln.AssetSegment,	
                 ln.IsSecuredFlag,	
                 ln.BankOfOrigin,	
                 ln.ESMABankOfOrigin,	
                 ln.OpenDate,	
                 ln.IssueDate,	
                 ln.OutstandingTotalBalance,
                `{env}-cdh`.derivative.converttoeur(ln.Currency, ln.OutstandingTotalBalance, ln.ReportingDate)  AS OutstandingTotalBalanceEur,	
                 ln.TotalPrincipal,
                cast(`{env}-cdh`.derivative.converttoeur(ln.Currency, ln.TotalPrincipal, ln.ReportingDate) as DECIMAL(20,2)) AS TotalPrincipalEur,
                 ln.TotalInterest,
                cast(`{env}-cdh`.derivative.converttoeur(ln.Currency, ln.TotalInterest, ln.ReportingDate) as DECIMAL(24,2)) AS TotalInterestEur,	
                 ln.TotalAccruals,	
                cast(`{env}-cdh`.derivative.converttoeur(ln.Currency, ln.TotalAccruals, ln.ReportingDate) as DECIMAL(20,2)) AS TotalAccrualsEur,
                 ln.TotalExpense,
                 ln.TotalCommissions,	
                 ln.GBV,
                 cast(`{env}-cdh`.derivative.converttoeur(ln.Currency, ln.GBV, ln.ReportingDate) as double) AS GBVEur,
                 ln.MaturityDate,	
                 ln.RepaymentFrequency,	
                 ln.InterestRateType,	
                 ln.Baserate,	
                 ln.SpreadRate,	
                 ln.Coupon,	
                 ln.N128Rate,	
                 ln.PenaltyRate,	
                 ln.DaysPastDueFinal,	
                 ln.EBAStatus, 
                 ln.PlatformNameFlag,	
                 ln.NoOfRestructures, 
                 ln.NoOfRestructuresESMA, 
                 ln.ActiveModificationProgramStatus, 
                 ln.LastModificationActionType, 
                 ln.LastModificationDate, 
                 ln.OperationalLegalStatusLevel3, 
                 ln.LegalProcessStatusLastChangeDate,	
                 ln.DenouncedDate,	
                 ln.DenouncementIndicator,	
                 ln.ExtrajudicialLetterEverFlag,	
                 ln.ExtrajudicialLetterLastDate,	
                 ln.PaymentOrderFlag,	
                 ln.PaymentOrderDate,	
                 ln.CodeofConductStage,	
                 ln.L3869Flag,	
                 ln.L3869Status,	
                 ln.L3869SubmissionDate,	
                 ln.L3869ProvisionalCourtDecisionDate,	
                 ln.L3869CourtDate,	
                 ln.PlatformName,	
                 ln.NPEDate, 
                 ln.PrincipalAtDefault,
                `{env}-cdh`.derivative.converttoeur(ln.Currency, ln.PrincipalAtDefault, ln.ReportingDate) AS PrincipalAtDefaultEur,   
                 ln.InterestAtDefault, 
                 ln.AmountDueAtDefault, 
                 ln.ABNPEDate, 
                 ln.ABNPEAmount, 
                 ln.RightPersonContacts,	
                 ln.PositiveRightPersonContacts,	
                 ln.NumberofPromises,
                 ln.NumberofContactActivities,	
                 ln.BaseLTV,	
                 ln.AgreementCREV,
                 ln.AgreementCREVTLC,	
                 ln.AgreementREV,	
                 ln.IsActiveFlag,	
                 ln.CourtRegion,	
                 ln.DueAmount,	
                 ln.SyndicationFlag,	
                 ln.SyndicationPercentage,
                 ln.SubServicerFlag,
                 ln.HighBalanceFlag,
                 ln.BaloonAmount,
                 ln.BaloonDate,
                 ln.BulletDate,
                 ln.BulletAmount,
                 act.PaymentAgreementID, 
                 act.ActiveExposureRestructureStartDate,
                 act.MonthsOfCleanPayHistory, 
                 act.TotalPaymentAgreementPaid, 
                 act.TotalPaymentAgreementDue,
                 act.PaymentArrangementOrigination, 
                 act.LastPaymentDate, 
                 act.Allocationamount, 	
                 res.ExposureRestructureStatus,	
                 res.ExposureRestructureType,	
                 res.ProductDescription,	
                 res.ExposureRestructureOrigination,
                 ln.Haircut,	
                 res.ConditionalWriteOffAmount,	
                 res.Tenor,	
                 res.InterestRate,	
                 play_df.BorrowerPostCode,
                 play_df.IsResident,
                 play_df.BorrowerType,
                 play_df.ForeignNational,
                 play_df.DeceasedFlag,  
                 play_df.BorrowerYearOfBirth,
                 play_df.NumberOfDebtors,
                 play_df.BorrowerEmploymentStatus,
                 play_df.OCWFlag,
                 play_df.OCWApplicationStatus,
                 play_df.OCWStatusDate,
                 play_df.Purpose,
                 play_df.PurposeGR,
                 play_df.PaymentFrequency,
                 play_df.PaymentFrequencyTaxonomy,
                 play_df.AccountStatus,
                 play_df.LoanStatusTaxonomy,
                 play_df.RevisionMargin1,
                 play_df.InterestRevisionDate1,
                 play_df.RevisionMargin2,
                 play_df.InterestRevisionDate2,
                 play_df.RevisionMargin3,
                 play_df.InterestRevisionDate3,
                 play_df.OnL3869InterestFreePeriod,
                 play_df.L3869InterestFreePeriodEndDate,
                 play_df.RevisedInterestRateIndex,
                 play_df.NumberOfFutureBaseRateChanges,
                 play_df.FutureBaseRateChangeDate,
                 play_df.SplitLoanFlag,            
                 play_df.SplitLoan1Balance,    
                 play_df.SplitLoan1StartDate,
                 play_df.SplitLoan1EndDate,
                 play_df.SplitLoan2Balance,
                 play_df.SplitLoan2StartDate,
                 play_df.SplitLoan2EndDate,
                 play_df.PrincipalGracePeriod,
                 cast(`{env}-cdh`.derivative.converttoeur(ln.Currency, play_df.PrincipalGracePeriod, ln.ReportingDate) as int) AS PrincipalGracePeriodEur,
                 play_df.FinalMargin,
                 play_df.FinalStepDate,
                 play_df.FutureFixedSchedule,
                 play_df.FutureFixedScheduleDate,
                 play_df.RepaymentMethod, 
                 play_df.SecondBorrowerYearOfBirth, 
                 play_df.ThirdBorrowerYearOfBirth, 
                 play_df.FourthBorrowerYearOfBirth, 
                 play_df.FifthBorrowerYearOfBirth,
                 play_df.PaymentDue,
                 play_df.OverdueDate,
                 play_df.AccountType,
                 play_df.SourceBucket,
                 play_df.Arrears1MonthAgo,            
                 play_df.Arrears2MonthsAgo,
                 play_df.DateLastCurrent,
                 play_df.DateLastInArrears, 
                 play_df.PaymentType,
                 play_df.PrimaryIncome,
                 play_df.SecondBorrowerID,
                 play_df.SecondaryIncome, 
                 play_df.RestructuringArrangement,
                 play_df.AdditionalCollateral,
                 play_df.NumberMonthsinArrears,
                 play_df.StepupFlag,
                 play_df.CurrentInterestRateDescription,
                 play_df.CurrentInterestRateIndex,
                 play_df.CurrentBaseInterestRateDescription,
                 play_df.CurrentBaseInterestIndex,
                 play_df.BaseRateResetFrequency,
                 play_df.InterestRateResetInterval,
                 play_df.CurrentInterestRate, 
                 play_df.CurrentInterestRateMargin, 
                 play_df.InterestRateTypeTaxonomy,    
                 play_df.AgreementSigningDate,                                  
                 play_df.DebtToIncome,                 
                 ln.ProductKey, -- EK 20260715
                 ln.SubProductDescription,  -- EK 20260715
                 cast(ln.L3869GBV as double) L3869GBV, -- EK 20260715
                 cast(ln.SecuredDebtorSoM as boolean) SecuredDebtorSoM,  -- EK 20260715 
                 cast(ln.SecuredDebtorSoQ as boolean) SecuredDebtorSoQ,  -- EK 20260715
                 ln.DebtorPayer  -- EK 20260715 
                 FROM Loan_pre ln
                 LEFT JOIN ActivePaymentAgreement act
                 ON ln.Exposurekey = act.ExposureKey
                 LEFT JOIN RestructuringHistory res
                 ON ln.Exposurekey = res.Exposurekey
                 LEFT JOIN play_df 
                 ON play_df.LoanIdentifier = ln.Exposurekey
""")

# COMMAND ----------

target_table_path_Loan = f"{target_area}.Loan"

if spark.catalog.tableExists(target_table_path_Loan):
    delete_from_unity_table(
    spark,
    target_table_path_Loan,
    run_date,
    portfolio_filter,
)

Loan = Loan.withColumn("GroupID", col("GroupID").cast("string"))
Loan = Loan.withColumn("IngestionTimestamp", lit(from_utc_timestamp(current_timestamp(), "Europe/Athens")))

Loan.write \
        .format("delta") \
        .partitionBy("ReportingDate") \
        .mode("append") \
        .saveAsTable(target_table_path_Loan) # in order to write in delta_lake


spark.sql(f"REPAIR TABLE {target_table_path_Loan} SYNC METADATA")
