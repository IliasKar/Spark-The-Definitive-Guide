USE [Analytic]
GO
/****** Object:  StoredProcedure [bi].[sp_settlement]    Script Date: 23/9/2026 1:36:09 μμ ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Stamatopoulos Alexandros>
-- Create date: <02/07/2021>
-- Description:	<Customer>
 --Modified by: DNA-6534 Ioannis Triantafyllakis 05/05/23 Get only primary settlement per Loan
 --Modified by: DNA-5916 Theoni Boura 11/05/23 Add ParticipationPercentage
-- =============================================
ALTER PROCEDURE [bi].[sp_settlement]
AS
BEGIN
	DECLARE @SessionId INTEGER;
	DECLARE @UnitName NVARCHAR(64) = 'bi.sp_settlement';
	EXECUTE DWH_LOG.prc.LogStart @UnitName, @SessionId OUTPUT
	BEGIN TRY
		EXECUTE DWH_LOG.prc.LogMessage 'Inserting into table bi.Settlement', 0, @SessionId

	declare @dt date = (select top 1 CalendarDate
						from [DWH].ref.Calendar WITH (READUNCOMMITTED)
						where CalendarDate >= convert(date,getdate()) and   IsBusinessDay =1
						)
	drop table if exists #tmp
	SELECT  distinct
		payments.AssetId
		,PortfolioId
		,InstallmentAmount
		,InstallmentNumber
		,InstallmentDate
		,row_number() over (partition by assetid order by installmentdate asc) cnt
	into #tmp
	FROM [DWH].trn.PaymentSchedule payments WITH (READUNCOMMITTED)
	WHERE  payments.CurrentRecord = 1 and InstallmentDate>=@dt
	--PK 23/12/2021
	drop table if exists #assetrelationships
	select distinct a.RelatedAssetId, a.AssetId,a.CurrentRecord,
	ar.ParticipationPercentage --TBOURA 17/05/2023--addparticipationpercentage
	into #assetrelationships
	from [DWH].ast.AssetRelationships ar WITH (READUNCOMMITTED)
	inner join
	(select ar.RelatedAssetId, ar.AssetId, max(cast(ar.CurrentRecord as int)) CurrentRecord
	        , max(ar.enddate) as EndDate
				from [DWH].ast.AssetRelationships ar WITH (READUNCOMMITTED)
				inner join [DWH].cls.Classification assetreltype on assetreltype.ClassificationId=ar.AssetRelationshipTypeId and assetreltype.Description='Settlement - Loan Rltnp'
				left join dwh.cls.Classification primaryset on primaryset.ClassificationId=AssetRelationshipSubTypeId and (primaryset.Description IN ('Primary', 'Ν/Α')) -- Giannis Triantafyllakis (dna-6534) 05/05/23
				group by ar.RelatedAssetId, ar.AssetId) as a
	on  ar.AssetId = a.AssetId and ar.RelatedAssetId = a.RelatedAssetId and ar.CurrentRecord=a.CurrentRecord and ar.EndDate=a.EndDate

	IF OBJECT_ID(N'tempdb..#settlements') IS NOT NULL
	BEGIN
	DROP TABLE #settlements
	END
	SELECT distinct a.AssetKey1 as AccountNumber
		  ,p.PortfolioName
		  ,SettlementType.Description [SettlementType]
		  ,SettlementStatus.Description SettlementStatus
		  ,sett.ImplementationDate
		  ,sett.CancellationDate
		  ,sett.RejectionDate
		  ,OverdueDate
		  ,sett.NumberOfInstallments
		  ,sett.NumberOfAccounts
		  ,AgreementMetrics.NumberOfRestructurings
		  ,PoolName
		  ,DueAmount
		  ,ass.AssetAmount [SettlementAmount]
		  ,s.InstallmentAmount
		  ,s.InstallmentDate
		  ,s.InstallmentNumber
		  ,KeptPercentage
		  ,SettlementBucket
		  --,cast(AssetStatus.Description as int) as [isActive]
		  , AssetStatus.Value as [isActive]
		  ,(CASE WHEN (ar.currentRecord = 1) THEN 1 ELSE 0 END) IsLatest
		   ,sett.MigratedFlag
		   ,ass.OpenDate AS ApprovalDate
		   ,ass.OpenDate AS SubmissionDate
		  ,[ass].AssetKey SettlementCode
		  ,p.PortfolioId
		  ,sett.SettlementId
		  ,a.AssetId as [LoanId]
		  ,SourceExtraction.ReferenceDate
		  ,CASE WHEN l.HasActiveSettlementFlag = 1 THEN 1 ELSE 0 END ConnectedToLoan
		  ,CAST(GETDATE() as DATE) ETLLoadingDate
		  ,ar.ParticipationPercentage  --DNA-5916 TB 11/05/23 Add ParticipationPercentage
	into #settlements
	FROM [DWH].[ast].[settlement] sett WITH (READUNCOMMITTED)
		inner join  DWH.cls.Classification SettlementType WITH (READUNCOMMITTED) on SettlementType.ClassificationId=sett.SettlementTypeId and SettlementType.Description<>N'Νόμος 3869 – Προσωρινή Διαταγή'
		left join  #assetrelationships as ar on ar.RelatedAssetId = sett.SettlementId
		left join  [DWH].adm.SGKAsset a WITH (READUNCOMMITTED) on a.AssetId = ar.AssetId
		left join  [DWH].ast.Asset Loan WITH (READUNCOMMITTED) on Loan.AssetId=a.AssetId and Loan.CurrentRecord=1
		left join  [DWH].ast.Loan l     WITH (READUNCOMMITTED) on l.LoanId=Loan.AssetId and l.CurrentRecord = 1
		left join  [DWH].ref.Portfolio p WITH (READUNCOMMITTED) on ISNULL(Loan.PortfolioId, 10) = p.PortfolioId --and p.CurrentRecord = 1
		left join  [DWH].ast.AgreementMetrics ON AgreementMetrics.AgreementId = Loan.ParentAssetId AND AgreementMetrics.CurrentRecord = 1
		left join  [DWH].ast.asset [ass] WITH (READUNCOMMITTED) on sett.SettlementId = ass.AssetId and ass.CurrentRecord = 1
		left join  [DWH].adm.Sourceextraction WITH (READUNCOMMITTED) on SourceExtraction.SourceSystem = 'Qualco'
		--left join  (select app.LoanID, applicationApprovaldate, ApplicationDate
		--			from [DWH].act.Application app WITH (READUNCOMMITTED)
		--			inner join  DWH.cls.Classification clsap WITH (READUNCOMMITTED) on  clsap.ClassificationId = app.ApplicationTypeId and clsap.Description = 'SettlementFLDT'
		--			where app.currentrecord=1)
		--		as app on  app.LoanId = Loan.AssetId --PK 23/12 there is no settlement-application relation yet
		left join  #tmp s WITH (READUNCOMMITTED) on s.assetid = sett.settlementid and s.cnt=1
		left join  DWH.cls.Classification SettlementStatus WITH (READUNCOMMITTED) on SettlementStatus.ClassificationId=sett.SettlementStatusId
		left join  DWH.cls.Classification AssetStatus WITH (READUNCOMMITTED) on AssetStatus.ClassificationId=ass.AssetStatusId
	WHERE sett.CurrentRecord =1;

	truncate table staging.settlement;
INSERT INTO staging.settlement
(      [AccountNumber]
      ,[PortfolioName]
      ,[SettlementType]
      ,[SettlementStatus]
      ,[ImplementationDate]
      ,[CancellationDate]
      ,[RejectionDate]
      ,[OverdueDate]
      ,[NumberOfInstallments]
      ,[NumberOfAccounts]
      ,[NumberOfRestructurings]
      ,[PoolName]
      ,[DueAmount]
      ,[SettlementAmount]
      ,[InstallmentAmount]
      ,[InstallmentDate]
      ,[InstallmentNumber]
      ,[KeptPercentage]
      ,[SettlementBucket]
      ,[isActive]
      ,[IsLatest]
      ,[MigratedFlag]
      ,[ApprovalDate]
      ,[SubmissionDate]
      ,[SettlementCode]
      ,[PortfolioId]
      ,[SettlementId]
      ,[LoanId]
	  ,[ReferenceDate]
	  ,[ConnectedToLoan]
	  ,[ETLLoadingDate]
	  ,[ParticipationPercentage]--DNA-5916 TB 11/05/23 Add ParticipationPercentage
)

SELECT *
FROM #settlements
		EXECUTE DWH_LOG.prc.LogMessage 'Inserted successfully into table bi.settlement', @@ROWCOUNT, @SessionId
	END TRY BEGIN CATCH
		DECLARE @ErrorMessage	NVARCHAR(4000)	= ERROR_MESSAGE()
		DECLARE @ErrorNumber	INTEGER			= ERROR_NUMBER()
		EXECUTE DWH_LOG.prc.LogError @ErrorMessage = @ErrorMessage, @ErrorNumber = @ErrorNumber, @SessionId = @SessionId;
		throw;
	END CATCH;
	-- Finalize Session
	EXECUTE DWH_LOG.prc.LogStop @SessionId
END;
