/****** StoredProcedure sp_AreaRule Script Date: 19-06-2017 08:40:37 ******/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Lars Klindt, Geopartner A/S
-- Create date: 20170619
-- Description:	sp_AreaRule, StrateGIS project
-- =============================================
-- This rule will assign a score for each element in the specified featureclass. 
-- The score is determined from the relative interesection of each grid cell multiplied with
-- the specified weight (in percent).
-- Please note, the feature calss is supposed to be in the table structure as generated,
-- when running the 'sp_CalculateLayer' procedure (ie. table is generated the StrateGIS database way).
CREATE PROCEDURE [dbo].[sp_AreaRule]
	-- the weight of the rule
	@FeatureClass nvarchar(30) = 'FeatureClass',
	-- the weight of the rule
	@WeightPercentage real = 100
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	DECLARE @WeightReal real = 0	
	-- recalculate WeightPercentage into decimal multiplier
	SELECT @WeightReal = @WeightPercentage / 100

	DECLARE @theRuleSQL nvarchar(max) = 'UPDATE ' + @FeatureClass + ' SET feature_score=feature_score + (area_overlap * @WeightRealIn)'
	DECLARE @theRuleSQLParmDefinition nvarchar(500) = '@WeightRealIn float';
	-- execute the inline SQL statement, result is stored in tem table
	exec sp_executesql @theRuleSQL, @theRuleSQLParmDefinition, @WeightRealIn = @WeightReal;

END
