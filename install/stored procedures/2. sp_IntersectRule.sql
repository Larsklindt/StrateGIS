/****** StoredProcedure sp_IntersectRule Script Date: 19-06-2017 08:40:37 ******/
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
-- The score is determined from intersecting features spcified by '@IntersectionFeatureClass' parameter.
-- the insection features must contain the field named 'score' which states the feature_score to apply to the resulting layer
-- Finally the score is multiplied with the specified weight (in percent).
-- Please note, the feature calss is supposed to be in the table structure as generated,
-- when running the 'sp_CalculateLayer' procedure (ie. table is generated the StrateGIS database way).
CREATE PROCEDURE [dbo].[sp_IntersectRule]
	-- the weight of the rule
	@FeatureClass nvarchar(30) = 'FeatureClass',
	-- the weight of the rule
	@WeightPercentage real = 100,
	-- the weight of the rule
	@IntersectionFeatureClass nvarchar(30) = 'IntersectionFeatureClass'
AS
BEGIN
	DECLARE @WeightReal real = 0
	-- recalculate WeightPercentage into decimal multiplier
	SELECT @WeightReal = @WeightPercentage / 100

	-- check if field 'square_id exists
	IF EXISTS(SELECT *
				FROM   INFORMATION_SCHEMA.COLUMNS
				WHERE  TABLE_NAME = @IntersectionFeatureClass
				AND COLUMN_NAME = 'square_id') 
		BEGIN
			/** SPEEDY MODE */
			-- we are running with precompiled squares instead of true spatial intersections

			-- instead of perfomring traditional spatial intersection
			-- update output featureclass with score for intersecting
			-- items in only one update statement!!!
			DECLARE @update_speedy_statement nvarchar(max)
			DECLARE @update_speedy_statement_param_def nvarchar(max)


			SET @update_speedy_statement = 'UPDATE ' + @FeatureClass + ' SET ' + @FeatureClass + '.feature_score = ' + @FeatureClass + '.feature_score + t.s_fs * (' + @FeatureClass + '.item_percentage_overlap / 100) * @WeightRealIn
												FROM  
												(  
												SELECT ' + @IntersectionFeatureClass + '.square_id as s_id, SUM(' + @IntersectionFeatureClass + '.feature_score) as s_fs
												FROM ' + @IntersectionFeatureClass + '  
												GROUP BY  ' + @IntersectionFeatureClass + '.square_id  
												) t  
												WHERE ' + @FeatureClass + '.square_id = t.s_id'
			SET @update_speedy_statement_param_def = '@WeightRealIn float';
			-- execute the inline SQL statement, resulting sum is stored in @FeatureClass feature_score property
			EXECUTE sp_executesql @update_speedy_statement, @update_speedy_statement_param_def, @WeightRealIn = @WeightReal;

		END
		ELSE
			BEGIN
				/* SLOW MODE */
				-- But accurate mode, where database spatial intersection method is used

				-- drop previous temp table, if any
				IF OBJECT_ID('tempdb..#tmpRuleIntersect') IS NOT NULL
					DROP TABLE #tmpRuleIntersect

				-- create temp table for holding intersecting geometries from squarenet
				CREATE TABLE #tmpRuleIntersect(id int identity, ogr_fid int, geom geometry, score int);
				-- insert intersecting features
				DECLARE @update_temptable_statement nvarchar(max) 
				-- store data into temp table
				SET @update_temptable_statement = 'INSERT INTO #tmpRuleIntersect select ogr_fid, geom, score from ' + @IntersectionFeatureClass
				EXECUTE sp_executesql @update_temptable_statement

				-- Determine number of intersection square features
				-- create inline SQL statement
				DECLARE @max int = 0;
				SELECT @max = count(id) from #tmpRuleIntersect
				DECLARE @counter int = 1; 


				-- Loop through each feature and calculate field for the parameters:
				-- Intersection overlap area
				-- percentage of overlapping area
				-- percentage of overlapping area
				-- percentage of intersection in relation to the complete poppulation area
				WHILE @counter <= @max
					BEGIN
						-- prepare declarations, calculations
						-- and insertion in resulting table
						DECLARE @theIntersectionGeom geometry
						SELECT @theIntersectionGeom = geom from #tmpRuleIntersect where id=@counter;  

						DECLARE @theScore float = 0;
						SELECT @theScore = score from #tmpRuleIntersect where id=@counter;  
			
						-- update output featureclass with score for intersecting items
						DECLARE @update_statement nvarchar(max)
						DECLARE @update_statement_param_def nvarchar(max)
						SET @update_statement = 'UPDATE ' + @FeatureClass + '
												SET feature_score = feature_score + ((item_percentage_overlap / 100) * @theScoreIn * @WeightRealIn)
												WHERE geom_square.STIntersects(@theIntersectionGeomIn) <> 0';
						SET @update_statement_param_def = '@theScoreIn float, @theIntersectionGeomIn geometry, @WeightRealIn float';
						-- execute the inline SQL statement, resulting geometry is stored in @inputGeometry parameter
						EXECUTE sp_executesql @update_statement, @update_statement_param_def, @theScoreIn = @theScore, @theIntersectionGeomIn = @theIntersectionGeom, @WeightRealIn = @WeightReal;

						SET @counter = @counter + 1
					END
			END
	-- return '1' as successfully completion
	return 1
END
