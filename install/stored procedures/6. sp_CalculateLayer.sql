/****** StoredProcedure sp_CalculateLayer Script Date: 19-06-2017 08:40:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Lars Klindt, Geopartner A/S
-- Create date: 20170619
-- Description:	sp_CalculateLayer, StrateGIS project
-- =============================================
-- This stored procedure will calculate one single layer. 
-- All rules for each layer will be applied in order to update the property feature_score. If output table is not existing, this script will create it
-- (ie. the fields for amnaging geometry, feature_score etc. is automatically created).
-- Please note: depending on the complexity and number of rules this stored procedure may execute for some time, before it is complete.
-- Please note: If one layer table is assigned to more than one category, this tables calculated feature_score is flushed and may affect other categories
-- The property RecalculateLayers determines if the grid in each of the table is recalculated (may take some time)
CREATE PROCEDURE [dbo].[sp_CalculateLayer] 
	-- the category table name, in which to store the
	-- intersecting features and feature properties
	@OutputTableName nvarchar(30) = 'CategoryTableName',
	-- input layer table name
	@FeatureClassName nvarchar(30) = 'TableName',
	-- input layer unique foreign key name (eg. 'fid', 'FID')
	@FIdFieldName nvarchar(30) = 'FID',
	-- input layer unique foreign key name (eg. 'fid', 'FID')
	@GeomFieldName nvarchar(30) = 'geom',
	-- input layer unique foreign key name (eg. 'fid', 'FID')
	@RecalculateFeatures bit = false,
	-- Category Id. Makes a new Category Definition for that category
	@CategoryId int = 'CategoryForDefinition' 
AS

BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	-- create new table for uoutput of aggragated calculations
	-- if not existing
   DECLARE @SQLCreateString NVARCHAR(MAX)
   SET @SQLCreateString = 'IF NOT EXISTS (SELECT * FROM sys.tables WHERE name=''' + @OutputTableName + ''')
				CREATE TABLE ' + @OutputTableName + ' (
				[ID] [int] IDENTITY(1,1) NOT NULL,
				[square_id] [nvarchar](30) NOT NULL,
				[origin_layer] [nvarchar](30) NOT NULL,
				[origin_featureid] [int] NOT NULL,
				[area_overlap] [float] NULL,
				[feature_score] [float] NULL,
				[item_percentage_overlap] [float] NULL,
				[item_percentage_population] [float] NULL,
				[geom] [geometry] NULL,
				[complete_overlap_square] [float] NULL,
				[complete_percentage_overlap_square] [float] NULL,
				[complete_percentage_population] [float] NULL,			
					PRIMARY KEY CLUSTERED 
					(
						[ID] ASC
					)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
					) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]'
	EXEC (@SQLCreateString)

	-- database name for current database instance
	DECLARE @db_name nvarchar(30)
	select @db_name = db_name()

	-- get the schema name 
	DECLARE @schema_name nvarchar(30)
	select @schema_name = SCHEMA_NAME()

	-- spatial reference id for layers
	DECLARE @srid int = 0
	DECLARE @SQL_Get_Srid_String NVARCHAR(MAX)
	SET @SQL_Get_Srid_String = 'select distinct @sridOUT = geom.STSrid from ' + @FeatureClassName + ' where geom.STSrid is not null'
	DECLARE @sridParmDefinition nvarchar(500);
	SET @sridParmDefinition = '@sridOUT int OUTPUT';
	-- execute the inline SQL statement
	exec sp_executesql @SQL_Get_Srid_String, @sridParmDefinition, @sridOUT=@srid OUTPUT;

	-- spatial geometry typ for layers
	DECLARE @geomType nvarchar(30)
	DECLARE @SQL_Get_geomtype_String NVARCHAR(MAX)
	SET @SQL_Get_geomtype_String = 'select distinct @geomTypeOUT = geom.MakeValid().STGeometryType() from ' + @FeatureClassName + ' where geom.MakeValid().STGeometryType() is not null'
	DECLARE @geomParmDefinition nvarchar(500);
	SET @geomParmDefinition = '@geomTypeOUT nvarchar(30) OUTPUT';
	-- execute the inline SQL statement
	exec sp_executesql @SQL_Get_geomtype_String, @geomParmDefinition, @geomTypeOUT=@geomType OUTPUT;

	-- create refrence in geometry_columns table for input table
   DECLARE @SQLCreate_Input_RefInGeometryColumnsString NVARCHAR(MAX)
   SET @SQLCreate_Input_RefInGeometryColumnsString = 'IF (SELECT Count(*) FROM geometry_columns WHERE f_table_name=''' + @FeatureClassName + ''') = 0' + 
			'INSERT INTO geometry_columns (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, geometry_type)' + 
			'VALUES (''' + @db_name + ''', ''' + @schema_name + ''', ''' + @FeatureClassName + ''', ''geom'', 2, @sridIn, ''' + @geomType + ''')'
	DECLARE @create_Input_ref_ParmDefinition nvarchar(500);
	SET @create_Input_ref_ParmDefinition = '@sridIn int';
	-- execute the inline SQL statement
	exec sp_executesql @SQLCreate_Input_RefInGeometryColumnsString, @create_Input_ref_ParmDefinition, @sridIn=@srid;

	-- create refrence in geometry_columns table for output table
	-- note output table name is always of polygon feature type
   DECLARE @SQLCreate_Output_RefInGeometryColumnsString NVARCHAR(MAX)
   SET @SQLCreate_Output_RefInGeometryColumnsString = 'IF (SELECT Count(*) FROM geometry_columns WHERE f_table_name=''' + @OutputTableName + ''') = 0' + 
			'INSERT INTO geometry_columns (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, geometry_type)' + 
			'VALUES (''' + @db_name + ''', ''' + @schema_name + ''', ''' + @OutputTableName + ''', ''geom'', 2, @sridIn, ''POLYGON'')'
	DECLARE @create_Output_ref_ParmDefinition nvarchar(500);
	SET @create_Output_ref_ParmDefinition = '@sridIn int';
	-- execute the inline SQL statement
	exec sp_executesql @SQLCreate_Output_RefInGeometryColumnsString, @create_Output_ref_ParmDefinition, @sridIn=@srid;


	if @RecalculateFeatures = 1
	BEGIN
		-- clear table
		DECLARE @SQLDeleteCreateString NVARCHAR(MAX)
		SET @SQLDeleteCreateString = 'DELETE FROM ' + @OutputTableName + ' '
		EXEC (@SQLDeleteCreateString)

		-- determine complete area for all featureclass features
		DECLARE	@featureClassArea float = 0
		EXEC	@featureClassArea = [dbo].[sp_DetermineLayerCompleteArea]
				@TableName = @FeatureClassName,
				@GeometryFieldName = @GeomFieldName

		-- SPECIAL case: set minimum area to 1 m2 for point and line feature types
		if @featureClassArea < 1 AND (@geomType = 'Point' OR @geomType = 'LINESTRING')
		BEGIN
			SET @featureClassArea = 1
		END
		-- create feature id field, if not existing
		DECLARE @SQLCreate_IDString NVARCHAR(MAX)
		SET @SQLCreate_IDString = 'IF NOT EXISTS(SELECT *
				FROM   INFORMATION_SCHEMA.COLUMNS
				WHERE  TABLE_NAME = ''' + @FeatureClassName + '''
				AND COLUMN_NAME = ''' + @FIdFieldName + ''') 
			BEGIN
				  alter table ' +  @FeatureClassName + '
					add [fid] [int] IDENTITY(1,1) NOT NULL
					CONSTRAINT [PK_' + @FeatureClassName + '] PRIMARY KEY CLUSTERED 
					(
						[fid] ASC
					) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
			END'
		EXEC (@SQLCreate_IDString)
		


		-- drop previous temp table, if any
		IF OBJECT_ID('tempdb..#tmpFeatureclass') IS NOT NULL
			DROP TABLE #tmpFeatureclass

		-- create temp table for holding all features ID's to traverse through table
		CREATE TABLE #tmpFeatureclass(id int identity, fid int);
		-- insert intersecting features
		DECLARE @SQLFill_tmpFeatureclass nvarchar(max) = 'INSERT INTO #tmpFeatureclass select ' + @FIdFieldName + ' from ' + @FeatureClassName + ' order by ' + @FIdFieldName + ' asc'
		DECLARE @ParmDefinition nvarchar(500);
		-- execute the inline SQL statement, result is stored in tem table
		exec sp_executesql @SQLFill_tmpFeatureclass;

		declare @max int
		SELECT @max = count(id) from #tmpFeatureclass
		DECLARE @counter int = 1; 

		-- todo: clean up prevous calculated features in category table

		-- loop through all features


		-- Loop through each feature and calculate field for the parameters:
		-- Intersection overlap area
		-- percentage of overlapping area
		-- percentage of overlapping area
		-- percentage of intersection in relation to the complete poppulation area
		WHILE @counter <= @max
			BEGIN
				-- prepare declarations, calculations
				-- and insertion in resulting table
				DECLARE @featureId int
				SELECT @featureId = fid FROM #tmpFeatureclass WHERE id=@counter

				-- insert properties into Categories table
				DECLARE @return_value int = -1
				EXEC @return_value = [dbo].[sp_Determineintersections]
					@OutputTableName = @OutputTableName,
					@TableName =@FeatureClassName,
					@FeatureIdFieldName = @FIdFieldName,
					@FeatureIdFieldValue = @featureId,
					@FeatureLayerCompleteArea = @featureClassArea

				SET @counter = @counter + 1
			END
	END
		IF EXISTS(SELECT *
			FROM   INFORMATION_SCHEMA.COLUMNS
			WHERE  TABLE_NAME = @FeatureClassName
			AND COLUMN_NAME = 'score') 
			BEGIN
				-- fill in field 'feature_score'
				-- only used if we are creating 'speedy layer'
				DECLARE @SQLCopyFeatureScore  nvarchar(max)
				SET @SQLCopyFeatureScore = 'UPDATE ' + @OutputTableName + '
											SET ' + @OutputTableName + '.feature_score = ' + @FeatureClassName + '.score
											FROM ' + @OutputTableName + ', ' + @FeatureClassName + '
											where ' + @OutputTableName + '.origin_featureid = ' + @FeatureClassName + '.' + @FIdFieldName
				EXEC (@SQLCopyFeatureScore)
			END
		ELSE 
			BEGIN

				-- fill in fields 'complete_percentage_overlap'
				-- and 'complete_percentage_population' in the 
				-- specified table @OutputTableName
				-- create temp table for holding all features ID's to traverse through table
				-- drop previous temp table, if any
				WITH CTE AS
				(
					SELECT square_id, ISNULL(SUM(area_overlap), 0) as f_area, ISNULL(SUM(feature_score), 0) as f_score
					FROM Skovrejs_Svb_analysis
					GROUP BY square_id HAVING COUNT(ID) > 1
				)
				UPDATE Skovrejs_Svb_analysis
					SET complete_overlap_square = f_area,
					complete_percentage_overlap_square = (f_area / 10000) * 100,
					complete_percentage_population = (f_area / @featureClassArea) * 100
				FROM Skovrejs_Svb_analysis P
				INNER JOIN CTE S ON S.square_id = P.square_id

				-- apply rules to layer
				-- ! first delete previous score, if any. !
				DECLARE @SQL_reset_featurescore nvarchar(250)
				SELECT @SQL_reset_featurescore = 'UPDATE ' + @OutputTableName + ' SET feature_score = 0'
				EXECUTE sp_executesql @SQL_reset_featurescore 

				-- fill in rules into temp table
				-- drop previous temp table, if any
				IF OBJECT_ID('tempdb..#tmpRules') IS NOT NULL
					DROP TABLE #tmpRules

				CREATE TABLE #tmpRules(id int identity, rules_id int, rule_name nvarchar(30));
				-- insert rules identifiers
				INSERT INTO #tmpRules
					Select category_definition_rules.id, rule_name
					FROM category_definition_rules
					INNER JOIN category_definition ON category_definition_rules.category_definition_id = category_definition.id
					INNER JOIN rules ON rules.id = category_definition_rules.rules_id
					WHERE category_definition.layer_name = @FeatureClassName


				DECLARE @RuleCounter int = 1
				DECLARE @RulesCount int
				SELECT @RulesCount = COUNT(ID) FROM #tmpRules

				-- Loop through all rules for each layer
				WHILE @RuleCounter <= @RulesCount
					BEGIN
						-- find rule name
						DECLARE @layer_rule nvarchar(30)
						SELECT @layer_rule = rule_name FROM #tmpRules WHERE id = @RuleCounter

						-- find rule identifier
						DECLARE @category_definition_rules_identifier nvarchar(30)
						SELECT @category_definition_rules_identifier = rules_id FROM #tmpRules WHERE id = @RuleCounter

						DECLARE @layer_rule_argument_weight real = 0
						-- find ruleargument, WEIGHT
						SELECT @layer_rule_argument_weight = rule_argument1 FROM category_definition_rules INNER JOIN category_definition ON category_definition_rules.category_definition_id = category_definition.id WHERE category_definition_rules.id = @category_definition_rules_identifier

						DECLARE @layer_rule_argument_IntersectionFeatureClass nvarchar(30)
						-- find ruleargument, WEIGHT
						SELECT @layer_rule_argument_IntersectionFeatureClass = rule_argument2 FROM category_definition_rules INNER JOIN category_definition ON category_definition_rules.category_definition_id = category_definition.id WHERE category_definition_rules.id = @category_definition_rules_identifier

						-- apply rules in order to place a specific score for each feature
						If @layer_rule  = 'sp_AreaRule' 
							BEGIN
								-- run stored procedure, area rule with arguments
								EXEC sp_AreaRule @FeatureClass = @OutputTableName, @WeightPercentage = @layer_rule_argument_weight
							END
						Else If @layer_rule  = 'sp_IntersectRule' 
							BEGIN
								-- run stored procedure, area rule with arguments
								EXEC sp_IntersectRule @FeatureClass = @OutputTableName, @WeightPercentage = @layer_rule_argument_weight, @IntersectionFeatureClass = @layer_rule_argument_IntersectionFeatureClass
							END

						SET @RuleCounter = @RuleCounter + 1
					END
			END

	BEGIN
		INSERT INTO [dbo].[category_definition]
           ([category_id]
           ,[layer_name]
           ,[output_layer_name])
		VALUES
           (@CategoryId
           ,@FeatureClassName
           ,@OutputTableName)

	END
	-- return '1' as successfully completion
	return 1
END
