/****** StoredProcedure sp_Determineintersections Script Date: 19-06-2017 08:40:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Lars Klindt, Geopartner A/S
-- Create date: 20170619
-- Description:	sp_Determineintersections, StrateGIS project
-- =============================================
-- Calculates degree of interception between two geometry features. the unique input feature is specifyed by @TableName,
-- @FeatureIdFieldName and @FeatureIdFieldValue. Intersecting features are found from the grid,
-- in order to calculate degree of intersection in percentage and ratio according to complete input
-- population, specified as a ratio between the current intersection area and the input tables complete area from all geometries
CREATE PROCEDURE [dbo].[sp_Determineintersections] 
	-- the category table name, in which to store the
	-- intersecting features and feature properties
	@OutputTableName nvarchar(30) = 'CategoryTableName',
	-- input layer table name
	@TableName nvarchar(30) = 'TableName',
	-- input layer unique foreign key name (eg. 'ogr_fid', 'FID')
	@FeatureIdFieldName nvarchar(30) = 'FeatureIdFieldName',
	-- input layer unique foreign key value
	@FeatureIdFieldValue int = -1,
	-- the input tables complete area from all geometries, 
	-- which can be calculated from sp_DetermineLayerCompleteArea
	@FeatureLayerCompleteArea Float = -1
AS

BEGIN
	-- at first: Find the geometry from specified featureid
	-- create inline SQL statement
	DECLARE @inputGeometry geometry
	DECLARE @findgeom_statement nvarchar(max)
	DECLARE @findgeom_param_def nvarchar(max)
	SET @findgeom_statement = 'SELECT @inputGeometryOut = geom
						  FROM ' + @TableName + ' 
						  WHERE ' + @FeatureIdFieldName + '=@FeatureIdFieldValueIn';
	SET @findgeom_param_def = '@FeatureIdFieldValueIn int, @inputGeometryOut geometry OUTPUT';
	-- execute the inline SQL statement, resulting geometry is stored in @inputGeometry parameter
	EXECUTE sp_executesql @findgeom_statement, @findgeom_param_def, @FeatureIdFieldValueIn = @FeatureIdFieldValue, @inputGeometryOut = @inputGeometry OUTPUT;

	-- spatial geometry type for layer
	DECLARE @geomType nvarchar(30)
	DECLARE @SQL_Get_geomtype_String NVARCHAR(MAX)
	SET @SQL_Get_geomtype_String = 'select distinct @geomTypeOUT = geom.MakeValid().STGeometryType() from ' + @TableName + ' where geom.MakeValid().STGeometryType() is not null'
	DECLARE @geomParmDefinition nvarchar(500);
	SET @geomParmDefinition = '@geomTypeOUT nvarchar(30) OUTPUT';
	-- execute the inline SQL statement
	exec sp_executesql @SQL_Get_geomtype_String, @geomParmDefinition, @geomTypeOUT=@geomType OUTPUT;

	-- make geometry valid
	if @inputGeometry.STIsValid() < 1
		SET @inputGeometry = @inputGeometry.MakeValid()

	-- drop previous temp table, if any
	IF OBJECT_ID('tempdb..#tmpSquarenetIntersect') IS NOT NULL
		DROP TABLE #tmpSquarenetIntersect

	-- create temp table for holding intersecting geometries from squarenet
	CREATE TABLE #tmpSquarenetIntersect(id int identity, geom geometry, squareId varchar(30));
	-- insert intersecting features
	INSERT INTO #tmpSquarenetIntersect select geom, square_id from square_grid WITH (INDEX(square_grid_spatial_index)) WHERE geom.STIntersects(@inputGeometry) = 1
	
	-- Determine number of intersection square features
	-- create inline SQL statement
	DECLARE @max int = 0; 
	DECLARE @counter int = 1; 
	DECLARE @SQLKvadratMax nvarchar(max) = 'SELECT @maxOUT = count(id) from #tmpSquarenetIntersect'
	DECLARE @MaxParmDefinition nvarchar(500);
	SET @MaxParmDefinition = '@maxOUT int OUTPUT';
	-- execute the inline SQL statement, result is stored in tem table
	exec sp_executesql @SQLKvadratMax, @MaxParmDefinition, @maxOUT=@max OUTPUT;

	-- Loop through each feature and calculate field for the parameters:
	-- Intersection overlap area
	-- percentage of overlapping area
	-- percentage of overlapping area
	-- percentage of intersection in relation to the complete poppulation area
	WHILE @counter <= @max
		BEGIN
			-- prepare declarations, calculations
			-- and insertion in resulting table
			DECLARE @theSquareId nvarchar(30)
			SELECT @theSquareId = squareId from #tmpSquarenetIntersect where id=@counter;  

			DECLARE @theSquareGeometry geometry
			SET @theSquareGeometry = geometry::STGeomFromText((SELECT geom from #tmpSquarenetIntersect where id=@counter).STAsText ( )  , 25832);  

			DECLARE @intersectionGeometry geometry; 
			SET @intersectionGeometry = geometry::STGeomFromText((SELECT @inputGeometry.STIntersection(@theSquareGeometry)).STAsText ( ), 25832);  
			
			Declare @area_overlap float
			set @area_overlap = @intersectionGeometry.STArea()

			-- SPECIAL case: set minimum area to 1 m2 for point and line feature types
			if @area_overlap = 0 AND (@geomType = 'Point' OR @geomType = 'LINESTRING')
			BEGIN
				SET @area_overlap = 1
			END
			
			Declare @percentage_overlap float
			set @percentage_overlap = (@area_overlap / @theSquareGeometry.STArea()) * 100

			Declare @percentage_population float
			set @percentage_population = (@area_overlap / @FeatureLayerCompleteArea) * 100
			
			-- store geometry and calculated parameters into resulting table for the specified category
			DECLARE @insert_statement nvarchar(max)
			DECLARE @insert_statement_param_def nvarchar(max)
			SET @insert_statement = 'INSERT INTO ' + @OutputTableName + '
								  (square_id, origin_layer, origin_featureid, area_overlap, item_percentage_overlap, item_percentage_population, geom, complete_overlap_square, complete_percentage_overlap_square, complete_percentage_population)
								  Values (@theSquareIdIn, @origin_layerIn, @origin_featureidIn, @area_overlapIn, @percentage_overlapIn, @percentage_populationIn, @geomIn, @area_overlapIn, @percentage_overlapIn, @percentage_populationIn)';
			SET @insert_statement_param_def = '@theSquareIdIn nvarchar(30), @origin_layerIn nvarchar(30), @origin_featureidIn int, @area_overlapIn Float, @percentage_overlapIn Float, @percentage_populationIn Float, @geomIn geometry';
			-- execute the inline SQL statement, resulting geometry is stored in @inputGeometry parameter
			EXECUTE sp_executesql @insert_statement, @insert_statement_param_def, @theSquareIdIn = @theSquareId, @origin_layerIn = @TableName, @origin_featureidIn = @FeatureIdFieldValue, @area_overlapIn = @area_overlap, @percentage_overlapIn = @percentage_overlap, @percentage_populationIn = @percentage_population, @geomIn =  @theSquareGeometry;


			SET @counter = @counter + 1
		END

	-- return '1' as successfully completion
	return 1
END
