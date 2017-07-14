/****** StoredProcedure sp_UpdateCategory Script Date: 19-06-2017 08:40:37 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Lars Klindt, Geopartner A/S
-- Create date: 20170619
-- Description:	sp_AreaRule, StrateGIS project
-- =============================================
-- Updates the complete content of the category, based on a summation of scores  and overlaps from all layers assigned to this category.
-- If the category does not exist, the category output table is created.
-- The category output table name is specified in the 'category' table.
CREATE PROCEDURE [dbo].[sp_UpdateCategory]
	-- the category name, in which to update specified layers
	@CategoryName nvarchar(30) = 'CategoryName'
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @Category_output_layer_name varchar(30)
	SELECT @Category_output_layer_name = category_output_layer_name FROM category WHERE category_name = @CategoryName

	-- create new table for output of aggragated category calculations
	-- if not existing
   DECLARE @SQLCreateString NVARCHAR(MAX)
   SET @SQLCreateString = 'IF NOT EXISTS (SELECT * FROM sys.tables WHERE name=''' + @Category_output_layer_name + ''')
			CREATE TABLE ' + @Category_output_layer_name + ' (
			[ID] [int] IDENTITY(1,1) NOT NULL,
			[square_id] [nvarchar](30) NOT NULL,
			[category_area_overlap] [float] NOT NULL,
			[category_score] [float] NOT NULL,
			[category_item_percentage_overlap] [float] NULL,
			[category_item_percentage_population] [float] NULL,
			[geom] [geometry] NOT NULL,
				PRIMARY KEY CLUSTERED 
				(
					[ID] ASC
				)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
				) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]'
   EXEC (@SQLCreateString)
   
	-- create refrence in geometry_columns table
   DECLARE @SQLCreateRefInGeometryColumnsString NVARCHAR(MAX)
   SET @SQLCreateRefInGeometryColumnsString = 'IF (SELECT Count(*) FROM geometry_columns WHERE f_table_name=''' + @Category_output_layer_name + ''') = 0
			INSERT INTO geometry_columns (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, geometry_type)
			VALUES (''Svendborg_Vand'', ''dbo'', ''' + @Category_output_layer_name + ''', ''geom'', 2, 25832, ''POLYGON'')'
   EXEC (@SQLCreateRefInGeometryColumnsString)
   
   -- clear table
   DECLARE @SQLDeleteCreateString NVARCHAR(MAX)
   SET @SQLDeleteCreateString = 'DELETE FROM ' + @Category_output_layer_name + ' '
   EXEC (@SQLDeleteCreateString)



   		-- drop previous temp table, if any
	IF OBJECT_ID('tempdb..#tmpcategorylayers') IS NOT NULL
		DROP TABLE #tmpcategorylayers

	-- create temp table for holding intersecting geometries from squarenet
	CREATE TABLE #categoryTemp(id int identity, output_layer_name varchar(30));

	INSERT INTO #categoryTemp
		SELECT output_layer_name
		FROM category_definition, category
		WHERE category_name = @CategoryName
		AND category_definition.category_id = category.id
		ORDER BY output_layer_name DESC


	DECLARE @max int = 0;
	SELECT @max = COUNT(id) FROM #categoryTemp
	DECLARE @counter int = 1;

	-- Loop through each featurelayer in order to update complete category
	WHILE @counter <= @max
		BEGIN
			DECLARE @theLayer varchar(30)
			SELECT @theLayer = output_layer_name FROM #categoryTemp WHERE id = @counter

			-- create insert/update staement, using join expression from featurelayers square_id
			DECLARE @SQLInsertUpdateCategory NVARCHAR(MAX)
			SET @SQLInsertUpdateCategory = 'WITH CTE AS
				(
					SELECT square_id, ISNULL(SUM(area_overlap), 0) as f_area, ISNULL(SUM(feature_score), 0) as f_score
					FROM ' + @theLayer + '
					GROUP BY square_id
				)
				UPDATE ' + @Category_output_layer_name + '
					SET category_area_overlap += f_area,
						category_score += f_score
				FROM ' + @Category_output_layer_name + ' P
				INNER JOIN CTE S ON S.square_id = P.square_id
				IF @@ROWCOUNT=0
					BEGIN
						INSERT INTO ' + @Category_output_layer_name + '
						(square_id, category_area_overlap, category_score, geom)	
						SELECT square_id, ISNULL(SUM(area_overlap), 0) as f_area, ISNULL(SUM(feature_score), 0) as f_score, geometry::STGeomFromText(''LINESTRING (100 100, 20 180, 180 180)'', 25832)
							FROM ' + @theLayer + '
							GROUP BY square_id
						UPDATE P SET P.geom =  S.geom
						FROM ' + @Category_output_layer_name + ' P
						INNER JOIN ' + @theLayer + ' S ON S.square_id = P.square_id 
					END
				ELSE
					BEGIN
						INSERT INTO ' + @Category_output_layer_name + '
						(square_id, category_area_overlap, category_score, geom)	
							SELECT ' + @theLayer + '.square_id, ISNULL(SUM(area_overlap), 0) as f_area, ISNULL(SUM(feature_score), 0) as f_score, geometry::STGeomFromText(''LINESTRING (100 100, 20 180, 180 180)'', 25832)
							FROM ' + @theLayer + '
							LEFT JOIN ' + @Category_output_layer_name + ' ON ' + @Category_output_layer_name + '.square_id = ' + @theLayer + '.square_id
							WHERE ' + @Category_output_layer_name + '.square_id is NULL
							GROUP BY ' + @theLayer + '.square_id
						UPDATE P SET P.geom =  S.geom
						FROM ' + @Category_output_layer_name + ' P
						INNER JOIN ' + @theLayer + ' S ON S.square_id = P.square_id 
						

					END'
			EXEC (@SQLInsertUpdateCategory)

			SET @counter = @counter + 1
		END
	-- determine complete area for all featureclass features
	-- create insert/update staement, using join expression from featurelayers square_id
	DECLARE @SQLUpdateCategoryarea NVARCHAR(MAX)
	SET @SQLUpdateCategoryarea = 'WITH CTE AS
		(
			SELECT ISNULL(SUM(category_area_overlap), 0) as featureclass_area
			FROM ' + @Category_output_layer_name + '			
		)
		UPDATE ' + @Category_output_layer_name + '
			SET category_item_percentage_overlap = (category_area_overlap/10000)*100,
				category_item_percentage_population = (category_area_overlap/S.featureclass_area)*100
		FROM ' + @Category_output_layer_name + ' P, CTE S
	'
	EXEC (@SQLUpdateCategoryarea)

	-- return '1' as successfully completion
	return 1

END
