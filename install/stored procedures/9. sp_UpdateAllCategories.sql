USE [StrateGIS]
GO
/****** Object:  StoredProcedure [dbo].[sp_UpdateAllCategories]    Script Date: 16-10-2017 14:17:54 ******/
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
CREATE PROCEDURE [dbo].[sp_UpdateAllCategories]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	-- create new table for output of aggragated category calculations
	-- if not existing
   DECLARE @SQLCreateString NVARCHAR(MAX)
   SET @SQLCreateString = 'IF NOT EXISTS (SELECT * FROM sys.tables WHERE name=''All_Categories'')
			CREATE TABLE All_Categories (
			[ID] [int] IDENTITY(1,1) NOT NULL,
			[square_id] [nvarchar](30) NOT NULL,
			[category_area_overlap] [float] NOT NULL,
			[category_score] [float] NOT NULL,
			[category_avg_score] [float] NULL,
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
   SET @SQLCreateRefInGeometryColumnsString = 'IF (SELECT Count(*) FROM geometry_columns WHERE f_table_name=''All_Categories'') = 0
			INSERT INTO geometry_columns (f_table_catalog, f_table_schema, f_table_name, f_geometry_column, coord_dimension, srid, geometry_type)
			VALUES (''Svendborg_Vand'', ''dbo'', ''All_Categories'', ''geom'', 2, 25832, ''POLYGON'')'
   EXEC (@SQLCreateRefInGeometryColumnsString)
   
   -- clear table
   DECLARE @SQLDeleteCreateString NVARCHAR(MAX)
   SET @SQLDeleteCreateString = 'DELETE FROM All_Categories'
   EXEC (@SQLDeleteCreateString)



   		-- drop previous temp table, if any
	IF OBJECT_ID('tempdb..#tmpcategorylayers') IS NOT NULL
		DROP TABLE #tmpcategorylayers

	-- create temp table for holding intersecting geometries from squarenet
	CREATE TABLE #allCategoryTemp(id int identity, category_output_layer_name varchar(30));

	INSERT INTO #allCategoryTemp
		SELECT category_output_layer_name
		FROM category
		WHERE includeInAllCategories=1
		ORDER BY category_name DESC


	DECLARE @max int = 0;
	SELECT @max = COUNT(id) FROM #allCategoryTemp
	DECLARE @counter int = 1;

	-- Loop through each featurelayer in order to update complete category
	WHILE @counter <= @max
		BEGIN
			DECLARE @theCategoryOutputLayerName varchar(30)
			SELECT @theCategoryOutputLayerName = category_output_layer_name FROM #allCategoryTemp WHERE id = @counter

			-- create insert/update staement, using join expression from featurelayers square_id
			DECLARE @SQLInsertUpdateCategory NVARCHAR(MAX)
			SET @SQLInsertUpdateCategory = 'WITH CTE AS
				(
					SELECT square_id, ISNULL(SUM(category_area_overlap), 0) as cat_area, ISNULL(SUM(category_score), 0) as cat_score, ISNULL(SUM(category_avg_score), 0) as cat_avg_score
					FROM ' + @theCategoryOutputLayerName + '
					GROUP BY square_id
				)
				UPDATE All_Categories
					SET All_Categories.category_area_overlap += cat_area,
						All_Categories.category_score += cat_score,
						All_Categories.category_avg_score += cat_avg_score
				FROM All_Categories P
				INNER JOIN CTE S ON S.square_id = P.square_id
				IF @@ROWCOUNT=0
					BEGIN
						INSERT INTO All_Categories
						(P.square_id, P.category_area_overlap, P.category_score, P.category_avg_score, geom)	
						SELECT square_id, ISNULL(SUM(' + @theCategoryOutputLayerName + '.category_area_overlap), 0) as cat_area, ISNULL(SUM(' + @theCategoryOutputLayerName + '.category_score), 0) as cat_score, ISNULL(SUM(' + @theCategoryOutputLayerName + '.category_avg_score), 0) as cat_avg_score, geometry::STGeomFromText(''LINESTRING (100 100, 20 180, 180 180)'', 25832)
							FROM ' + @theCategoryOutputLayerName + '
							GROUP BY square_id
						UPDATE P SET P.geom =  S.geom
						FROM All_Categories P
						INNER JOIN ' + @theCategoryOutputLayerName + ' S ON S.square_id = P.square_id 
					END
				ELSE
					BEGIN
						INSERT INTO All_Categories
						(P.square_id, P.category_area_overlap, P.category_score, P.category_avg_score, P.geom)	
							SELECT ' + @theCategoryOutputLayerName + '.square_id, ISNULL(SUM(' + @theCategoryOutputLayerName + '.category_area_overlap), 0) as cat_area, ISNULL(SUM(' + @theCategoryOutputLayerName + '.category_score), 0) as cat_score, ISNULL(SUM(' + @theCategoryOutputLayerName + '.category_avg_score), 0) as cat_avg_score, geometry::STGeomFromText(''LINESTRING (100 100, 20 180, 180 180)'', 25832)
							FROM ' + @theCategoryOutputLayerName + '
							LEFT JOIN All_Categories ON All_Categories.square_id = ' + @theCategoryOutputLayerName + '.square_id
							WHERE All_Categories.square_id is NULL
							GROUP BY ' + @theCategoryOutputLayerName + '.square_id
						UPDATE All_Categories SET All_Categories.geom =  S.geom
						FROM All_Categories P
						INNER JOIN ' + @theCategoryOutputLayerName + ' S ON S.square_id = P.square_id 
						

					END'
			EXEC (@SQLInsertUpdateCategory)



			SET @counter = @counter + 1
		END
	-- determine complete area for all featureclass features
	-- create insert/update staement, using join expression from featurelayers square_id
	DECLARE @SQLUpdateAllCategoryarea NVARCHAR(MAX)
	SET @SQLUpdateAllCategoryarea = 'WITH CTE AS
		(
			SELECT ISNULL(SUM(category_area_overlap), 0) as featureclass_area
			FROM All_Categories			
		)
		UPDATE All_Categories
			SET All_Categories.category_item_percentage_overlap = (category_area_overlap/10000)*100,
				All_Categories.category_item_percentage_population = (category_area_overlap/S.featureclass_area)*100
		FROM All_Categories P, CTE S
	'
	EXEC (@SQLUpdateAllCategoryarea)

	-- return '1' as successfully completion
	return 1

END
