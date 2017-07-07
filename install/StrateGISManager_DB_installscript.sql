/****** INSTALLSCRIPT FOR StrateGIS DATABASE
This script will install tables for managament and settings for StrateGIS project database
Please note: the database is empty and stored procedures and settings has to be installed separately afterwards
Script Date: 19-06-2017 08:25:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[category](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[category_name] [varchar](30) NOT NULL,
	[category_output_layer_name] [varchar](30) NOT NULL,
 CONSTRAINT [PK_category] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO


/****** Category defintion table
Contain settings for tables assigned to a number of categories
Script Date: 19-06-2017 08:26:45 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[category_definition](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[category_id] [int] NOT NULL,
	[layer_name] [nvarchar](30) NOT NULL,
	[output_layer_name] [nvarchar](30) NOT NULL,
 CONSTRAINT [PK_category_definition_1] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [dbo].[category_definition]  WITH CHECK ADD  CONSTRAINT [FK_category_definition_category] FOREIGN KEY([category_id])
REFERENCES [dbo].[category] ([id])
GO

ALTER TABLE [dbo].[category_definition] CHECK CONSTRAINT [FK_category_definition_category]
GO


/****** category_definition_rules table
Contain specification of rules assigned to each layer
Please Note: zero to many relation between rules and tables, which means that none, one or multiple rules can be assigned to the same layer
Script Date: 19-06-2017 08:26:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[category_definition_rules](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[category_definition_id] [int] NOT NULL,
	[rules_id] [int] NOT NULL,
	[rule_argument1] [real] NULL,
	[rule_argument2] [nvarchar](30) NULL
) ON [PRIMARY]

GO


/****** globals table
Contain global definitions for StrateGIS database
Script Date: 19-06-2017 08:27:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[globals](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[property_name] [varchar](30) NOT NULL,
	[property_value] [varchar](255) NULL
) ON [PRIMARY]

GO

INSERT INTO [dbo].[globals]
           ([property_name]
           ,[property_value])
     VALUES
           ('FIdFieldName'
           ,'fid')
GO
INSERT INTO [dbo].[globals]
           ([property_name]
           ,[property_value])
     VALUES
           ('GeometryFieldname'
           ,'geom')
GO

/****** grid table
The reference grid, used when clipping geometry objects into smaller parts
 and used to sum and compare properties from different tables into categories
Script Date: 19-06-2017 08:28:13 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[square_grid](
	[fid] [int] IDENTITY(1,1) NOT NULL,
	[geom] [geometry] NULL,
	[square_id] [nvarchar](254) NULL,
 CONSTRAINT [PK_square_grid] PRIMARY KEY CLUSTERED 
(
	[fid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO

/****** Note: Rememeber to adjust the specified BOUNDING_BOX ******/
CREATE SPATIAL INDEX [square_grid_spatial_index] ON [dbo].[square_grid]
(
	[geom]
)USING  GEOMETRY_GRID 
WITH (BOUNDING_BOX =(585000, 6090000, 617400, 6123000), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO



/****** rules table
Definition of rules, by name and stored procedure.
Please Note: There must exist one stored procedure per rule
Script Date: 19-06-2017 08:28:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[rules](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[rule_name] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_rules] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

INSERT INTO [dbo].[rules]
           ([rule_name])
     VALUES
           ('sp_AreaRule')
GO

INSERT INTO [dbo].[rules]
           ([rule_name])
     VALUES
           ('sp_IntersectRule')
GO


/****** Object:  Table [geometry_columns]    
Helper table for displaying tables in QGIS
Script Date: 21-06-2017 20:42:22 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[geometry_columns](
	[f_table_catalog] [varchar](128) NOT NULL,
	[f_table_schema] [varchar](128) NOT NULL,
	[f_table_name] [varchar](256) NOT NULL,
	[f_geometry_column] [varchar](256) NOT NULL,
	[coord_dimension] [int] NOT NULL,
	[srid] [int] NOT NULL,
	[geometry_type] [varchar](30) NOT NULL,
 CONSTRAINT [geometry_columns_pk] PRIMARY KEY CLUSTERED 
(
	[f_table_catalog] ASC,
	[f_table_schema] ASC,
	[f_table_name] ASC,
	[f_geometry_column] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO


INSERT INTO [dbo].[geometry_columns]
           ([f_table_catalog]
           ,[f_table_schema]
           ,[f_table_name]
           ,[f_geometry_column]
           ,[coord_dimension]
           ,[srid]
           ,[geometry_type])
     VALUES
           ('StrateGIS'
           ,'dbo'
           ,'square_grid'
           ,'geom'
           ,2
           ,25832
           ,'POLYGON')
GO


/****** Object:  Table [spatial_ref_sys]    
Spatial reference table for QGIS
Script Date: 21-06-2017 20:43:07 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[spatial_ref_sys](
	[srid] [int] NOT NULL,
	[auth_name] [varchar](256) NULL,
	[auth_srid] [int] NULL,
	[srtext] [varchar](2048) NULL,
	[proj4text] [varchar](2048) NULL,
PRIMARY KEY CLUSTERED 
(
	[srid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

INSERT INTO [dbo].[spatial_ref_sys]
           ([srid]
           ,[auth_name]
           ,[auth_srid]
           ,[srtext]
           ,[proj4text])
     VALUES
           (25832
           ,'EPSG'
           ,25832
           ,'PROJCS["ETRS89 / UTM zone 32N",GEOGCS["ETRS89",DATUM["European_Terrestrial_Reference_System_1989",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6258"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4258"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",9],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],AUTHORITY["EPSG","25832"],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
           ,'+proj=utm +zone=32 +ellps=GRS80 +units=m +no_defs')
GO
