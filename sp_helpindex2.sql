USE [master];
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_helpindex2')
	EXEC ('CREATE PROC dbo.sp_helpindex2 AS SELECT ''stub version, to be replaced''')
GO
ALTER PROCEDURE dbo.sp_helpindex2
( 
 @Table sysname
,@Schema sysname = 'dbo'
,@IndexExtendedInfo bit = 0
,@MissingIndexesInfo bit = 1
,@ColumnsInfo bit = 1
)

/*
	Yaniv Etrogi 20100328
	sp_helpindex2 adds the included columns information that is not provided by the original sp_helpindex.
	
	Yaniv Etrogi 20101115
	Modified the bellow line:
	,ROW_NUMBER() OVER (PARTITION BY sc.is_included_column ORDER BY sc.key_ordinal) ColPos
	sc.key_ordinal replaces sc.column_id to resolve a bug where in a composite index the keys were not correctly ordered.
	
	Yaniv Etrogi 20110411
	Modified the ORDER BY of the final reult set to be ordered by the [columns] column instead of the index name making redundant indexes to be more easily noticed in the the result set 
	Added 4 important columns showing index statistics information regarding the usage of the index: user_updates, user_seeks, user_scans and user_lookups


	Yaniv Etrogi 20110420
	Added an aditional result set that shows missing indexes information
	
	Boaz Goldstein 20130123
	Added Index_Size_MB column that shows size for each index in first result set

	Yaniv Etrogi 20150628
	1. Add the following condition to prevent cases where an index on a partitioned table returned the partitiniong column even though it was not part of the index key
	AND ic.key_ordinal = CASE WHEN (ic.key_ordinal = 0 AND ic.is_included_column = 0) THEN 1 ELSE ic.key_ordinal END 
	2. Add an additional colummn to the output: data_compression
	3. Add an additional colummn to the output: partition_count
	4. When @IndexExtendedInfo is on modified the avg_fragmentation_in_percent to return a single row which is the AVG to prevent cases where on a partitioned table many rows were returned (row per partiton)

	Yaniv Etrogi 20150709
	Removed the colummn data_compression from the output as it brought duplicates on parrtitoned tables where some of the partitotns where compressed on some not compressed

	Yaniv Etrogi 20161101
	Add support for columnstore and xml indexes
	Add a new input parameter @Schema to support tables with schemas other than the dbo schema

	Yaniv Etrogi 20170509
	Add @Schema in additional SARGs to resolve a bug

	Yaniv Etrogi 20190102
	Add the index_id and data_compression columns to the output
*/
AS
SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;


-- index information
IF OBJECT_ID('tempdb.dbo.#indexes', 'U') IS NOT NULL DROP TABLE dbo.#indexes;
CREATE TABLE dbo.#indexes
(
	[schema] [nvarchar](128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[table] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[index] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[object_id] [int] NOT NULL,
	[data_space_id] [int] NULL,
	[index_id] [int] NOT NULL,
	[type] [tinyint] NOT NULL,
	[index_size_mb] [bigint] NULL,
	[ignore_dup_key] [bit] NULL,
	[is_unique] [bit] NULL,
	[is_hypothetical] [bit] NULL,
	[is_primary_key] [bit] NULL,
	[is_unique_constraint] [bit] NULL,
	[auto_created] [bit] NULL,
	[no_recompute] [bit] NULL,
	[allow_row_locks] [bit] NULL,
	[allow_page_locks] [bit] NULL,
	[is_disabled] [bit] NULL,
	[fill_factor] [tinyint] NOT NULL,
	[is_padded] [bit] NULL,
	[data_compression_desc] [nvarchar](60) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[partition_count] [int] NULL,
	[columns] [varchar](MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[included_columns] [varchar](MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

INSERT dbo.#indexes
  ([schema]
  ,[table]
  ,[index]
  ,[object_id]
  ,data_space_id
  ,index_id
  ,[type]
  ,index_size_mb
  ,[ignore_dup_key]
  ,is_unique
  ,is_hypothetical
  ,is_primary_key
  ,is_unique_constraint
  ,auto_created
  ,no_recompute
  ,[allow_row_locks]
  ,[allow_page_locks]
  ,is_disabled
  ,fill_factor
  ,is_padded
  ,data_compression_desc
  ,partition_count
  ,[columns]
  ,included_columns)
SELECT
  SCHEMA_NAME(o.SCHEMA_ID) 
 ,o.name AS [table]
 ,i.name AS [index]
 ,i.object_id
 ,i.data_space_id
 ,i.index_id
 ,i.type
 ,isize.index_size_mb
 ,i.[ignore_dup_key]
 ,i.is_unique
 ,i.is_hypothetical
 ,i.is_primary_key
 ,i.is_unique_constraint
 ,s.auto_created
 ,s.no_recompute
 ,i.[allow_row_locks]
 ,i.[allow_page_locks]
 ,i.is_disabled
 ,i.fill_factor
 ,i.is_padded
 ,isize.data_compression_desc
 ,isize.partition_count
 ,LEFT(list, ISNULL(splitter - 1, LEN(list))) AS [columns]
 ,SUBSTRING(list, indCol.splitter + 1, 4000) AS included_columns
FROM sys.indexes i
INNER JOIN sys.objects o ON i.[object_id] = o.[object_id]
INNER JOIN sys.stats s ON i.[object_id] = s.[object_id] AND i.index_id = s.stats_id
INNER JOIN (
							SELECT
								  i.index_id
								 ,i.object_id
								 ,p.data_compression_desc
								  ,SUM(au.used_pages) / 128 AS index_size_mb
								  ,COUNT(DISTINCT p.partition_number) AS partition_count
							FROM sys.indexes i
							INNER JOIN sys.partitions p ON p.index_id = i.index_id AND p.object_id = i.object_id
							INNER JOIN sys.allocation_units au ON au.container_id = p.partition_id
							GROUP BY i.index_id, i.object_id ,p.data_compression_desc
           ) isize ON isize.index_id = i.index_id AND isize.object_id = i.object_id
           
CROSS APPLY (SELECT NULLIF(CHARINDEX('|', indexCols.list), 0) splitter ,list
             FROM
              (
               SELECT
									CAST((SELECT CASE WHEN sc.is_included_column = 1 AND sc.ColPos = 1 THEN '|' ELSE '' END + CASE WHEN sc.ColPos > 1 THEN ', ' ELSE '' END + name
                      FROM
                        (
													 SELECT
														ic.is_included_column
													 ,ic.index_column_id
													 ,c.name
													 ,ROW_NUMBER() OVER (PARTITION BY ic.is_included_column ORDER BY ic.key_ordinal) ColPos
													 FROM sys.index_columns ic
													 INNER JOIN sys.columns c ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
													 WHERE ic.index_id = i.index_id 
													 AND ic.[object_id] = i.[object_id] 
													 AND ic.key_ordinal = CASE WHEN (ic.key_ordinal = 0 AND ic.is_included_column = 0) THEN 1 ELSE ic.key_ordinal END
                        ) sc
                      ORDER BY sc.is_included_column, ColPos
                     FOR XML PATH('') ,TYPE ) AS varchar(MAX)) list
              ) indexCols
            ) indCol
WHERE o.name = @Table AND SCHEMA_NAME(o.schema_id) = @Schema;


-- Index usage stats
IF OBJECT_ID('tempdb.dbo.#index_usage_stats', 'U') IS NOT NULL DROP TABLE dbo.#index_usage_stats;
CREATE TABLE dbo.#index_usage_stats
(
	[index] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[user_updates] [bigint] NOT NULL,
	[user_seeks] [bigint] NOT NULL,
	[user_scans] [bigint] NOT NULL,
	[user_lookups] [bigint] NOT NULL
);

INSERT dbo.#index_usage_stats
  ([index]
  ,user_updates
  ,user_seeks
  ,user_scans
  ,user_lookups)
SELECT
  i.name 
 ,s.user_updates
 ,s.user_seeks
 ,s.user_scans
 ,s.user_lookups
FROM sys.indexes i
INNER JOIN sys.dm_db_index_usage_stats s ON s.object_id = i.object_id AND i.index_id = s.index_id
WHERE OBJECTPROPERTY(i.object_id, 'IsIndexable') = 1 AND OBJECTPROPERTY(i.object_id, 'IsSystemTable') = 0 
AND s.index_id > 0 
AND OBJECT_NAME(i.object_id) = @Table --AND SCHEMA_NAME(i.schema_id) = @Schema
AND database_id = DB_ID();


-- Table information
IF OBJECT_ID('tempdb.dbo.#tables', 'U') IS NOT NULL DROP TABLE dbo.#tables;
CREATE TABLE dbo.#tables
(
	[database] [nvarchar](128) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[schema] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[table] [sysname] COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	[row_count] [bigint] NULL,
	[reserved_mb] [bigint] NULL,
	[data_mb] [bigint] NULL,
	[index_size_mb] [bigint] NULL,
	[unused_mb] [bigint] NULL
);

INSERT dbo.#tables
  ([database]
  ,[schema]
  ,[table]
  ,row_count
  ,reserved_mb
  ,data_mb
  ,index_size_mb
  ,unused_mb)
SELECT
	 DB_NAME()																					AS [database]
	,s.name																							AS [schema]
	,o.name																							AS [table]
	,ps.rows																						AS row_count
	,((ps.reserved + ISNULL(it.reserved,0))* 8) / 1024	AS reserved_mb 
	,(ps.data * 8) / 1024																AS data_mb
	,((CASE WHEN (ps.used + ISNULL(it.used,0)) > ps.data THEN (ps.used + ISNULL(it.used,0)) - ps.data ELSE 0 END) * 8) /1024 AS index_size_mb
	,((CASE WHEN (ps.reserved + ISNULL(it.reserved,0)) > ps.used THEN (ps.reserved + ISNULL(it.reserved,0)) - ps.used ELSE 0 END) * 8)/1024 AS unused_mb
FROM
 (SELECT ps.object_id
	,SUM (CASE WHEN (ps.index_id < 2) THEN row_count ELSE 0 END) AS [rows]
	,SUM (ps.reserved_page_count) AS reserved
	,SUM (CASE WHEN (ps.index_id < 2) THEN (ps.in_row_data_page_count + ps.lob_used_page_count + ps.row_overflow_used_page_count) ELSE (ps.lob_used_page_count + ps.row_overflow_used_page_count) END) AS data
	,SUM (ps.used_page_count) AS used
  FROM sys.dm_db_partition_stats ps
  GROUP BY ps.object_id) AS ps
LEFT OUTER JOIN 
 (SELECT 
	   it.parent_id
	  ,SUM(ps.reserved_page_count) AS reserved
	  ,SUM(ps.used_page_count) AS used
  FROM sys.dm_db_partition_stats ps
  INNER JOIN sys.internal_tables it ON it.object_id = ps.object_id WHERE it.internal_type IN (202,204)
  GROUP BY it.parent_id
	) AS it ON it.parent_id = ps.object_id
INNER JOIN sys.all_objects o  ON ps.object_id = o.object_id
INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type <> 'S' and o.type <> 'IT'
AND o.name = @Table AND SCHEMA_NAME(o.schema_id) = @Schema;


-- Additional index information if requested.
IF (@IndexExtendedInfo = 1)
BEGIN;

		IF OBJECT_ID('tempdb.dbo.#index_physical_stats', 'U') IS NOT NULL DROP TABLE dbo.#index_physical_stats;
		CREATE TABLE dbo.#index_physical_stats
		(
			[database_id] [smallint] NULL,
			[object_id] [int] NULL,
			[index_id] [int] NULL,
			[partition_number] [int] NULL,
			[index_type_desc] [nvarchar](60) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
			[alloc_unit_type_desc] [nvarchar](60) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
			[index_depth] [tinyint] NULL,
			[index_level] [tinyint] NULL,
			[avg_fragmentation_in_percent] [float] NULL,
			[fragment_count] [bigint] NULL,
			[avg_fragment_size_in_pages] [float] NULL,
			[page_count] [bigint] NULL,
			[avg_page_space_used_in_percent] [float] NULL,
			[record_count] [bigint] NULL,
			[ghost_record_count] [bigint] NULL,
			[version_ghost_record_count] [bigint] NULL,
			[min_record_size_in_bytes] [int] NULL,
			[max_record_size_in_bytes] [int] NULL,
			[avg_record_size_in_bytes] [float] NULL,
			[forwarded_record_count] [bigint] NULL,
			[compressed_page_count] [bigint] NULL
		);

		INSERT dbo.#index_physical_stats
		  (database_id
		  ,object_id
		  ,index_id
		  ,partition_number
		  ,index_type_desc
		  ,alloc_unit_type_desc
		  ,index_depth
		  ,index_level
		  ,avg_fragmentation_in_percent
		  ,fragment_count
		  ,avg_fragment_size_in_pages
		  ,page_count
		  ,avg_page_space_used_in_percent
		  ,record_count
		  ,ghost_record_count
		  ,version_ghost_record_count
		  ,min_record_size_in_bytes
		  ,max_record_size_in_bytes
		  ,avg_record_size_in_bytes
		  ,forwarded_record_count
		  ,compressed_page_count)
    SELECT 
		   database_id
		  ,object_id
		  ,index_id
		  ,partition_number
		  ,index_type_desc
		  ,alloc_unit_type_desc
		  ,index_depth
		  ,index_level
		  ,avg_fragmentation_in_percent
		  ,fragment_count
		  ,avg_fragment_size_in_pages
		  ,page_count
		  ,avg_page_space_used_in_percent
		  ,record_count
		  ,ghost_record_count
		  ,version_ghost_record_count
		  ,min_record_size_in_bytes
		  ,max_record_size_in_bytes
		  ,avg_record_size_in_bytes
		  ,forwarded_record_count
		  ,compressed_page_count 
    FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID(@Schema + N'.' + @Table, N'U'), NULL, NULL, N'LIMITED');


-- Final output for @IndexExtendedInfo
    SELECT
      i.[index] 
     ,CONVERT(varchar(250), 
	   CASE WHEN i.[type] = 1 THEN 'clustered' WHEN i.[type] = 2 THEN 'nonclustered' WHEN i.[type] = 3 THEN 'xml' WHEN i.[type] = 4 THEN 'spatial' WHEN i.[type] = 5 THEN 'clustered columnstore' WHEN i.[type] = 6 THEN 'nonclustered columnstore' WHEN i.[type] = 7 THEN 'nonclustered hash' ELSE 'unexecpected index type' END 
	 + CASE WHEN i.[ignore_dup_key] <> 0 THEN ', ignore duplicate keys' ELSE '' END 
	 + CASE WHEN i.is_unique <> 0 THEN ', unique' ELSE '' END 
	 + CASE WHEN i.is_hypothetical <> 0 THEN ', hypothetical' ELSE '' END 
	 + CASE WHEN i.is_primary_key <> 0 THEN ', primary key' ELSE '' END + CASE WHEN i.is_unique_constraint <> 0 THEN ', unique key' ELSE '' END 
	 + CASE WHEN i.auto_created <> 0 THEN ', auto create' ELSE '' END + CASE WHEN i.no_recompute <> 0 THEN ', stats no recompute' ELSE '' END 
	 + ' located on ' + d.name) AS [description] 
	  ,i.index_id
     ,i.[columns] 
     ,i.included_columns
     ,ius.user_seeks
     ,ius.user_scans
     ,ius.user_lookups
     ,ius.user_updates		 
	 ,ips.avg_fragmentation_in_percent
     ,t.row_count
     ,t.reserved_mb
     ,t.data_mb 
     ,t.index_size_mb AS total_index_size_mb
     ,i.index_size_mb
     ,i.data_compression_desc AS [data_compression]
	 ,i.partition_count
     ,i.is_disabled
     ,i.fill_factor
     ,i.is_padded
     ,i.[allow_row_locks]
     ,i.[allow_page_locks]
    FROM dbo.#indexes i
    INNER JOIN dbo.#tables t ON i.[schema] = t.[schema] AND i.[table] = t.[table]
    INNER JOIN sys.data_spaces d ON d.data_space_id = i.data_space_id
	INNER JOIN dbo.#index_physical_stats AS ips ON ips.index_id = i.index_id
    LEFT JOIN  dbo.#index_usage_stats ius ON ius.[index] = i.[index]
    ORDER BY i.[columns] ,i.included_columns;	
END; --IF (@IndexExtendedInfo = 1)

	ELSE

BEGIN;
-- Final output
    SELECT DISTINCT
      i.[index] 
     ,CONVERT(varchar(250), 
	   CASE WHEN i.[type] = 1 THEN 'clustered' WHEN i.[type] = 2 THEN 'nonclustered' WHEN i.[type] = 3 THEN 'xml' WHEN i.[type] = 4 THEN 'spatial' WHEN i.[type] = 5 THEN 'clustered columnstore' WHEN i.[type] = 6 THEN 'nonclustered columnstore' WHEN i.[type] = 7 THEN 'nonclustered hash' ELSE 'unexecpected index type' END 
	 + CASE WHEN i.[ignore_dup_key] <> 0 THEN ', ignore duplicate keys' ELSE '' END 
	 + CASE WHEN i.is_unique <> 0 THEN ', unique' ELSE '' END 
	 + CASE WHEN i.is_hypothetical <> 0 THEN ', hypothetical' ELSE '' END 
	 + CASE WHEN i.is_primary_key <> 0 THEN ', primary key' ELSE '' END + CASE WHEN i.is_unique_constraint <> 0 THEN ', unique key' ELSE '' END 
	 + CASE WHEN i.auto_created <> 0 THEN ', auto create' ELSE '' END + CASE WHEN i.no_recompute <> 0 THEN ', stats no recompute' ELSE '' END 
	 + ' located on ' + d.name) AS [description] 
	  ,i.index_id
     ,i.[columns] 
     ,i.included_columns
     ,ius.user_seeks
     ,ius.user_scans
     ,ius.user_lookups
     ,ius.user_updates
     ,t.row_count
     ,t.reserved_mb
     ,t.data_mb 
     ,t.index_size_mb AS total_index_size_mb
     ,i.index_size_mb
     ,i.data_compression_desc AS [data_compression]
	 ,i.partition_count
     ,i.is_disabled
     ,i.fill_factor
     ,i.is_padded
     ,i.[allow_row_locks]
     ,i.[allow_page_locks]
    FROM dbo.#indexes i
    INNER JOIN dbo.#tables t ON i.[schema] = t.[schema] AND i.[table] = t.[table]
    INNER JOIN sys.data_spaces d ON d.data_space_id = i.data_space_id
    LEFT JOIN  dbo.#index_usage_stats ius ON ius.[index] = i.[index]
    ORDER BY i.[columns] ,i.included_columns;
END;

	  
-- Missing indexes information
IF (@MissingIndexesInfo = 1)
BEGIN;
	SELECT
      d.equality_columns
     ,d.inequality_columns
     ,d.included_columns
     ,s.unique_compiles
     ,s.user_seeks
     ,s.user_scans
     ,s.last_user_seek
     ,s.last_user_scan
     ,s.avg_total_user_cost
     ,s.avg_user_impact
    FROM sys.dm_db_missing_index_details d
    INNER JOIN sys.dm_db_missing_index_groups g ON d.index_handle = g.index_handle
    INNER JOIN sys.dm_db_missing_index_group_stats s ON g.index_group_handle = s.group_handle
    WHERE database_id = DB_ID() 
	--AND OBJECT_NAME(d.object_id) = 'ProductDescription'
	AND d.object_id = OBJECT_ID(@Schema + N'.' + @Table)
    ORDER BY avg_total_user_cost * avg_user_impact * (user_seeks + user_scans) DESC;
END;


-- Columns information
IF (@ColumnsInfo = 1)
BEGIN;
	EXEC sp_columns @table_name = @Table, @table_owner = @Schema;
END;


RETURN 0;
GO
USE master;EXEC sp_MS_marksystemobject 'sp_helpindex2';
GO

