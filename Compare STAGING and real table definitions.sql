-- Temp table to store mismatches
IF OBJECT_ID('tempdb..#ColumnDifferences') IS NOT NULL DROP TABLE #ColumnDifferences;

CREATE TABLE #ColumnDifferences (
    TableName NVARCHAR(128),
    ColumnName NVARCHAR(128),
    Issue NVARCHAR(512),
    Base_DataType NVARCHAR(128),
    Staging_DataType NVARCHAR(128),
    Base_Length INT,
    Staging_Length INT,
    Base_Precision INT,
    Staging_Precision INT,
    Base_Scale INT,
    Staging_Scale INT,
    Base_Nullable BIT,
    Staging_Nullable BIT
);

DECLARE @BaseTable SYSNAME, @StagingTable SYSNAME;

DECLARE table_cursor CURSOR FOR
SELECT name FROM sys.tables
WHERE name LIKE 'srs_%'
  AND name NOT LIKE '%_staging'
  AND OBJECT_ID(name + '_staging') IS NOT NULL;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @BaseTable;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @StagingTable = @BaseTable + '_staging';

    WITH TableColumns AS (
        SELECT 
            tbl.name AS TableName,
            col.column_id,
            col.name AS ColumnName,
            typ.name AS DataType,
            col.max_length,
            col.precision,
            col.scale,
            col.is_nullable
        FROM sys.tables tbl
        JOIN sys.columns col ON tbl.object_id = col.object_id
        JOIN sys.types typ ON col.user_type_id = typ.user_type_id
        WHERE tbl.name IN (@BaseTable, @StagingTable)
    )
    INSERT INTO #ColumnDifferences
    SELECT 
        @BaseTable AS TableName,
        COALESCE(b.ColumnName, s.ColumnName) AS ColumnName,
        CASE
            WHEN b.ColumnName IS NULL THEN 'Column missing in base table'
            WHEN s.ColumnName IS NULL THEN 'Column missing in staging table'
            WHEN b.DataType <> s.DataType THEN 'Data type mismatch'
            WHEN b.max_length <> s.max_length THEN 'Length mismatch'
            WHEN b.precision <> s.precision THEN 'Precision mismatch'
            WHEN b.scale <> s.scale THEN 'Scale mismatch'
            WHEN b.is_nullable <> s.is_nullable THEN 'Nullability mismatch'
            ELSE NULL
        END AS Issue,
        b.DataType AS Base_DataType,
        s.DataType AS Staging_DataType,
        b.max_length AS Base_Length,
        s.max_length AS Staging_Length,
        b.precision AS Base_Precision,
        s.precision AS Staging_Precision,
        b.scale AS Base_Scale,
        s.scale AS Staging_Scale,
        b.is_nullable AS Base_Nullable,
        s.is_nullable AS Staging_Nullable
    FROM 
        (SELECT * FROM TableColumns WHERE TableName = @BaseTable) b
    FULL OUTER JOIN 
        (SELECT * FROM TableColumns WHERE TableName = @StagingTable) s
        ON b.ColumnName = s.ColumnName
    WHERE
        -- Only return rows where there's a mismatch
        b.ColumnName IS NULL OR
        s.ColumnName IS NULL OR
        b.DataType <> s.DataType OR
        b.max_length <> s.max_length OR
        b.precision <> s.precision OR
        b.scale <> s.scale OR
        b.is_nullable <> s.is_nullable;

    FETCH NEXT FROM table_cursor INTO @BaseTable;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

-- Show mismatches
SELECT * FROM #ColumnDifferences
ORDER BY TableName, ColumnName;
