/*srs_cap_20240701_2425.csv
srs_cap_20240708_2425.csv
srs_cap_20240715_2425.csv
srs_cap_20240722_2425.csv
srs_cap_20240729_2425.csv
srs_cap_20240819_2425.csv
srs_cap_20240827_2425.csv
srs_cap_20240902_2425.csv
srs_cap_20240909_2425.csv
srs_cap_20240916_2425.csv
srs_cap_20240923_2425.csv
srs_cap_20240930_2425.csv
srs_cap_20241007_2425.csv
srs_cap_20241014_2425.csv
srs_cap_20241021_2425.csv
srs_cap_20241028_2425.csv
srs_cap_20241106_2425.csv
srs_cap_20241111_2425.csv
srs_cap_20241125_2425.csv
srs_cap_20241202_2425.csv
srs_cap_20241209_2425.csv
srs_cap_20241216_2425.csv
srs_cap_20250106_2425.csv*/
select count(*) from sipr.dbo.srs_acd_history_STAGING
truncate table sipr.dbo.srs_acd_history_STAGING
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240701_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240708_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240715_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240722_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240729_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240819_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240827_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240902_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240909_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240916_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240923_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20240930_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241007_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241014_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241021_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241028_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241106_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241111_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241125_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241202_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241209_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20241216_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );
BULK INSERT sipr.dbo.srs_acd_history_STAGING     FROM 'D:\DataQuality\CAP_Weekly\July2024\srs_cap_20250106_2425.csv'WITH (FIRSTROW = 2,        FIELDTERMINATOR = ',',        ROWTERMINATOR = '0x0A',        TABLOCK    );

INSERT INTO sipr.dbo.srs_acd_history SELECT * FROM sipr.dbo.srs_acd_history_STAGING