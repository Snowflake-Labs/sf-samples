-- Step 1: Switch to a neutral system database
USE master;
GO

-- Show sessions connected to Northwind
SELECT
    spid,
    loginame,
    db_name(dbid) AS DBName,
    status,
    cmd,
    program_name
FROM sys.sysprocesses
WHERE dbid = DB_ID('Northwind');

KILL 59;

-- Step 2: Drop the target database
DROP DATABASE Northwind;
GO

-- Step 3 (Optional): Re-create and reset everything
CREATE DATABASE Northwind;
GO

USE Northwind;
GO

-- list all schemas and tables
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE';

-- enable Change Tracking on Database
ALTER DATABASE Northwind
    SET CHANGE_TRACKING = ON
    (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);

-- Enable Change Tracking on Tables
ALTER TABLE dbo.Orders
    ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE dbo.OrderDetails
    ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE dbo.Products
    ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = ON);

ALTER TABLE dbo.Customers
    ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = ON);

-- check the data

SELECT * FROM dbo.Orders ORDER by OrderID DESC;
SELECT * FROM dbo.Products ORDER BY 1 DESC;
SELECT * FROM dbo.OrderDetails WHERE ProductID = 78;

-- live orders

SELECT TOP 10 *
FROM Orders
ORDER BY OrderDate DESC;

SELECT TOP 10 *
FROM [OrderDetails]
WHERE ProductID = 78
ORDER BY OrderID DESC;










-- Add Waffles --
UPDATE O
SET
    O.OrderDate = DATEFROMPARTS(1997, 11, 01),
    O.RequiredDate = DATEFROMPARTS(1997, 11, 05),
    O.ShippedDate = DATEFROMPARTS(1997, 11, 02)
FROM dbo.Orders O
         JOIN dbo.OrderDetails OD ON O.OrderID = OD.OrderID
WHERE OD.ProductID = 78
  AND O.OrderDate >= DATEADD(DAY, -7, GETDATE());


ALTER TABLE dbo.Region
    ENABLE CHANGE_TRACKING
        WITH (TRACK_COLUMNS_UPDATED = ON);

