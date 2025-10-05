DECLARE @StartTime DATETIME = GETDATE();
DECLARE @EndTime DATETIME = DATEADD(MINUTE, 5, @StartTime);
DECLARE @WaffleProductID INT = 78;
DECLARE @NewOrderID INT;

WHILE GETDATE() < @EndTime
    BEGIN
        -- 🔀 Random CustomerID + City from Customers table
        DECLARE @CustomerID NVARCHAR(5);
        DECLARE @ShipCity NVARCHAR(15);
        DECLARE @ShipRegion NVARCHAR(15);
        DECLARE @ShipPostalCode NVARCHAR(10);
        DECLARE @ShipCountry NVARCHAR(15);

        SELECT TOP 1
            @CustomerID = CustomerID,
            @ShipCity = City,
            @ShipRegion = Region,
            @ShipPostalCode = PostalCode,
            @ShipCountry = Country
        FROM Customers
        WHERE City IS NOT NULL
        ORDER BY NEWID();

        -- Insert into Orders
        INSERT INTO Orders (
            CustomerID,
            EmployeeID,
            OrderDate,
            RequiredDate,
            ShippedDate,
            ShipVia,
            Freight,
            ShipName,
            ShipAddress,
            ShipCity,
            ShipRegion,
            ShipPostalCode,
            ShipCountry
        )
        VALUES (
                   @CustomerID, 1, GETDATE(), DATEADD(DAY, 2, GETDATE()), GETDATE(),
                   1, ROUND(RAND() * 20 + 5, 2),
                   'Pop-Up Waffle Stand', 'Waffle Street 42', @ShipCity, @ShipRegion, @ShipPostalCode, @ShipCountry
               );

        SET @NewOrderID = SCOPE_IDENTITY();

        -- Insert into OrderDetails
        INSERT INTO [OrderDetails] (
            OrderID, ProductID, UnitPrice, Quantity, Discount
        )
        VALUES (
                   @NewOrderID, @WaffleProductID, 5.99, FLOOR(RAND() * 40 + 10), 0
               );

        -- Wait 5 seconds before next insert
        WAITFOR DELAY '00:00:05';
    END;