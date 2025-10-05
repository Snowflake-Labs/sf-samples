-- Step 1: Add Chocolate Waffles to Products
INSERT INTO dbo.Products (
    ProductName,
    SupplierID,
    CategoryID,
    QuantityPerUnit,
    UnitPrice,
    UnitsInStock,
    UnitsOnOrder,
    ReorderLevel,
    Discontinued
)
VALUES (
           'Chocolate Waffles',
           1,              -- SupplierID
           3,              -- CategoryID (e.g., Confections)
           '12-pack',
           5.99,
           0,              -- Out of stock
           50,             -- On order
           10,
           0
       );

-- Step 2: Get the new ProductID
DECLARE @WaffleProductID INT;
SELECT @WaffleProductID = ProductID
FROM dbo.Products
WHERE ProductName = 'Chocolate Waffles';

-- Step 3: Capture new Orders in a table variable
DECLARE @OrderIDs TABLE (OrderID INT);

-- Use static realistic 1997 dates
INSERT INTO dbo.Orders (
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
OUTPUT INSERTED.OrderID INTO @OrderIDs(OrderID)
VALUES
    ('ALFKI', 1, '1997-11-01', '1997-11-05', '1997-11-02', 1, 12.34, 'Alfred''s HQ', 'Str. 57', 'Berlin', NULL, '12209', 'Germany'),
    ('FRANK', 2, '1997-11-02', '1997-11-06', '1997-11-03', 2, 8.90, 'Frankenversand', 'Platz 43', 'Munich', NULL, '80331', 'Germany'),
    ('WILMK', 3, '1997-11-03', '1997-11-07', '1997-11-04', 3, 15.20, 'Wilman Kala', 'Keskuskatu 45', 'Helsinki', NULL, '00100', 'Finland');

-- Step 4: Insert OrderDetails for each order with quantity
DECLARE @Quantities TABLE (Quantity INT);
INSERT INTO @Quantities (Quantity) VALUES (200), (180), (220);

DECLARE @i INT = 1;
DECLARE @OrderID INT;
DECLARE @Quantity INT;

DECLARE order_cursor CURSOR FOR
    SELECT OrderID FROM @OrderIDs;

OPEN order_cursor;
FETCH NEXT FROM order_cursor INTO @OrderID;

WHILE @@FETCH_STATUS = 0
    BEGIN
        SELECT @Quantity = Quantity
        FROM (
                 SELECT Quantity, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn
                 FROM @Quantities
             ) AS q
        WHERE rn = @i;

        INSERT INTO dbo.OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount)
        VALUES (@OrderID, @WaffleProductID, 5.99, @Quantity, 0);

        SET @i = @i + 1;
        FETCH NEXT FROM order_cursor INTO @OrderID;
    END

CLOSE order_cursor;
DEALLOCATE order_cursor;

-- Step 5: Confirm UnitsInStock is 0
UPDATE dbo.Products
SET UnitsInStock = 0
WHERE ProductID = @WaffleProductID;