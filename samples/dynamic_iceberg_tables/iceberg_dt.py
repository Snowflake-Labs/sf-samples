"""
AWS Glue Job for Creating Iceberg Tables for Snowflake Webinar Demo
This script creates multiple Iceberg tables with 1-2MM rows each for demonstration purposes.
"""

import sys
import boto3
import random
from datetime import datetime, timedelta
from decimal import Decimal
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import uuid

# Simple data generators to replace Faker
class SimpleDataGenerator:
    def __init__(self):
        self.first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 
                           'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
                           'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
                           'Matthew', 'Betty', 'Anthony', 'Helen', 'Mark', 'Sandra', 'Donald', 'Donna']
        
        self.last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                          'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
                          'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
                          'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker']
        
        self.cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
                      'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
                      'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle',
                      'Denver', 'Washington', 'Boston', 'El Paso', 'Nashville', 'Detroit', 'Oklahoma City']
        
        self.states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN',
                      'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV',
                      'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN',
                      'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
        
        self.streets = ['Main St', 'Oak Ave', 'First St', 'Second St', 'Park Ave', 'Elm St', 'Washington St',
                       'Maple Ave', 'Cedar St', 'Pine St', 'Lake Ave', 'Hill St', 'Broadway', 'Church St']
        
        self.product_names = ['Pro', 'Ultra', 'Premium', 'Standard', 'Basic', 'Elite', 'Classic', 'Modern',
                             'Advanced', 'Essential', 'Deluxe', 'Superior', 'Master', 'Expert', 'Smart']
        
        self.product_types = ['Widget', 'Gadget', 'Device', 'Tool', 'Kit', 'Set', 'System', 'Unit',
                             'Component', 'Assembly', 'Module', 'Package', 'Bundle', 'Collection']
    
    def first_name(self):
        return random.choice(self.first_names)
    
    def last_name(self):
        return random.choice(self.last_names)
    
    def email(self, first_name=None, last_name=None):
        if not first_name:
            first_name = self.first_name()
        if not last_name:
            last_name = self.last_name()
        domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com']
        return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"
    
    def phone_number(self):
        area_code = random.randint(200, 999)
        exchange = random.randint(200, 999)
        number = random.randint(1000, 9999)
        return f"({area_code}) {exchange}-{number}"
    
    def date_of_birth(self, min_age=18, max_age=80):
        today = datetime.now()
        min_birth = today - timedelta(days=max_age * 365)
        max_birth = today - timedelta(days=min_age * 365)
        time_between = max_birth - min_birth
        days_between = time_between.days
        random_days = random.randrange(days_between)
        return min_birth + timedelta(days=random_days)
    
    def address(self):
        number = random.randint(1, 9999)
        street = random.choice(self.streets)
        return f"{number} {street}"
    
    def city(self):
        return random.choice(self.cities)
    
    def state_abbr(self):
        return random.choice(self.states)
    
    def zipcode(self):
        return f"{random.randint(10000, 99999)}"
    
    def catch_phrase(self):
        adj = random.choice(self.product_names)
        noun = random.choice(self.product_types)
        return f"{adj} {noun}"
    
    def text(self, max_chars=200):
        words = ['innovative', 'reliable', 'efficient', 'premium', 'quality', 'durable', 'advanced',
                'professional', 'versatile', 'powerful', 'compact', 'lightweight', 'ergonomic',
                'user-friendly', 'cost-effective', 'sustainable', 'cutting-edge', 'robust']
        
        text_words = random.choices(words, k=random.randint(10, 20))
        text = ' '.join(text_words)
        return text[:max_chars] if len(text) > max_chars else text
    
    def date_between(self, start_date, end_date):
        if isinstance(start_date, str):
            if start_date.startswith('-'):
                # Handle relative dates like '-5y', '-30d'
                if 'y' in start_date:
                    years = int(start_date.replace('-', '').replace('y', ''))
                    start_date = datetime.now() - timedelta(days=years * 365)
                elif 'd' in start_date:
                    days = int(start_date.replace('-', '').replace('d', ''))
                    start_date = datetime.now() - timedelta(days=days)
        
        if isinstance(end_date, str) and end_date == 'today':
            end_date = datetime.now()
        
        time_between = end_date - start_date
        days_between = time_between.days
        if days_between <= 0:
            days_between = 1
        random_days = random.randrange(days_between)
        return start_date + timedelta(days=random_days)
    
    def ean13(self):
        return ''.join([str(random.randint(0, 9)) for _ in range(13)])

# Initialize
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
DATABASE_NAME = 'bronze_analytics_db'
S3_BUCKET = '<s3_bucket>/glue-iceberg/'
TABLE_PREFIX = 'de'
NUM_CUSTOMERS = 500000  # 500K customers
NUM_PRODUCTS = 50000    # 50K products
NUM_ORDERS = 1500000    # 1.5M orders
NUM_ORDER_ITEMS = 3000000  # 3M order items (avg 2 items per order)

# Initialize data generator
fake = SimpleDataGenerator()
random.seed(42)  # For reproducible data

def create_customers_table():
    """Create customers table with realistic data"""
    print("Creating customers table...")
    
    # Generate customer data
    customers_data = []
    for i in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        customer = {
            'customer_id': i + 1,
            'customer_uuid': str(uuid.uuid4()),
            'first_name': first_name,
            'last_name': last_name,
            'email': fake.email(first_name, last_name),
            'phone': fake.phone_number(),
            'date_of_birth': fake.date_of_birth(min_age=18, max_age=80),
            'gender': random.choice(['M', 'F', 'O']),
            'address_line1': fake.address(),
            'address_line2': fake.address() if random.random() < 0.3 else None,
            'city': fake.city(),
            'state': fake.state_abbr(),
            'postal_code': fake.zipcode(),
            'country': 'US',
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
            'registration_date': fake.date_between(start_date='-5y', end_date='today'),
            'last_login_date': fake.date_between(start_date='-30d', end_date='today'),
            'total_orders': random.randint(0, 50),
            'total_spent': round(random.uniform(0, 10000), 2),
            'is_active': random.choices([True, False], weights=[0.8, 0.2])[0],
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        customers_data.append(customer)
        
        if (i + 1) % 50000 == 0:
            print(f"Generated {i + 1} customers...")
    
    # Create DataFrame
    customers_df = spark.createDataFrame(customers_data)
    
    # Write as Iceberg table using Spark SQL
    customers_df.createOrReplaceTempView("temp_customers")
    
    # Create Iceberg table using SQL with explicit location
    table_location = f"s3://{S3_BUCKET}/{TABLE_PREFIX}_customers/"
    spark.sql(f"""
        CREATE OR REPLACE TABLE glue_catalog.{DATABASE_NAME}.{TABLE_PREFIX}_customers
        USING ICEBERG
        LOCATION '{table_location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
        AS SELECT * FROM temp_customers
    """)
    
    print(f"Created customers table with {NUM_CUSTOMERS} rows")
    return customers_df

def create_products_table():
    """Create products table with realistic data"""
    print("Creating products table...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Beauty', 'Automotive', 'Toys']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'GlobalTech', 'StyleCorp', 'QualityPlus']
    
    products_data = []
    for i in range(NUM_PRODUCTS):
        category = random.choice(categories)
        brand = random.choice(brands)
        
        product = {
            'product_id': i + 1,
            'product_uuid': str(uuid.uuid4()),
            'product_name': f"{brand} {fake.catch_phrase()}",
            'description': fake.text(max_chars=200),
            'category': category,
            'subcategory': f"{category} Sub {random.randint(1, 5)}",
            'brand': brand,
            'sku': f"SKU{str(i+1).zfill(8)}",
            'barcode': fake.ean13(),
            'unit_price': round(random.uniform(5.99, 999.99), 2),
            'cost_price': round(random.uniform(2.99, 500.00), 2),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'dimensions_cm': f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}",
            'color': random.choice(['Red', 'Blue', 'Green', 'Black', 'White', 'Yellow', 'Purple', 'Orange']),
            'size': random.choice(['XS', 'S', 'M', 'L', 'XL', 'XXL', 'One Size']),
            'material': random.choice(['Cotton', 'Polyester', 'Plastic', 'Metal', 'Wood', 'Glass']),
            'stock_quantity': random.randint(0, 1000),
            'reorder_level': random.randint(10, 100),
            'supplier_id': random.randint(1, 100),
            'is_active': random.choices([True, False], weights=[0.9, 0.1])[0],
            'launch_date': fake.date_between(start_date='-3y', end_date='today'),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        products_data.append(product)
        
        if (i + 1) % 10000 == 0:
            print(f"Generated {i + 1} products...")
    
    # Create DataFrame
    products_df = spark.createDataFrame(products_data)
    
    # Write as Iceberg table using Spark SQL
    products_df.createOrReplaceTempView("temp_products")
    
    # Create Iceberg table using SQL with explicit location
    table_location = f"s3://{S3_BUCKET}/{TABLE_PREFIX}_products/"
    spark.sql(f"""
        CREATE OR REPLACE TABLE glue_catalog.{DATABASE_NAME}.{TABLE_PREFIX}_products
        USING ICEBERG
        LOCATION '{table_location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
        AS SELECT * FROM temp_products
    """)
    
    print(f"Created products table with {NUM_PRODUCTS} rows")
    return products_df

def create_orders_table():
    """Create orders table with realistic data"""
    print("Creating orders table...")
    
    orders_data = []
    start_date = datetime.now() - timedelta(days=730)  # 2 years of data
    
    for i in range(NUM_ORDERS):
        order_date = start_date + timedelta(
            days=random.randint(0, 729),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Shipping date is 1-7 days after order date
        shipping_date = order_date + timedelta(days=random.randint(1, 7))
        
        # Delivery date is 1-3 days after shipping
        delivery_date = shipping_date + timedelta(days=random.randint(1, 3))
        
        order = {
            'order_id': i + 1,
            'order_uuid': str(uuid.uuid4()),
            'customer_id': random.randint(1, NUM_CUSTOMERS),
            'order_date': order_date,
            'order_status': random.choices(['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'], 
                                        weights=[0.05, 0.1, 0.15, 0.65, 0.05])[0],
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer', 'Cash']),
            'payment_status': random.choices(['Paid', 'Pending', 'Failed', 'Refunded'], weights=[0.85, 0.05, 0.05, 0.05])[0],
            'shipping_method': random.choice(['Standard', 'Express', 'Overnight', 'Pickup']),
            'shipping_address': f"{fake.address()}, {fake.city()}, {fake.state_abbr()} {fake.zipcode()}",
            'billing_address': f"{fake.address()}, {fake.city()}, {fake.state_abbr()} {fake.zipcode()}",
            'subtotal': round(random.uniform(20.00, 2000.00), 2),
            'tax_amount': 0,  # Will calculate based on subtotal
            'shipping_cost': round(random.uniform(0, 25.00), 2),
            'discount_amount': round(random.uniform(0, 100.00), 2),
            'total_amount': 0,  # Will calculate
            'currency': 'USD',
            'notes': fake.text(max_chars=100) if random.random() < 0.2 else None,
            'shipping_date': shipping_date if random.random() < 0.8 else None,
            'delivery_date': delivery_date if random.random() < 0.7 else None,
            'created_at': order_date,
            'updated_at': order_date + timedelta(hours=random.randint(0, 48))
        }
        
        # Calculate tax (8.5% rate)
        order['tax_amount'] = round(order['subtotal'] * 0.085, 2)
        order['total_amount'] = round(
            order['subtotal'] + order['tax_amount'] + order['shipping_cost'] - order['discount_amount'], 2
        )
        
        orders_data.append(order)
        
        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1} orders...")
    
    # Create DataFrame
    orders_df = spark.createDataFrame(orders_data)
    
    # Write as Iceberg table using Spark SQL
    orders_df.createOrReplaceTempView("temp_orders")
    
    # Create Iceberg table using SQL with explicit location
    table_location = f"s3://{S3_BUCKET}/{TABLE_PREFIX}_orders/"
    spark.sql(f"""
        CREATE OR REPLACE TABLE glue_catalog.{DATABASE_NAME}.{TABLE_PREFIX}_orders
        USING ICEBERG
        LOCATION '{table_location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
        AS SELECT * FROM temp_orders
    """)
    
    print(f"Created orders table with {NUM_ORDERS} rows")
    return orders_df

def create_order_items_table():
    """Create order items table with realistic data"""
    print("Creating order items table...")
    
    order_items_data = []
    item_id = 1
    
    for order_id in range(1, NUM_ORDERS + 1):
        # Each order has 1-5 items (weighted towards 1-2 items)
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.4, 0.35, 0.15, 0.07, 0.03])[0]
        
        for item_seq in range(num_items):
            product_id = random.randint(1, NUM_PRODUCTS)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(5.99, 999.99), 2)
            
            order_item = {
                'order_item_id': item_id,
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': round(quantity * unit_price, 2),
                'discount_percent': random.choices([0, 5, 10, 15, 20], weights=[0.7, 0.1, 0.1, 0.05, 0.05])[0],
                'tax_rate': 0.085,
                'line_total': 0,  # Will calculate
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Calculate line total with discount
            discounted_total = order_item['total_price'] * (1 - order_item['discount_percent'] / 100)
            order_item['line_total'] = round(discounted_total, 2)
            
            order_items_data.append(order_item)
            item_id += 1
        
        if order_id % 100000 == 0:
            print(f"Generated order items for {order_id} orders...")
    
    # Create DataFrame
    order_items_df = spark.createDataFrame(order_items_data)
    
    # Write as Iceberg table using Spark SQL
    order_items_df.createOrReplaceTempView("temp_order_items")
    
    # Create Iceberg table using SQL with explicit location
    table_location = f"s3://{S3_BUCKET}/{TABLE_PREFIX}_order_items/"
    spark.sql(f"""
        CREATE OR REPLACE TABLE glue_catalog.{DATABASE_NAME}.{TABLE_PREFIX}_order_items
        USING ICEBERG
        LOCATION '{table_location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
        AS SELECT * FROM temp_order_items
    """)
    
    print(f"Created order items table with {len(order_items_data)} rows")
    return order_items_df

def main():
    """Main execution function"""
    print(f"Starting Iceberg table creation for webinar demo")
    print(f"Database: {DATABASE_NAME}")
    print(f"S3 Bucket: {S3_BUCKET}")
    print(f"Table Prefix: {TABLE_PREFIX}")
    
    # Iceberg configurations are now set via Glue job parameters
    print("Spark configurations for Iceberg are set via job parameters")
    
    # Set up Iceberg catalog
    try:
        # Use the glue_catalog for Iceberg tables
        spark.sql("USE CATALOG glue_catalog")
        print("Successfully using glue_catalog for Iceberg tables")
        
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{DATABASE_NAME}")
        print(f"Created/verified database: {DATABASE_NAME}")
        
    except Exception as e:
        print(f"Error setting up Iceberg catalog: {e}")
        # Try alternative approach
        print("Attempting alternative catalog setup...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        spark.sql(f"USE DATABASE {DATABASE_NAME}")
    
    # Create tables
    customers_df = create_customers_table()
    products_df = create_products_table()
    orders_df = create_orders_table()
    order_items_df = create_order_items_table()
    
    # Print summary
    print("\n" + "="*50)
    print("ICEBERG TABLES CREATION SUMMARY")
    print("="*50)
    print(f"Database: {DATABASE_NAME}")
    print(f"Tables created:")
    print(f"  - {TABLE_PREFIX}_customers: {customers_df.count():,} rows")
    print(f"  - {TABLE_PREFIX}_products: {products_df.count():,} rows") 
    print(f"  - {TABLE_PREFIX}_orders: {orders_df.count():,} rows")
    print(f"  - {TABLE_PREFIX}_order_items: {order_items_df.count():,} rows")
    print("="*50)
    
    # Show sample data
    print("\nSample data from customers table:")
    customers_df.show(5, truncate=False)
    
    print("\nSample data from products table:")
    products_df.show(5, truncate=False)
    
    print("\nSample data from orders table:")
    orders_df.show(5, truncate=False)
    
    print("\nSample data from order_items table:")
    order_items_df.show(5, truncate=False)

if __name__ == "__main__":
    main()
    job.commit()
