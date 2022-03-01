import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from datetime import datetime
import psycopg2

def log(logfile, message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(logfile,"a") as f: 
        f.write('[' + timestamp + ']: ' + message + '\n')
        print(message)

def transform():

    log(logfile, "-------------------------------------------------------------")
    log(logfile, "Inicia Fase De Transformacion")
    df_category = pd.read_sql_query("""SELECT 
        cat.Id, 
        cat.CategoryName, 
        cat.Description 
        FROM Category cat;
        """, con=engine.connect())

    df_emp_territories = pd.read_sql_query("""SELECT 
        et.Id, 
        et.EmployeeId, 
        t.TerritoryDescription, 
        t.RegionId 
        FROM EmployeeTerritory et
        INNER JOIN Territory t ON t.Id = et.TerritoryId;
        """, con=engine.connect())

    df_emp_region = pd.read_sql_query("""SELECT r.Id, r.RegionDescription FROM "Region" r;
        """, con=engine.connect())

       
    df_customer = pd.read_sql_query("""SELECT 
        cust.Id, 
        cust.CompanyName, 
        cust.ContactName, 
        cust.ContactTitle, 
        cust.Address, 
        cust.City, 
        cust.Region, 
        COALESCE(cust.PostalCode, 'N/A') as PostalCode, 
        cust.Country, 
        cust.Phone, 
        COALESCE(cust.Fax, 'N/A') as Fax 
        FROM Customer cust;
        """, con=engine.connect())

    df_employee = pd.read_sql_query("""SELECT 
        emp.Id, 
        emp.LastName, 
        emp.FirstName, 
        emp.Title, 
        emp.TitleOfCourtesy, 
        emp.BirthDate, 
        emp.HireDate, 
        emp.Address, 
        emp.City, 
        emp.Region, 
        emp.PostalCode, 
        emp.Country, 
        emp.HomePhone,
        emp.Extension, 
        COALESCE(emp.Photo, 'N/A') as Photo, 
        emp.Notes, 
        emp.PhotoPath
        FROM Employee emp;
        """, con=engine.connect())

    df_location = pd.read_sql_query("""SELECT DISTINCT
        ord.ShipAddress,
        ord.ShipName, 
        ord.ShipCity, 
        ord.ShipRegion, 
        COALESCE(ord.shipPostalCode, 'N/A') as ShipPostalCode, 
        ord.ShipCountry
        FROM "Order" ord
        ORDER BY ShipName;
        """, con=engine.connect())

    df_product = pd.read_sql_query("""SELECT 
        prod.Id, 
        prod.ProductName, 
        prod.QuantityPerUnit, 
        prod.UnitPrice,
        prod.UnitsInStock,
        prod.UnitsOnOrder,
        prod.ReorderLevel, 
        CASE prod.Discontinued
            WHEN 0
            THEN "NO"
            WHEN 1
            THEN "SI"
            END AS Discontinued
        FROM Product prod;
        """, con=engine.connect())

    df_shipper = pd.read_sql_query("""SELECT 
        ship.Id, 
        ship.CompanyName, 
        ship.Phone
        FROM Shipper ship;
        """, con=engine.connect())

    df_supplier = pd.read_sql_query("""SELECT 
        sup.Id, 
        sup.CompanyName, 
        sup.ContactName, 
        sup.ContactTitle, 
        sup.Address, 
        sup.City, 
        sup.Region, 
        sup.PostalCode, 
        sup.Country, 
        sup.Phone, 
        COALESCE(sup.Fax, 'N/A') as Fax, 
        COALESCE(sup.HomePage, 'N/A') as HomePage
        FROM Supplier sup;
        """, con=engine.connect())

    df_fact_order = pd.read_sql_query("""SELECT 
        od.Id, 
        od.OrderId, 
        prod.Id as ProductId, 
        od.UnitPrice, 
        od.Quantity, 
        od.Discount, 
        ord.CustomerId, 
        ord.EmployeeId,
        cat.Id as CategoryId,
        prod.SupplierId,
        ord.ShipAddress as LocationId,
        strftime('%Y%m%d', datetime(ord.OrderDate)) as OrderDate,
        strftime('%Y%m%d', datetime(ord.RequiredDate)) as RequiredDate,
        COALESCE(strftime('%Y%m%d', datetime(ord.ShippedDate)),'0') as ShippedDate,
        ship.Id as ShipperId
        FROM OrderDetail od
        INNER JOIN "Order" ord ON ord.Id = od.OrderId
        INNER JOIN Shipper ship ON ship.Id = ord.ShipVia
        INNER JOIN Product prod ON od.ProductId = prod.Id
        INNER JOIN Category cat ON cat.Id = prod.CategoryId;
        """, con=engine.connect())

    log(logfile, "Finaliza Fase De Transformacion")
    log(logfile, "-------------------------------------------------------------")
    return df_fact_order,df_category,df_customer,df_employee,df_location,df_product,df_shipper,df_supplier, df_emp_territories, df_emp_region
   
def load():
    """ Connect to the PostgreSQL database server """
    conn_string = 'postgresql://postgres:172164@localhost/DW_Northwind_Snowflake'
    db = create_engine(conn_string)
    conn = db.connect()
    try:
        log(logfile, "-------------------------------------------------------------")
        log(logfile, "Inicia Fase De Carga")
        df_customer.to_sql('dim_customer', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_customer")
        df_employee.to_sql('dim_employee', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_employee")
        df_location.to_sql('dim_location', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_location")
        df_supplier.to_sql('dim_supplier', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_supplier")
        df_category.to_sql('dim_category', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_category")
        df_product.to_sql('dim_product', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_product")
        df_shipper.to_sql('dim_shipper', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_shipper")
        df_fact_order.to_sql('fact_orders', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a fact_orders")
        df_emp_territories.to_sql('dim_emp_territories', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_emp_territories")
        df_emp_region.to_sql('dim_emp_region', conn, if_exists='append',index=False)
        log(logfile, "Carga de datos a dim_emp_region")
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        log(logfile, "Finaliza Fase De Carga")
        log(logfile, "-------------------------------------------------------------")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally: 
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def extract():
    log(logfile, "--------------------------------------------------------")
    log(logfile, "Inicia Fase De Extraccion")
    metadata = MetaData()
    metadata.create_all(engine)
    log(logfile, "Finaliza Fase De Extraccion")
    log(logfile, "--------------------------------------------------------")


if __name__ == '__main__':
    
    logfile = "ProyectoETL_logfile_snowflake.txt"
    log(logfile, "ETL Trabajo iniciado.")
    engine = create_engine('sqlite:///Northwind_large.sqlite')
    extract()
    ( df_fact_order,df_category,df_customer,df_employee,df_location,df_product,df_shipper,df_supplier, df_emp_territories, df_emp_region) = transform()
    load()
    log(logfile, "ETL Trabajo finalizado.")
