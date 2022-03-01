--Creacion de la dimension Category
CREATE TABLE "dim_category"
(
    "Id" integer NOT NULL,
    "CategoryName" character varying(14) NOT NULL,
    "Description" character varying(58) NOT NULL,
    CONSTRAINT dim_category_pkey PRIMARY KEY ("Id")
)

--Creacion de la dimension Customer
CREATE TABLE "dim_customer"
(
    "Id" character varying(5) NOT NULL,
    "CompanyName" character varying(36) NOT NULL,
    "ContactName" character varying(23) NOT NULL,
    "ContactTitle" character varying(30) NOT NULL,
    "Address" character varying(46) NOT NULL,
    "City" character varying(15) NOT NULL,
    "Region" character varying(15) NOT NULL,
    "PostalCode" character varying(9) NOT NULL,
    "Country" character varying(11) NOT NULL,
    "Phone" character varying(17) NOT NULL,
    "Fax" character varying(17) NOT NULL,
    CONSTRAINT dim_customer_pkey PRIMARY KEY ("Id")
)


--Creacion de la dimension Employee
CREATE TABLE "dim_employee"
(
    "Id" integer NOT NULL,
    "LastName" character varying(9) NOT NULL,
    "FirstName" character varying(8)  NOT NULL,
    "Title" character varying(24) NOT NULL,
    "TitleOfCourtesy" character varying(4) NOT NULL,
    "BirthDate" date NOT NULL,
    "HireDate" date NOT NULL,
    "Address" character varying(29) NOT NULL,
    "City" character varying(8) NOT NULL,
    "Region" character varying(13) NOT NULL,
    "PostalCode" character varying(7) NOT NULL,
    "Country" character varying(3) NOT NULL,
    "HomePhone" character varying(14) NOT NULL,
    "Extension" integer NOT NULL,
    "Photo" character varying(36) NOT NULL,
    "Notes" character varying(448) NOT NULL,
    "PhotoPath" character varying(38) NOT NULL,
    CONSTRAINT dim_employee_pkey PRIMARY KEY ("Id")
)

--Creacion de la dimension Location
CREATE TABLE "dim_location"
(
    "Id" integer NOT NULL,
    "ShipName" character varying(200) NOT NULL,
    "ShipAddress" character varying(200) NOT NULL,
    "ShipCity" character varying(200) NOT NULL,
    "ShipRegion" character varying(100) NOT NULL,
    "ShipPostalCode" character varying(100) NOT NULL,
    "ShipCountry" character varying(100) NOT NULL,
    CONSTRAINT dim_location_pkey PRIMARY KEY ("Id")
)

--Creacion de la dimension Product
CREATE TABLE "dim_product"
(
    "Id" integer NOT NULL,
    "ProductName" character varying(32) NOT NULL,
    "QuantityPerUnit" character varying(20) NOT NULL,
    "UnitPrice" numeric(5,2) NOT NULL,
    "UnitsInStock" integer NOT NULL,
    "UnitsOnOrder" integer NOT NULL,
    "ReorderLevel" integer NOT NULL,
    "Discontinued" character varying(3) NOT NULL,
    CONSTRAINT dim_product_pkey PRIMARY KEY ("Id")
)


--Creacion de la dimension Tiempo
CREATE TABLE dim_time
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

--Creacion de la dimension Shipper
CREATE TABLE "dim_shipper"
(
    "Id" integer NOT NULL,
    "CompanyName" character varying(16) NOT NULL,
    "Phone" character varying(14) NOT NULL,
    CONSTRAINT dim_shipper_pkey PRIMARY KEY ("Id")
)

--Creacion de la dimension Supplier
CREATE TABLE "dim_supplier"
(
    "Id" integer NOT NULL,
    "CompanyName" character varying(38) NOT NULL,
    "ContactName" character varying(26) NOT NULL,
    "ContactTitle" character varying(28) NOT NULL,
    "Address" character varying(45) NOT NULL,
    "City" character varying(13) NOT NULL,
    "Region" character varying(15) NOT NULL,
    "PostalCode" character varying(8) NOT NULL,
    "Country" character varying(11) NOT NULL,
    "Phone" character varying(15) NOT NULL,
    "Fax" character varying(15) NOT NULL,
    "HomePage" character varying(94) NOT NULL,
    CONSTRAINT dim_supplier_pkey PRIMARY KEY ("Id")
)

--Creacion de la tabla de hechos 
CREATE TABLE "fact_orders"
(
    "Id" character varying(10) NOT NULL,
    "OrderId" integer NOT NULL,
    "ProductId" integer NOT NULL,
    "UnitPrice" numeric(5,2) NOT NULL,
    "Quatity" integer NOT NULL,
    "Discount" numeric(5,2) NOT NULL,
    "CustomerId" character varying(10) NOT NULL,
    "EmployeeId" integer NOT NULL,
    CONSTRAINT fact_orders_pkey PRIMARY KEY ("Id")
)


--//////////////////////////////////////////////////////////////////////////////////////////////////////////
--Generacion de datos de la dimension tiempo: fechas desde el 2010/01/01 hasta 10 anios despues de la ultima factura.
INSERT INTO dim_time
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '2010-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 5477) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;

--Relaciones de la tabla de hechos
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_product FOREIGN KEY ("ProductId")
    REFERENCES public.dim_product ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_customer FOREIGN KEY ("CustomerId")
    REFERENCES public.dim_customer ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_employee FOREIGN KEY ("EmployeeId")
    REFERENCES public.dim_employee ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_category FOREIGN KEY ("CategoryId")
    REFERENCES public.dim_category ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_supplier FOREIGN KEY ("SupplierId")
    REFERENCES public.dim_supplier ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_orderdate FOREIGN KEY ("OrderDate")
    REFERENCES public.dim_time (date_dim_id)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_requireddate FOREIGN KEY ("RequiredDate")
    REFERENCES public.dim_time (date_dim_id)
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_shippeddate FOREIGN KEY ("ShippedDate")
    REFERENCES public.dim_category ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_shipper FOREIGN KEY ("ShipperId")
    REFERENCES public.dim_shipper ("Id")
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;

--eliminar datos
truncate table dim_category;
truncate table dim_customer;
truncate table dim_employee;
truncate table dim_location;
truncate table dim_product;
truncate table dim_shipper;
truncate table dim_supplier;
truncate table fact_orders;


ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_category FOREIGN KEY ("CategoryId")
    REFERENCES public.dim_category ("Id") MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID,
	
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_customer FOREIGN KEY ("CustomerId")
        REFERENCES public.dim_customer ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
		
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_employee FOREIGN KEY ("EmployeeId")
        REFERENCES public.dim_employee ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
		
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_location FOREIGN KEY ("LocationId")
        REFERENCES public.dim_location ("ShipAddress") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
		
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_orderdate FOREIGN KEY ("OrderDate")
        REFERENCES public.dim_time (date_dim_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
		
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_product FOREIGN KEY ("ProductId")
        REFERENCES public.dim_product ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_requireddate FOREIGN KEY ("RequiredDate")
        REFERENCES public.dim_time (date_dim_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,

ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_shippeddate FOREIGN KEY ("ShippedDate")
        REFERENCES public.dim_time (date_dim_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
		
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_shipper FOREIGN KEY ("ShipperId")
        REFERENCES public.dim_shipper ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
		
ALTER TABLE public.fact_orders
    ADD CONSTRAINT fk_supplier FOREIGN KEY ("SupplierId")
        REFERENCES public.dim_supplier ("Id") MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID