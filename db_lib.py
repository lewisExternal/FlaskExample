import sqlite3
from sqlite3 import Error
import os.path
import pandas as pd 

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def select_all_records(conn,table_name):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :param table_name: the table string 
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + table_name)

    rows = cur.fetchall()

    return rows

def sql_report(conn,date):
    """
    Return specific report 
    :param conn: the Connection object
    :param date: the date for the report to be ran 
    :return: tuple to be used for the result 
    """

    sql_report_query1 = """     SELECT date(o.created_at) as date 
                                ,SUM(ol.quantity) as items
                                ,COUNT(DISTINCT o.customer_id) as customers
                                ,ROUND((SUM(ol.full_price_amount) - SUM(ol.discounted_amount)),2) as total_discount_amount
                                ,ROUND(AVG(ol.discount_rate),2) as discount_rate_avg
                                ,ROUND(SUM(olAgg.order_total_amount) / SUM(olAgg.order_count),2) as order_total_avg
                                ,ROUND(SUM(c.rate*ol.discounted_amount),2) as commissions_total 
                                ,ROUND((SUM(c.rate*ol.discounted_amount) / SUM(olAgg.order_count)),2) as commissions_order_average 
                                ,ROUND((SUM(CASE WHEN PP.promotion_id = 1 THEN c.rate*ol.discounted_amount ELSE 0 END) / SUM(CASE WHEN PP.promotion_id = 1 THEN 1 ELSE 0 END)),2) as promotions_1
                                ,ROUND((SUM(CASE WHEN PP.promotion_id = 2 THEN c.rate*ol.discounted_amount ELSE 0 END) / SUM(CASE WHEN PP.promotion_id = 2 THEN 1 ELSE 0 END)),2) as promotions_2
                                ,ROUND((SUM(CASE WHEN PP.promotion_id = 3 THEN c.rate*ol.discounted_amount ELSE 0 END) / SUM(CASE WHEN PP.promotion_id = 3 THEN 1 ELSE 0 END)),2) as promotions_3
                                ,ROUND((SUM(CASE WHEN PP.promotion_id = 4 THEN c.rate*ol.discounted_amount ELSE 0 END) / SUM(CASE WHEN PP.promotion_id = 4 THEN 1 ELSE 0 END)),2) as promotions_4
                                ,ROUND((SUM(CASE WHEN PP.promotion_id = 5 THEN c.rate*ol.discounted_amount ELSE 0 END) / SUM(CASE WHEN PP.promotion_id = 5 THEN 1 ELSE 0 END)),2) as promotions_5
                                FROM orders as o 
                                INNER JOIN order_lines as ol 
                                ON o.id = ol.order_id
                                INNER JOIN (
                                                SELECT order_id
                                                ,SUM(total_amount) as order_total_amount
                                                ,1 as order_count
                                                FROM order_lines
                                                GROUP BY order_id

                                            ) as olAgg 
                                ON o.id = olAgg.order_id
                                LEFT JOIN commissions as  c
                                ON date(o.created_at) = date(c.date)
                                AND o.vendor_id = c.vendor_id
                                LEFT JOIN product_promotions as pp
                                ON date(o.created_at) = date(pp.date)
                                AND ol.product_id = pp.product_id
                                WHERE date(o.created_at) = '""" + date + """'
                                GROUP BY date(o.created_at);
                       """ 

    cur1 = conn.cursor()
    cur1.execute(sql_report_query1)
    rows = cur1.fetchall()
    #cols = [description[0] for description in cur1.description]

    return rows[0]

def import_data_from_csv(conn,table_name):
    """
    Import data from provided CSVs 
    :param conn: the Connection object
    :param table_name: the table string 
    :return:
    """

    df = pd.read_csv(table_name+'.csv')
    df.to_sql(table_name, conn, if_exists='replace', index=False)


def main():

    # create db name 
    database = r"pythonsqlite.db"

    # define database table schema 
    
    sql_create_commissions_table = """ CREATE TABLE IF NOT EXISTS commissions (
                                        date text NOT NULL,
                                        vendor_id integer NOT NULL,
                                        rate DOUBLE NOT NULL
                                    ); """

    sql_create_products_table = """ CREATE TABLE IF NOT EXISTS products (
                                    id integer PRIMARY KEY,
                                    description text NOT NULL
                                );"""
    
    sql_create_product_promotions_table = """ CREATE TABLE IF NOT EXISTS product_promotions (
                                    date text NOT NULL,
                                    product_id integer NOT NULL,
                                    promotion_id integer NOT NULL
                                );"""

    sql_create_orders_table = """ CREATE TABLE IF NOT EXISTS orders (
                                    id integer PRIMARY KEY,
                                    created_at text NOT NULL,
                                    vendor_id integer NOT NULL,
                                    customer_id integer NOT NULL
                                );"""
    
    sql_create_order_lines_table = """ CREATE TABLE IF NOT EXISTS order_lines (
                                    order_id integer PRIMARY KEY,
                                    product_id integer NOT NULL,
                                    product_description text NOT NULL,
                                    product_price integer NOT NULL,
                                    product_vat_rate DOUBLE NOT NULL,
                                    discount_rate DOUBLE NOT NULL,
                                    quantity integer NOT NULL,
                                    full_price_amount integer NOT NULL,
                                    discounted_amount DOUBLE NOT NULL,
                                    vat_amount DOUBLE NOT NULL,
                                    total_amount DOUBLE NOT NULL
                                );"""  

    sql_create_promotions_table = """ CREATE TABLE IF NOT EXISTS promotions (
                                    id integer PRIMARY KEY,
                                    description text NOT NULL
                                );"""   


    # create a database connection
    conn = create_connection(database)

    if conn is not None:
        
        # create tables
        create_table(conn, sql_create_commissions_table)
        create_table(conn, sql_create_products_table)
        create_table(conn, sql_create_product_promotions_table)
        create_table(conn, sql_create_orders_table)
        create_table(conn, sql_create_order_lines_table)
        create_table(conn, sql_create_promotions_table)

        # import data from csv 
        try:
            import_data_from_csv(conn,'commissions')
            import_data_from_csv(conn,'products')
            import_data_from_csv(conn,'product_promotions')
            import_data_from_csv(conn,'orders')
            import_data_from_csv(conn,'order_lines')
            import_data_from_csv(conn,'promotions')

        except Exception as e:
            print("Data import has failed with the following error: "+str(e))  

    else:
        print("Error! cannot create the database connection.")

    
if __name__ == '__main__':
    main()