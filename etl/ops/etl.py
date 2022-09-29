from multiprocessing import context
from dagster import Out, Output, job, op
import logging
from etl.db_con import get_sql_conn, get_postgres_creds

#import needed libraries
from sqlalchemy import create_engine
import pyodbc
from fpdf import FPDF
import pandas as pd
import os



#extract data from sql server
@op(out={"df": Out(is_required=True), "tbl": Out(is_required=True)})
def extract_customer_details(context):
    try:
        lgr = logging.getLogger('console_logger')
        # get password from environmnet var
        with get_sql_conn() as conn:
            #if conn.open:
                #my_logger.error("sql connection is open")
            src_cursor = conn.cursor()
            # execute query
            src_cursor.execute(""" select  t.name as table_name
            from sys.tables t where t.name in ('tblCustomer') """)
            src_tables = src_cursor.fetchall()
            for tbl in src_tables:
                #query and load save data to dataframe
                df = pd.read_sql_query(f'select * FROM {tbl[0]}', conn)
                #logging table name
                context.log.info("table name " + str(tbl[0]))
                context.log.info(df.head())
                lgr.error("table name " + str(tbl[0]) )
                lgr.error(df)

                yield Output(df, "df")
                yield Output(tbl[0], "tbl")

            #load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))

#Create pdf to of extracted dataframe
@op
def create_pdf(context, df):
    try:
        lgr = logging.getLogger("console_logger")
        for index, row in df.iterrows():
            #create FPDF object
            pdf = FPDF('P', 'mm', 'A4')
            context.log.info("pdf created!!!")
            lgr.error("pdf not created!!!")
            #Add a page
            pdf.add_page()
            #specify font
            pdf.set_font('helvetica', '', 16)
            #create obj for each row in table
            idx = str(row['CustomerID'])
            name = str(row['Name'])
            spent = str(row['Spent'])
            cashback = str(row['Cashback'])
            pdf.cell(150, 10, f'Hello {name}!')
            pdf.cell(80, 10, 'Greetings', ln=1)
            pdf.cell(0, 10, f'Total money spent uptill now: {spent}', ln=1)
            pdf.cell(0, 10, f'Total Cashback availed: {cashback}', ln=1)

            pdf.output(f'pdf_{idx}.pdf')
    except Exception as e:
        print("Pdf creation error: " + str(e))
        lgr.error(str(e))       


#load data to postgres
@op
def load_customer_details(context, df, tbl):
    try:
        lgr = logging.getLogger("console_logger")
        rows_imported = 0
        # print info and errors
        lgr.error("table received name " + str(tbl))
        context.log.info("table received name " + str(tbl))
        context.log.info(df.head())
        lgr.error(df.head())
        lgr.error(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        engine = get_postgres_creds()
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False, schema="public")
        rows_imported += len(df)
        # print success message
        context.log.info("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))
        lgr.error(str(e))