from dagster import job, op

from etl.ops.etl import extract_customer_details, load_customer_details, create_pdf

@job
def run_etl_job():
    """
    A job definition. This example job has a single op.

    For more hints on writing Dagster jobs, see our documentation overview on Jobs:
    https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs
    """
    df, tbl = extract_customer_details()
    create_pdf(df)
    load_customer_details(df,tbl)
