from dagster import job, op, graph

@op
def return_fifty():
    return 50.0

@op
def log_number_received(context, number):
    context.log.info(f"number received: {number}")
    return number

@op
def multiply_by_one_point_eight(number):
    return number * 1.8

@op
def add_thirty_two(number):
    return number + 32.0

@op
def log_number_sent(context, number):
    context.log.info(f"number sent: {number}")

@graph#(tags={"dagster/max_retries": 3})
def celsius_to_fahrenheit(number):
    return add_thirty_two(multiply_by_one_point_eight(log_number_received(number)))


@job(tags={"dagster/max_retries": 4})
def all_together_nested():
    log_number_sent(celsius_to_fahrenheit(return_fifty()))

