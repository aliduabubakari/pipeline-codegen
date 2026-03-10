from dagster import op, job

@op
def extract():
    print('task extract')

@op
def load():
    print('task load')

@job
def ok_sequential():
    extract()
    load()