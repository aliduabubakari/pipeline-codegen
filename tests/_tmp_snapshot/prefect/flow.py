from prefect import flow, task

@task
def extract():
    print('task extract')

@task
def load():
    print('task load')

@flow
def ok_sequential():
    extract()
    load()

if __name__ == '__main__':
    ok_sequential()