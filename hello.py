from prefect import flow, task

@task
def create_message():
    msg = "Hello, world!"
    return(msg)

@task
def create_another():
    msg2 = "Good to see you"
    return(msg2)

@flow
def hello_world():
    task_message = create_message()
    second_message = create_another()
    print(task_message)
    print(second_message)

if __name__ == "__main__":
    hello_world()