from prefect import flow, task, get_run_logger
from sympy import isprime
import requests

@task(retries=3, retry_delay_seconds=2, log_prints=True)
def get_number():
    num = requests.get("http://ids-sds.pods.uvarc.io/int/4")
    num = num.json()
    num = int(num['id'])
    print(f"Fetched number: {num}")
    logger = get_run_logger()
    logger.info(f"Fetched number: {num}")
    return(num)

@task
def determine_prime(number):
    if isprime(number):
        logger = get_run_logger()
        logger.info(f"{number} is prime")
    else:
        logger = get_run_logger()
        logger.info(f"{number} is not prime")

@flow
def determine_prime_flow():
    number = get_number()
    determine_prime(number)

if __name__ == "__main__":
    determine_prime_flow.serve(name="scheduled-prime", cron="* * * * *") #run every minute of every hour of every day..