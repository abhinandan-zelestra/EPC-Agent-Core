import time


def invoke_with_retry(func, retries=3):

    for i in range(retries):

        try:
            return func()

        except Exception as e:

            if i == retries - 1:
                raise

            time.sleep(2 ** i)