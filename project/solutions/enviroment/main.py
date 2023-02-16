import os


def run():
    user = os.getenv("BI_USER")
    return user


if __name__ == "__main__":
    result = run()
    print(result)
    print(os.getcwd())
    print(__file__)
