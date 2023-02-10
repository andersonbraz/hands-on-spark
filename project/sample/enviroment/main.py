import os

def run_my_example():
    user = os.getenv('BI_USER')
    return user
    
if __name__ == "__main__":
    result = run_my_example()
    print(result)
    print(os.getcwd())
    print(__file__)