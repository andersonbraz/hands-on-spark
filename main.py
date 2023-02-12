# from project.samples.minio.main import run_my_example
# from project.samples.join.main import run_my_example
# from project.samples.enviroment.main import run_my_example
from project.samples.pipeline.main import MyPipeline



if __name__ == "__main__":
    pipe = MyPipeline()
    input = pipe.extract()
    result = pipe.transform(input)
    print(result)
