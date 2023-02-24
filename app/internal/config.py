from starlette.config import Config

config =  lambda x: None

def loadConfig(fileName):
    config = Config(f"envs/{fileName}.env")
    for (k, v) in config.file_values.items():
        globals()[k] = v
        print(k, v)