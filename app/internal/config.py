from starlette.config import Config


default_values = {
    "S3_URL": None,
    "S3_BUCKET_NAME": None,
    "S3_ACCESS_KEY": None,
    "S3_SECRET_KEY": None
}

config =  lambda x: None

def loadConfig(fileName):
    config = Config(f"envs/{fileName}.env")
    for (k, v) in config.file_values.items():
        globals()[k] = v

    for variable, default_value in default_values.items():
        if variable not in globals():
            globals()[variable] = default_value