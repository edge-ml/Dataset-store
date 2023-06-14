import boto3
import glob
from tqdm import tqdm
from app.dataLoader.FileSystemDataLoader import FileSystemDataLoader
from app.dataLoader.S3DataLoader import S3DataLoader

files = glob.glob("../TS_DATA/*.bin")

fs_dataloader = FileSystemDataLoader()
s3_dataloader = S3DataLoader()

for f in tqdm(files):
    name = f.split("/")[-1].replace(".bin", "")
    print("Saving object", name)
    time, data = fs_dataloader.load_series(name)
    s3_dataloader.save_series(name, time, data)