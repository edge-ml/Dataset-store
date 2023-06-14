import argparse
import glob
from tqdm import tqdm
from app.dataLoader.FileSystemDataLoader import FileSystemDataLoader
from app.dataLoader.S3DataLoader import S3DataLoader

# Parse command line arguments
parser = argparse.ArgumentParser(description='Process files and save data to S3')
parser.add_argument('--path', type=str, help='Path to the directory containing .bin files')
args = parser.parse_args()

# Check if the --path argument is provided
if not args.path:
    parser.error('Please provide the --path argument.')

# Retrieve the files in the specified directory
files = glob.glob(args.path + '/*.bin')

# Initialize data loaders
fs_dataloader = FileSystemDataLoader()
s3_dataloader = S3DataLoader()

# Process and save the data for each file
for f in tqdm(files):
    name = f.split("/")[-1].replace(".bin", "")
    print("Saving object", name)
    time, data = fs_dataloader.load_series(name)
    s3_dataloader.save_series(name, time, data)
