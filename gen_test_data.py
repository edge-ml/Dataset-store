import math

NUM_ROWS = 1000000
NUM_SENSORS = 10


with open("test_file_001.csv", "w") as f:
    header = ",".join(["time"] + [f"sensor_{i}" for i in range(NUM_SENSORS)])
    f.write(header + "\n")
    for i in range(NUM_ROWS):
        row = ",".join([str(i * 1000)] + [str(math.sin(i)) for _ in range(NUM_SENSORS)])
        f.write(row + "\n")