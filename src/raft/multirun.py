#!/Users/affe/PycharmProjects/pythonProject/venv/bin/python3
import itertools
import os
import pathlib
import shutil
import subprocess
import argparse
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED


from rich import print

parser = argparse.ArgumentParser(description='Parsing distributed log')

parser.add_argument('file', type=str, help='The log file', nargs='?')
parser.add_argument('-i', '--iterations', type=int, help='how many iterations we want to run each tests')
parser.add_argument('-w', '--workers', type=int, help='max workers of the ThreadPoolExecutor')
parser.add_argument('-t', '--test', type=str, help='which test to run')
parser.add_argument('-l', '--location', type=str, help='dir to store failed logs')
args = parser.parse_args()

iterations = args.iterations
workers = args.workers
test = args.test
location = args.location

single_tests = [
    "TestInitialElection2A",
    "TestReElection2A",
    "TestManyElections2A",
]

def run_test(test: str):
    test_cmd = ["go", "test", f"-run={test}", "-race"]
    f, tmp_file_path = tempfile.mkstemp()
    start = time.time()
    proc = subprocess.run(test_cmd, stdout=f, stderr=f)
    elapsed_time = time.time() - start
    os.close(f)
    return test, tmp_file_path, proc.returncode, elapsed_time


total = len(single_tests) * iterations
completed = 0
output = pathlib.Path(location)
tests = itertools.chain.from_iterable(itertools.repeat(single_tests, iterations))

with ThreadPoolExecutor(max_workers=workers) as executor:
    futures = []
    while completed < total:
        n = len(futures)
        # If there are fewer futures than workers assign them
        if n < workers:
            for test in itertools.islice(tests, workers - n):
                futures.append(executor.submit(run_test, test))

        # Wait until a task completes
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)

        for future in done:
            test, tmp_path, rc, elapsed_time = future.result()

            dest = (output / f"{test}_{completed}.log").as_posix()
            # If the test failed, save the output for later analysis
            if rc != 0:
                print(f"Failed test {test} - {dest}")
                output.mkdir(exist_ok=True, parents=True)
                shutil.copy(tmp_path, dest)

            os.remove(tmp_path)
            completed += 1
            futures = list(not_done)
