#!/Users/affe/PycharmProjects/pythonProject/venv/bin/python3
import os
import sys
import argparse

from rich.console import Console
from rich.columns import Columns
from rich import print

TOPICS = {
    "ERRO": "red",
    "INFO": "bright_white",
    "TIMR": "bright_black",
    "ROLE": "bright_red",
    "CAND": "purple",
    "LOCK": "cyan",
    "VOTE": "green",
}

topics = {
    "ERRO",
    "INFO",
    "TIMR",
    "ROLE",
    "CAND",
    "LOCK",
    "VOTE"
}


parser = argparse.ArgumentParser(description='Parsing distributed log')

parser.add_argument('file', type=str, help='The log file', nargs='?')
parser.add_argument('-j', '--just', type=str, nargs='+', help='the topics we want to subscribe')
parser.add_argument('-e', '--exclude', type=str, nargs='+', help='the topics we want to exclude')
parser.add_argument('-c', '--columns', type=int, help='the columns we want to print', default=5)
args = parser.parse_args()

# initialize args
file = args.file
just = args.just
ignore = args.exclude
n_columns = args.columns

lines = []
if file is not None:
    f = open(file, 'r')
    lines = f.readlines()
    f.close()

input_ = lines if lines else sys.stdin
# Print just some topics or exclude some topics
if just:
    topics = just
if ignore:
    topics = [lvl for lvl in topics if lvl not in set(ignore)]

topics = set(topics)
console = Console()
width = console.size.width
colorize = True

panic = False
for line in input_:
    try:
        # Assume format from Go output
        if not line[0].isnumeric():
            print(line)
            continue
        time = int(line[:6])
        topic = line[7:11]
        msg = line[12:].strip()
        # To ignore some topics
        if topic not in topics:
            continue

        # Debug() calls from the test suite aren't associated with
        # any particular peer. Otherwise we can treat second column
        # as peer id
        if topic != "TEST" and n_columns:
            i = int(msg[1])
            peer_id = msg[1]
            msg = msg[3:]

        # Colorize output by using rich syntax when needed
        if colorize and topic in TOPICS:
            color = TOPICS[topic]
            msg = f"[{color}]S{peer_id} {msg}[/{color}]"

        # Single column. Always the case for debug calls in tests
        if n_columns is None or topic == "TEST":
            print(time, msg)
        # Multi column printing, timing is dropped to maximize horizontal
        # space. Heavylifting is done through rich.column.Columns object
        else:
            cols = ["" for _ in range(n_columns)]
            msg = "" + msg
            cols[i] = msg
            col_width = int(width / n_columns)
            cols = Columns(cols, width=col_width - 1,
                           equal=True, expand=True)
            print(cols)
    except:
        # Code from tests or panics does not follow format
        # so we print it as is
        if line.startswith("panic"):
            panic = True
        # Output from tests is usually important so add a
        # horizontal line with hashes to make it more obvious
        if not panic:
            print("-" * console.width)
        print(line, end="")