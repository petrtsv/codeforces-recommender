import requests
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import gzip


import problem_page_parser

DATA_DIR = Path("data")
STATEMENTS_DIR = DATA_DIR / "statements"


def load_problems_metadata():
    contests = requests.get("https://codeforces.com/api/contest.list?gym=false").json()['result'] + \
               requests.get("https://codeforces.com/api/contest.list?gym=true").json()['result']

    contests_df = pd.DataFrame.from_records(contests)[['id', 'type', 'name']]
    contests_df['div1'] = contests_df['name'].str.contains('Div. 1')
    contests_df['div2'] = contests_df['name'].str.contains('Div. 2')
    contests_df['div3'] = contests_df['name'].str.contains('Div. 3')
    contests_df = contests_df.add_prefix("contest_")
    contests_df = contests_df.set_index('contest_id')
    contests_df.to_csv(DATA_DIR / "contests.csv", header=True)

    problems = requests.get("https://codeforces.com/api/problemset.problems").json()['result']['problems']
    problems_df = pd.DataFrame.from_records(problems)
    problems_df = problems_df.join(contests_df, on='contestId')
    problems_df.to_csv(DATA_DIR / "problems.csv", header=True, index_label='problem_id')


def load_problem_statements():
    STATEMENTS_DIR.mkdir(exist_ok=True)
    problems_df = pd.read_csv(DATA_DIR / "problems.csv", index_col='problem_id')
    for problem_id, problem_info in tqdm(list(problems_df.iterrows())):
        contest_id = problem_info['contestId']
        index = problem_info['index']
        filename = STATEMENTS_DIR / ("%d.gz" % (int(problem_id),))
        if filename.exists():
            continue
        problem_page_url = "https://codeforces.com/contest/%d/problem/%s" % (contest_id, index)
        raw_page = requests.get(problem_page_url).text
        try:
            statement = problem_page_parser.parse_problem(raw_page)['statement']
            with gzip.open(filename, mode='wt', encoding="utf-8") as output_file:
                print(statement, file=output_file)
        except:
            continue


def main():
    DATA_DIR.mkdir(exist_ok=True)
    print("Loading problems metadata...")
    load_problems_metadata()
    print("Done")
    print("Loading problem statements...")
    load_problem_statements()
    print("Done")


if __name__ == '__main__':
    main()
