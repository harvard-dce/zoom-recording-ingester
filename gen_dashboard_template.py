import site
from os.path import join, dirname
import subprocess
import json
import sys

site.addsitedir(join(dirname(dirname(__file__)), "cdk"))
from cdk import names

if len(sys.argv) != 3:
    print("Usage: python gen_dashboard_template.py source_stack dest_stack")

COPY_FROM_STACK_NAME = sys.argv[1]
GEN_FOR_STACK_NAME = sys.argv[2]

cmd = f"aws --profile prod cloudwatch get-dashboard --dashboard-name {COPY_FROM_STACK_NAME}"
r = subprocess.run(cmd.split(), capture_output=True)

body = json.loads(json.loads(r.stdout.decode("ascii"))["DashboardBody"])

with open("source_dashboard.json", "w") as f:
    f.write(json.dumps(body))

with open("source_dashboard.json", "r") as infile:
    with open("output_dashboard.json", "w") as outfile:
        for line in infile.readlines():
            # fix function names
            for func in names.FUNCTIONS:
                if func in line:
                    old_name = f"{COPY_FROM_STACK_NAME}-{func}-function"
                    new_name = f"{GEN_FOR_STACK_NAME}-{func}"
                    new_name = new_name.replace("ingester", "ingest")
                    line = line.replace(old_name, new_name)
                    print(old_name, new_name)

            # fix all other resources
            line = line.replace(COPY_FROM_STACK_NAME, GEN_FOR_STACK_NAME)
            outfile.write(line)
