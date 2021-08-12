import subprocess
import json
import sys
import os
from cdk import names

if len(sys.argv) < 3 or len(sys.argv) > 4:
    print(
        "Usage: python gen_dashboard_template.py source_stack dest_stack [source_aws_profile]"
    )

INPUT_STACK_NAME = sys.argv[1]
OUTPUT_STACK_NAME = sys.argv[2]

profile_arg = ""
if len(sys.argv) == 4:
    profile_arg = f"--profile {sys.argv[3]}"

cmd = f"aws {profile_arg} cloudwatch get-dashboard --dashboard-name {INPUT_STACK_NAME}"
r = subprocess.run(cmd.split(), capture_output=True)

output = r.stdout.decode("ascii")
if not output:
    print(
        f"Dashboard for {INPUT_STACK_NAME} not found. Are you using the correct AWS profile?"
    )
    sys.exit(0)
body = json.loads(json.loads(r.stdout.decode("ascii"))["DashboardBody"])

with open("source_dashboard.json", "w") as f:
    f.write(json.dumps(body))

with open("source_dashboard.json", "r") as infile:
    with open(f"{OUTPUT_STACK_NAME}_dashboard.json", "w") as outfile:
        for line in infile.readlines():
            # fix function names
            for func in names.FUNCTIONS:
                if func in line:
                    old_name = f"{INPUT_STACK_NAME}-{func}-function"
                    new_name = f"{OUTPUT_STACK_NAME}-{func}"
                    new_name = new_name.replace("ingester", "ingest")
                    line = line.replace(old_name, new_name)
                    print(old_name, new_name)

            # fix all other resources
            line = line.replace(INPUT_STACK_NAME, OUTPUT_STACK_NAME)
            outfile.write(line)

os.remove("source_dashboard.json")
