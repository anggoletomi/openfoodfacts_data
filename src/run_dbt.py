import os
import subprocess
from dotenv import load_dotenv

def main():
    load_dotenv(".env")

    project_dir = os.path.join("dbt", "openfoodfacts_snowflake_dbt")
    profiles_dir = os.path.join("dbt", "openfoodfacts_snowflake_dbt")

    # 1) dbt run
    cmd_run = [
        "dbt", "run",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir
    ]
    process = subprocess.run(cmd_run)
    if process.returncode != 0:
        print("dbt run failed!")
        exit(process.returncode)

    # 2) dbt test
    cmd_test = [
        "dbt", "test",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir
    ]
    test_process = subprocess.run(cmd_test)
    if test_process.returncode != 0:
        print("dbt test failed!")
        exit(test_process.returncode)

    print("dbt run + test completed successfully.")

if __name__ == "__main__":
    main()