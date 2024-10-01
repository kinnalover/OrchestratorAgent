# TODO here we should handle running the tasks
import asyncio
import os
from pathlib import Path
import subprocess
import sys
import time
from rpa.lib import logger as lib_logger
from rpa.lib import rdbms as lib_rdbms

pg_url = 'postgresql://postgres:password@localhost:5432/postgres'
rdbms_postgres = lib_rdbms.RPARdbms(pg_url)
logger = lib_logger.RPALogger(rdbms_postgres, 'agent')
processes_folder = Path().home().joinpath('Desktop', 'Processes')
processes_folder.mkdir(exist_ok=True)
gitlab_token = 't7JLur7H7fyWQfxiCsED'


class Process:
    def __init__(self, git_repo: str):
        self.name = git_repo.split("/")[-1].replace('.git', '').strip()
        self.git_repo = git_repo
        self.process_folder = self.process_folder = processes_folder.joinpath(self.name)

    def check_the_process_folder(self):
        if processes_folder.joinpath(self.name).exists():

            return True
        else:

            processes_folder.joinpath(self.name).mkdir(exist_ok=True, parents=True)
            return False

    def clone_repo(self):
        # Ensure the repo URL uses HTTPS and inject the token for authentication
        if self.git_repo.startswith("https://"):
            auth_url = self.git_repo.replace("https://", f"https://oauth2:{gitlab_token}@")
        else:
            auth_url = self.git_repo.replace("http://", f"http://oauth2:{gitlab_token}@")
        try:
            # Run the git clone command
            result = subprocess.run(
                ["git", "clone", auth_url, str(self.process_folder)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.info(f"Repository cloned successfully into {self.process_folder}")
            return True
        except subprocess.CalledProcessError as e:
            logger.info(f"Error cloning repository: {e.stderr}")
            return False

    def setup_venv(self):

        venv_dir = Path(self.process_folder).joinpath('venv')
        self.venv_path = venv_dir
        if venv_dir.exists():
            logger.info('Virtual environment already exists')
            return True
        # Step 1: Create the virtual environment
        logger.info(f"Creating virtual environment at {venv_dir}...")
        str(venv_dir)
        os.chdir(str(self.process_folder))
        for i in range(5):
            try:
                result = subprocess.run(["python", "-m", "venv", "venv"],
                                        check=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True)

                # Print standard output and standard error
                logger.info(result.stdout)
                logger.info(result.stderr)
                break
            except Exception as ex:
                logger.info(ex)
        else:
            return False

        logger.info("Virtual environment created successfully.")

    def install_requirements(self):
        requirements_file: Path = self.process_folder.joinpath('requirements.txt')
        if requirements_file.exists():
            logger.info(f"Installing dependencies from {requirements_file}...")

            try:
                # Install requirements using the virtual environment's pip
                venv_python = self.process_folder.joinpath('venv', 'Scripts', 'python.exe')
                subprocess.run([str(venv_python), "-m", "pip", "install", "-r", str(requirements_file)], check=True)
                logger.info("Dependencies installed successfully.")
            except subprocess.CalledProcessError as e:
                logger.info(f"Error installing dependencies: {e}")
                return
        else:
            return False

    def open_process_in_separate_window(self, venv_path: str, script_path: str):
        venv_python = os.path.join(venv_path, 'Scripts')

        # Detect OS and open the process in a new terminal window

        run_file = self.process_folder.joinpath(str(self.name).replace("-", "_"), "run.py")
        os.chdir(str(self.process_folder))
        cmd = fr'cd {venv_python} && activate && cd {self.process_folder}  && python -m {str(self.name).replace("-", "_")}.run'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)

        return process

    def run_script(self):
        logger.info("Checking process existence")
        if self.check_the_process_folder():
            logger.info("Process exists folder")
        else:
            logger.info("New process: clonning")
            self.clone_repo()
        self.setup_venv()
        self.install_requirements()
        logger.info("Starting the robot...")
        venv_python = os.path.join(self.venv_path, 'Scripts')
        os.chdir(str(self.process_folder))
        cmd = fr'cd {venv_python} && activate && cd {self.process_folder}  && python -m {str(self.name).replace("-", "_")}.run'

        process = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print("process finished")
        logger.info("Process finished")
        return process.stdout, process.stderr

def monitor_process(self, process):
    try:
        while True:
            # Check if the process is still running
            logger.info("check process")

            if process.poll():
                # Process has exited
                if process.returncode != 0:
                    error = process.stderr.readline().decode().strip()
                    if error:
                        logger.info(error)
                else:
                    output = process.stdout.readline().decode().strip()
                    if output:
                        logger.info(f"Script log: {output}")
                    return "Script completed successfully"
            else:
                logger.info('s')

            # Collect logs from stdout

            logger.info("time sleep 2")
            time.sleep(2)

    except Exception as e:
        return f"Error during monitoring: {e}"


if __name__ == '__main__':
    git_repo = "http://gitlab.cloud.halykbank.nb/rpa-projects/foo-bar.git"
    process = Process(git_repo)

    process.run_script()
