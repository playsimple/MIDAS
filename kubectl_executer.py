import subprocess
from druid_conf import DruidConfig
from logging_config import logger


class CommandsGenerator:
    def __init__(self):
        self.conf = DruidConfig()

    def generate_cmd_to_change_replicas(self, desired_replicas: int) -> str:
        cmd = f"kubectl scale statefulsets {self.conf.middle_manager} -n {self.conf.namespace} --replicas={desired_replicas}"
        return cmd


class CommandsRunner:
    @staticmethod
    def run(command):
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True, shell=True)
            logger.info(f"Command succeeded:{command}")
            logger.info(result.stdout)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with exit code {e.returncode}")
            logger.error(e.stderr)
            return False


class KubectlExecuter:
    def __init__(self):
        self.cmd_generator = CommandsGenerator()
        self.cmd_runner = CommandsRunner()

    def change_replicas(self, desired_replicas: int):
        command = self.cmd_generator.generate_cmd_to_change_replicas(desired_replicas)
        resp = self.cmd_runner.run(command)
        return resp
