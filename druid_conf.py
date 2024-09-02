import os
import yaml
from munch import munchify


class DruidConfig:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'druid_conf.yaml')) as __fp:
        __config = munchify(yaml.safe_load(__fp))

    def __init__(self):
        env = os.environ.get("env", "staging")
        self.env_config = self.__config[env]

    @property
    def mm_service(self) -> str:
        return self.env_config.mm_service

    @property
    def overlord_service(self) -> str:
        return self.env_config.overlord_service

    @property
    def namespace(self) -> str:
        return self.env_config.namespace

    @property
    def middle_manager(self) -> str:
        return self.env_config.middle_manager

    @property
    def suffix_domain(self) -> str:
        return self.env_config.suffix_domain

    @property
    def mm_port(self) -> int:
        return self.__config.ports.middle_manager

    @property
    def overlord_port(self) -> int:
        return self.__config.ports.overlord

    @property
    def workers_route(self) -> str:
        return self.__config.routes.get_workers

    @property
    def disable_worker_route(self) -> str:
        return self.__config.routes.disable_worker
    
    @property
    def enable_worker_route(self) -> str:
        return self.__config.routes.enable_worker

    @property
    def pending_tasks_route(self) -> str:
        return self.__config.routes.pending_tasks

    @property
    def running_tasks_route(self) -> str:
        return self.__config.routes.running_tasks
    
    @property
    def worker_status_route(self) -> str:
        return self.__config.routes.worker_status
    
    @property
    def all_running_tasks_route(self) -> str:
        return self.__config.routes.all_running_tasks

    @property
    def min_mm_count(self) -> int:
        return self.__config.min_mm_count

    @property
    def max_mm_count(self) -> int:
        return self.__config.max_mm_count

    @property
    def workers_per_mm(self) -> int:
        return self.__config.workers_per_mm
