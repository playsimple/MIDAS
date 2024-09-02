from druid_conf import DruidConfig
import requests
from requests.exceptions import HTTPError, RequestException
from logging_config import logger
from typing import Union


class HttpRequest:

    @staticmethod
    def get(url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = {}
            try:
                data = response.json()
            except ValueError as e:
                logger.warn(f"Response content is not valid JSON: {e}. API: {url}")
            return data

        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}. API: {url}")
            return None
        except RequestException as req_err:
            logger.error(f"Request error occurred: {req_err}. API: {url}")
            return None

    @staticmethod
    def post(url, json={}):
        try:
            response = requests.post(url, json=json)
            response.raise_for_status()
            data = {}
            try:
                data = response.json()
            except ValueError as e:
                logger.warn(f"Response content is not valid JSON: {e}. API: {url}")
            return data
        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}. API: {url}")
            return None
        except Exception as err:
            logger.error(f"Other error occurred: {err}. API: {url}")
            return None


class DruidApiHelper:

    def __init__(self):
        self.druid_conf = DruidConfig()      
        self.http_requester = HttpRequest()

    def get_count_of_pending_tasks(self) -> Union[int, None]:
        url = f"http://{self.druid_conf.overlord_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.overlord_port}{self.druid_conf.pending_tasks_route}"
        pending_tasks = self.http_requester.get(url)
        if pending_tasks is not None:
            pending_tasks_count = len(pending_tasks)
            return pending_tasks_count
        return None  
    
    def get_count_of_running_mm(self) -> Union[int, None]:
        url = f"http://{self.druid_conf.overlord_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.overlord_port}{self.druid_conf.workers_route}"
        workers = self.http_requester.get(url)
        if workers is not None:
            running_mm_count = len(workers)
            return running_mm_count
        return None
    
    def get_running_tasks_count(self) -> Union[int, None]:
        url = f"http://{self.druid_conf.overlord_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.overlord_port}{self.druid_conf.all_running_tasks_route}"
        running_tasks = self.http_requester.get(url)
        if running_tasks is not None:
            return len(running_tasks)
        return None
    
    def get_running_tasks_on_mm(self, mm_id: int) -> Union[int, None]:
        url = f"http://{self.druid_conf.middle_manager}-{mm_id}.{self.druid_conf.mm_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.mm_port}{self.druid_conf.running_tasks_route}"
        running_tasks = self.http_requester.get(url)
        if running_tasks is not None:
            return len(running_tasks)
        return None    
    
    def get_free_workers_on_mm(self, mm_id: int) -> Union[int, None]:
        url = f"http://{self.druid_conf.middle_manager}-{mm_id}.{self.druid_conf.mm_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.mm_port}{self.druid_conf.running_tasks_route}"
        running_tasks = self.http_requester.get(url)
        if running_tasks is not None:
            return self.druid_conf.workers_per_mm - len(running_tasks)
        return None
        
    def is_mm_idle(self, mm_id: int) -> Union[bool, None]:
        url = f"http://{self.druid_conf.middle_manager}-{mm_id}.{self.druid_conf.mm_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.mm_port}{self.druid_conf.running_tasks_route}"
        running_tasks = self.http_requester.get(url)
        if running_tasks is not None and len(running_tasks) == 0:
            return True
        elif running_tasks is not None:
            return False
        return None
    
    def is_mm_disable(self, mm_id: int) -> Union[bool, None]:
        url = f"http://{self.druid_conf.middle_manager}-{mm_id}.{self.druid_conf.mm_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.mm_port}{self.druid_conf.worker_status_route}"
        resp = self.http_requester.get(url)
        if resp is not None:
            if not list(resp.values())[0]:
                return True
            return False
        return None 

    def disable_idle_mm(self, mm_id: int) -> Union[bool, None]:
        url = f"http://{self.druid_conf.middle_manager}-{mm_id}.{self.druid_conf.mm_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.mm_port}{self.druid_conf.disable_worker_route}"
        resp = self.http_requester.post(url)
        if resp is not None:
            return True
        return None
    
    def enable_mm(self, mm_id: int) -> Union[bool, None]:
        url = f"http://{self.druid_conf.middle_manager}-{mm_id}.{self.druid_conf.mm_service}.{self.druid_conf.namespace}.{self.druid_conf.suffix_domain}:{self.druid_conf.mm_port}{self.druid_conf.enable_worker_route}"
        resp = self.http_requester.post(url)
        if resp is not None:
            return True
        return None
