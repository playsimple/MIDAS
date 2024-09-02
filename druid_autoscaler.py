from druid_api_helper import DruidApiHelper
from kubectl_executer import KubectlExecuter
from druid_conf import DruidConfig
from logging_config import logger
from typing import List, Union


class ReplicaCalculator:
    def __init__(self):
        self.conf = DruidConfig()

    def calc_scale_up_to(self, waiting_tasks: int, running_mm_count: int) -> int:
        needed_mm_count = (waiting_tasks / self.conf.workers_per_mm) + running_mm_count
        if waiting_tasks % self.conf.workers_per_mm > 0:
            needed_mm_count = needed_mm_count + 1
        final_mm_count = min(self.conf.max_mm_count, needed_mm_count)
        return int(final_mm_count)

    def calc_scale_down_to(self, count_idle_mm: int, running_mm_count: int) -> int:
        needed_mm_count = running_mm_count - count_idle_mm
        final_mm_count = max(self.conf.min_mm_count, needed_mm_count)
        return int(final_mm_count)
    
    def get_worker_cap_to_check_against_to_disable_mm(self, mm_id: int) -> int:
        return int(0.9*((mm_id)* self.conf.workers_per_mm))
    
    def get_min_workers(self) -> int:
        return self.conf.min_mm_count
        
    
class AutoscalerMeta:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AutoscalerMeta, cls).__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        self.__disable_mm_count  = 0
        self.__prev_scale_up_mm_count = 0

    def set_prev_scale_state(self, node_count: int) -> None:
         logger.info(f"Changing prev scale up mm count state, current is : {self.__prev_scale_up_mm_count}")
         self.__prev_scale_up_mm_count = node_count
         logger.info(f"Changed prev scale up mm count state, current is : {self.__prev_scale_up_mm_count}")

    def inc_disable_mm_count(self) -> None:
        logger.info(f"Changing disable mm count, current is : {self.__disable_mm_count}")
        self.__disable_mm_count += 1
        logger.info(f"Changed disabled mm count, current is : {self.__disable_mm_count}")
    
    def dec_disable_mm_count(self) -> None:
        if self.__disable_mm_count > 0:
            logger.info(f"Changing disable mm count, current is : {self.__disable_mm_count}")
            self.__disable_mm_count -= 1
            logger.info(f"Changed disabled mm count, current is : {self.__disable_mm_count}")
    
    def get_prev_scale_state(self) -> int:
        return self.__prev_scale_up_mm_count
    
    def get_disable_mm_count(self) -> int:
        return self.__disable_mm_count
        
     
    
class Autoscaler:
     def __init__(self):
        self.druid_api_helper = DruidApiHelper()
        self.replica_calculator = ReplicaCalculator()
        self.kubectl_executor = KubectlExecuter()
        self.meta = AutoscalerMeta()
     
     def execute(self) -> Union[bool, None]:
         raise NotImplementedError

    
class ScaleUp(Autoscaler):
    def _get_free_workers_in_disable_mm(self, running_mm_count: int) -> List[int]:
        res = []
        disable_mm_count = self.meta.get_disable_mm_count()
        for i in range(disable_mm_count, 0, -1):
          free_workers_on_mm = self.druid_api_helper.get_free_workers_on_mm(running_mm_count - i)
          if free_workers_on_mm is None:
              free_workers_on_mm = 0
          res.append(free_workers_on_mm)
        return res
    
    def _try_enable_mm(self, mm_id_to_enable: int) -> bool:
        retries = 3

        for _ in range(retries):
            logger.info(f"Enabling middle-manager : {mm_id_to_enable}")
            is_enabled = self.druid_api_helper.enable_mm(mm_id_to_enable)
            if is_enabled is None:
                logger.error(f"Failed to enable middle-manager id: {mm_id_to_enable}")
                continue
            logger.info("Successfully enabled middle-manager")
            return True         
        return False
    
    def _scale_up_mm_pods(self, pending_tasks: int, running_mm_count: int) -> bool:
        mm_count = self.replica_calculator.calc_scale_up_to(pending_tasks, running_mm_count)
        if mm_count > running_mm_count:
            logger.info(f"Current number of pending tasks: {pending_tasks}")
            logger.info(f"Triggering mm scale-up, replica change from {running_mm_count} to {mm_count}")
            is_done = self.kubectl_executor.change_replicas(mm_count)
            if is_done:
                logger.info(f"Successfully scaled up from {running_mm_count} to {mm_count}")
                self.meta.set_prev_scale_state(mm_count)
                return True
            else:
                logger.error(f"Failed to scale up from {running_mm_count} to {mm_count}")
        self.meta.set_prev_scale_state(0)
        return False
    
    def _fill_disabled_mm(self, pending_tasks: int, running_mm_count: int) -> int:
        free_workers_per_mm: List[int] = self._get_free_workers_in_disable_mm(running_mm_count)
        for free_workers in free_workers_per_mm:
            if pending_tasks <= 0:
                return 0
            disable_mm_count = self.meta.get_disable_mm_count()
            mm_id_to_enable = running_mm_count - disable_mm_count
            if not self._try_enable_mm(mm_id_to_enable):
                break
            self.meta.dec_disable_mm_count()
            pending_tasks = pending_tasks - free_workers

        return max(0,pending_tasks)
    
    def execute(self) -> Union[bool, None]:
        pending_tasks = self.druid_api_helper.get_count_of_pending_tasks()
        if pending_tasks is None:
            logger.error("Couldn't fetch pending tasks from druid, will check again in next cycle")
            return None
        
        running_mm_count = self.druid_api_helper.get_count_of_running_mm()
        if running_mm_count is None:
            logger.error("Couldn't get current MM replica count from druid, will check again in next cycle")
            return None
        
        prev_scale_state = self.meta.get_prev_scale_state()
        if prev_scale_state > 0 and not prev_scale_state == running_mm_count:
            logger.info(f"Prev scaleup has not completed so not scaling further till scale up gets complete, Running mm count: {running_mm_count}, Prev scaled count: {prev_scale_state}")
            return None
  
        if pending_tasks == 0:
            return False
        
        logger.info(f"Current Pending Tasks: {pending_tasks}, Prev scale up state: {prev_scale_state}, Running MM count: {running_mm_count}")
        disable_mm_count = self.meta.get_disable_mm_count()
        logger.info(f"Current disabled mm count is {disable_mm_count}")
        if disable_mm_count > 0:
            logger.info(f"Found disabled middle managers enabling them to fullfill workers request, no of disabled mm: {disable_mm_count}")
            pending_tasks = self._fill_disabled_mm(pending_tasks, running_mm_count)
            logger.info(f"Number of pending tasks after enabling disabled middle-managers: {pending_tasks}")
        self._scale_up_mm_pods(pending_tasks, running_mm_count) if pending_tasks else None  
        return True
        
    
class ScaleDown(Autoscaler):

    def _disable_mm_handler(self, mm_id: int) -> Union[bool, None]:
        is_disabled = self.druid_api_helper.is_mm_disable(mm_id)
        if is_disabled is None:
            return None
        if is_disabled:
            logger.info(f"MM-{mm_id} is already disabled")
            return True
        is_disabled = self.druid_api_helper.disable_idle_mm(mm_id)
        if not is_disabled:
            logger.error(f"Failed to disable idle middle-manager id: {mm_id}")
            return False
        logger.info(f"Successfully Disabled idle middle-manager id : {mm_id}")
        self.meta.inc_disable_mm_count()
        return True
    
    def _can_mm_be_idle(self, mm_id: int) -> Union[bool, None]:
        cap_to_check_against = self.replica_calculator.get_worker_cap_to_check_against_to_disable_mm(mm_id)
        tot_running_tasks = self.druid_api_helper.get_running_tasks_count()
        running_tasks_mm = self.druid_api_helper.get_running_tasks_on_mm(mm_id)
        if tot_running_tasks is None or running_tasks_mm is None:
            return None
        running_tasks_to_check_against = tot_running_tasks - running_tasks_mm
        if cap_to_check_against > running_tasks_to_check_against:
            return True
        return False
    
    def _idle_mm_handler(self, mm_id: int) -> Union[bool, None]:
        is_mm_idle = self.druid_api_helper.is_mm_idle(mm_id)
        if is_mm_idle is None:
            logger.error("Couldn't check if MM is idle or not due to API failure")
            return None
        return is_mm_idle
    
    def _scale_down_mm_pod(self, running_mm_count: int) -> bool:     
        mm_count = self.replica_calculator.calc_scale_down_to(1, running_mm_count)
        logger.info(f"Triggering mm scale-down, replica change from {running_mm_count} to {mm_count}")
        is_done = self.kubectl_executor.change_replicas(mm_count)
        if is_done:
            logger.info(f"Successfully scaled down from {running_mm_count} to {mm_count}")
        else:
            logger.error(f"Failed to scale down from {running_mm_count} to {mm_count}")
            return False
        self.meta.set_prev_scale_state(0)
        self.meta.dec_disable_mm_count()
        return True
    
    def execute(self) -> Union[bool, None]:
        running_mm_count = self.druid_api_helper.get_count_of_running_mm()
        if running_mm_count is None:
            logger.error("Couldn't get current MM replica count from druid, will check again in next cycle")
            return None
        if running_mm_count <= self.replica_calculator.get_min_workers():
            return None
        mm_id = running_mm_count - 1
        is_idle = self._idle_mm_handler(mm_id)
        if is_idle:
            logger.info(f"Found idle middle manager. Disabling idle middle-manager id: {mm_id}")
            is_disable =self._disable_mm_handler(mm_id)
            if is_disable:
                self._scale_down_mm_pod(running_mm_count) 
                return True
        disable_mm_count = self.meta.get_disable_mm_count()
        mm_id = mm_id - disable_mm_count
        can_be_idle = self._can_mm_be_idle(mm_id)
        if can_be_idle:
            logger.info(f"Middle-Manager-{mm_id} can be downscaled as cluster has enough resources, Disabling it")
            self._disable_mm_handler(mm_id) 
            return True
        return False

class AutoScalerFacade:

    def __init__(self):
        self.scale_up = ScaleUp()
        self.scale_down = ScaleDown()

    def execute(self) -> None:
        is_scale_up = self.scale_up.execute()
        if is_scale_up is False and not is_scale_up is None:
            self.scale_down.execute()
        