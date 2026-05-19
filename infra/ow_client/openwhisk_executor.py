import base64
import sys
import time
import warnings
import logging

import requests
from typing import List
from urllib3.exceptions import InsecureRequestWarning
from ow_client.logger import logger

from ow_client import utils
from ow_client.result_dataset import ResultDataset
from ow_client.utils import debug, ppdegub

AUTH_TOKEN = "MjNiYzQ2YjEtNzFmNi00ZWQ1LThjNTQtODE2YWE0ZjhjNTAyOjEyM3pPM3haQ0xyTU42djJCS0sxZFhZRnBYbFBrY2NPRnFtMTJDZEFzTWdSVTRWck5aOWx5R1ZDR3VNREdJd1A="


class OpenwhiskExecutor:
    def __init__(self, host, port, debug=False):
        self.protocol = 'https'
        self.host = host
        self.port = port
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({"Authorization": f"Basic {AUTH_TOKEN}", "Content-Type": "application/json"})
        self.session.verify = False
        warnings.filterwarnings('ignore', category=InsecureRequestWarning)
        self.__monitor_interval = 2  # seconds
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        logger.info("OpenwhiskExecutor initialized")

    def burst(self, action_name, params_list, file, is_zip=False, memory=256, debug_mode=False, custom_image=None,
              backend="rabbitmq",
              granularity=None, chunk_size=None, join=False, timeout=900000,
              completion_timeout=None) -> ResultDataset:
        """
        Function to invoke a burst of actions
        :param action_name: the name of the action to invoke. Action must be located into functions folder.
        Action name not include the extension of the file, but file must be named as functions/<action_name>{.rs, .zip}
        :param params_list: list with the parameters to pass to the actions (list of dicts)
        :param is_zip: indicates if the action is a zip file or a single .rs file
        :param memory: memory to allocate to the action
        :param debug_mode: if True, the debug mode is activated
        :param custom_image: if not None, the action is executed in a container with the specified image
        :param backend: the backend to use for the burst. (Rabbitmq, RedisStream...)
        :param granularity: workers per Burst invocation. If None, heterogeneous mode is used
        :param chunk_size: in burst comm middleware message exchanges (in KB)
        :param join: if True, the burst is executed in heterogeneous containers that respects requested granularity
        :param timeout: timeout in milliseconds for the action execution (default: 60000)
        :return: Dataset with the results and some metrics of the executions
        """
        dataset = ResultDataset()
        self.__create_action(action_name, file, is_zip, memory, custom_image, timeout)
        activation_ids = self.__invoke_burst_actions(action_name, params_list, granularity, backend, chunk_size, join,
                                                     debug_mode)
        for index, activation_id in enumerate(activation_ids):
            dataset.add_invocation(index, activation_id, time.time(), is_burst=True)
        fetch_count = 0
        self.__wait_for_completion(dataset, completion_timeout=completion_timeout)
        return dataset

    def map(self, action_name, params_list, file, is_zip=False, memory=256, custom_image=None, timeout=60000) -> ResultDataset:
        """
        Function to invoke a map (classic) of actions
        :param action_name: the name of the action to invoke. Action must be located into functions folder.
        Action name not include the extension of the file, but file must be named as functions/<action_name>{.rs, .zip}
        :param params_list: list with the parameters to pass to the actions (list of dicts)
        :param is_zip: indicates if the action is a zip file or a single .rs file
        :param memory: memory to allocate to the action
        :param custom_image: if not None, the action is executed in a container with the specified image
        :param timeout: timeout in milliseconds for the action execution (default: 60000)
        :return: Dataset with the results and some metrics of the executions
        """
        dataset = ResultDataset()
        self.__create_action(action_name, file, is_zip, memory, custom_image, timeout)
        for index, input in enumerate(params_list):
            activation_id = self.__invoke_single_action(action_name, input)
            dataset.add_invocation(index, activation_id, time.time(), is_burst=False)
        self.__wait_for_completion(dataset)
        return dataset

    def __create_action(self, action_name, file, is_zip, memory, custom_image, timeout=60000):
        action_data = {
            "exec": {
                "main": "main",
                "kind": "rust:1.34"
            },
            "name": action_name,
            "version": "0.0.1",
            "namespace": "guest",
            "limits": {
                "memory": memory,
                "timeout": timeout
            }
        }
        if is_zip:
            code = open(file, "rb").read()
            code = (base64.b64encode(code)).decode('ascii')
            binary = True
        else:
            with open(file, "r") as rust_file:
                rust_code = rust_file.read()
            code = rust_code.strip()
            binary = False

        action_data["exec"]["binary"] = binary
        action_data["exec"]["code"] = code

        if custom_image:
            action_data["exec"]["image"] = custom_image
            action_data["exec"]["kind"] = "blackbox"

        response = self.session.put(
            f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/actions/{action_name}?overwrite=true",
            json=action_data, timeout=300)

        if response.status_code == 200:
            logger.info(f"Function {action_name} created in Openwhisk successfully")
        else:
            logger.info(f"Error creating function {action_name}: {response.text}")

    def __invoke_single_action(self, action_name, params):
        invoke_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/actions/{action_name}"

        response = self.session.post(invoke_url, json=params)

        if str(response.status_code).startswith("2"):
            logger.info(f"Function {action_name} invoked in Openwhisk successfully")
            result = response.json()
            return result["activationId"]
        else:
            logger.error(f"Function not invoked {action_name}: {response.text}")
            return None

    def __check_function_finished(self, activation_id):
        activation_get_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/activations/{activation_id}"
        response = self.session.get(activation_get_url)
        if str(response.status_code).startswith("2"):
            logger.debug(f"Querying function {activation_id} to Openwhisk")
            logger.debug(response.json())
            result_invk = response.json()
            if result_invk["response"]["result"]:
                logger.info(f"Function {activation_id} finished")
                return result_invk
            else:
                logger.error(f"Function {activation_id} bad finished")
                return None
        elif response.status_code == 404:
            return None
        else:
            logger.debug(f"Function {activation_id} not finished: {response.text}")
            return None

    def get_activation_record(self, activation_id):
        activation_get_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/activations/{activation_id}"
        response = self.session.get(activation_get_url)
        if str(response.status_code).startswith("2"):
            return response.json()
        return None

    def __wait_for_completion(self, dataset, completion_timeout=None):
        monitor_count = 0
        start_time = time.time()
        while any("result" not in item for item in dataset.results):
            if completion_timeout is not None:
                elapsed = time.time() - start_time
                if elapsed > completion_timeout:
                    missing = [item for item in dataset.results if "result" not in item]
                    for item in missing:
                        logger.warning(f"Activation {item['activationId']} timed out after {elapsed:.0f}s")
                        dataset.add_result(
                            item["activationId"],
                            0,
                            0,
                            {"error": f"completion_timeout after {elapsed:.0f}s"},
                        )
                    break
            missing_results = [item for item in dataset.results if "result" not in item]
            for item in missing_results:
                result = self.__check_function_finished(item["activationId"])
                if result:
                    logger.info(f"[{item['activationId']} finished]: {result['response']['result']}")
                    dataset.add_result(item["activationId"], result["start"], result["end"],
                                       result["response"]["result"])
            monitor_count += 1
            logger.debug(f"Monitor count: {monitor_count}")
            time.sleep(self.__monitor_interval)
        return dataset

    def __invoke_burst_actions(self, action_name, params_list, granularity,
                               backend, chunk_size, join, debug_mode) -> List[str] | None:
        burst_url = f"{self.protocol}://{self.host}:{self.port}/api/v1/namespaces/guest/actions/{action_name}?burst=true"
        if granularity:
            burst_url += f"&granularity={granularity}"
        if join:
            burst_url += "&join=true"
        if chunk_size:
            burst_url += f"&chunk_size={chunk_size}"
        burst_url += f"&backend={backend}&debug={str(debug_mode).lower()}"
        params_list = {"value": params_list}
        response = self.session.post(burst_url, json=params_list)

        if str(response.status_code).startswith("2"):
            result = response.json()
            logger.info(f"Burst {action_name} invoked successfully. Actions: {result['activationIds']}")
            return result["activationIds"]
        else:
            logger.error(f"Burst not invoked {action_name}: {response.text}")
            return None
