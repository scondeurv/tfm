from datetime import datetime

from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from ow_client.logger import logger

from ow_client.utils import debug


class ResultDataset:
    # results is a list of dicts with the following structure:
    # {
    #   "numFn": order of the function in the map,
    #   "activationId": "id of the activation",
    #   "submit": "timestamp of the submission",
    #   "start": "timestamp of the start of the execution",
    #   "end": "timestamp of the end of the execution",
    #   "result": "result of the execution",
    # }
    def __init__(self):
        self.results = []
        self.is_burst = False

    # invocation_input is a dict with the following structure:
    # {
    #   "numFn": order of the function in the map,
    #   "activationId": "id of the activation",
    #   "submit": "timestamp of the submission",
    # }
    def add_invocation(self, numFn, activationId, submit, is_burst=False):
        self.results.append({
            "numFn": numFn,
            "activationId": activationId,
            "submit": submit
        })
        self.is_burst = is_burst

    def add_result(self, activationId, start, end, result):
        roi = next((roi for roi in self.results if roi["activationId"] == activationId), None)
        if roi:
            roi["start"] = start
            roi["end"] = end
            roi["result"] = result
        else:
            logger.debug("Error: result not found")

    def get_results(self):
        return list(map(lambda x: x["result"], self.results))

    # in x-axis, timeline of the executions
    # in y-axis, each execution is a line
    # we want to show horizontal segments for each execution with start and end times
    def plot(self):
        fig, ax = plt.subplots(figsize=(10, 6))
        self.results.sort(key=lambda x: x["start"])
        colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']

        for i, execution in enumerate(self.results):
            submit_time = datetime.fromtimestamp(execution["submit"])
            start_time = datetime.fromtimestamp(execution["start"] / 1000)
            end_time = datetime.fromtimestamp(execution["end"] / 1000)
            ax.hlines(y=i, xmin=start_time, xmax=end_time, color="green", lw=2)
            ax.plot(submit_time, i, 'o', color="red", lw=2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.xticks(rotation=45)
        ax.set_xlabel('Time')
        ax.set_ylabel('# of execution')
        plt.tight_layout()
        plt.show()
