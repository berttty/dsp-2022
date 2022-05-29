from typing import Mapping

from ramu.phase import Phase

from ramu.phases.grid_generation import GridGeneration


class Workflow:

    start_name: str
    start: Phase

    end_name: str
    end: Phase

    source_path: str
    step_paths: Mapping[str, str]

    def __init__(self, start_name: str, end_name: str, source_path: str, sink_path: str):
        """
        default constructor
        :param start_name: first phase that need to be executed
        :param end_name: last phase that will be executed in the workflow
        """
        # TODO: validate the start_name is before than the end_name
        self.start_name = start_name.lower()
        self.end_name = end_name.lower()
        self.source_path = source_path
        self.step_paths = {}
        self.addCheckpoint(self.end_name, sink_path)

    def addCheckpoint(self, phase_name: str, path: str):
        self.step_paths.update(phase_name, path)

    def run(self):
        activate: bool = False
        current: Phase = None

        if self.start_name == "gridgeneration" or activate:
            activate = True
            source =
            current = GridGeneration()
