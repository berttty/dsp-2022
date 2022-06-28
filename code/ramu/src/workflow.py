from typing import Mapping

from context import RamuContext
from phase import Phase
from phases.clean_timeseries import CleanTimeSeries
from phases.grid_generation import GridGeneration
from phases.grid_labeling import GridLabeling
from phases.tile_calculations import TileUsageCalculation
from phases.tile_generation import TileGeneration


class Workflow:
    """
    Workflow create the workflow to execute and prepare the phase required to be executed
    """
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
       # self.addCheckpoint(self.end_name, sink_path)

    def addCheckpoint(self, phase_name: str, path: str):
        self.step_paths.update(phase_name, path)

    def run(self):
        activate: bool = False
        current: Phase = None
        context: RamuContext = RamuContext()

        if self.start_name == 'grid_generation' or activate:
            activate = True
            current = GridGeneration()
            current.context = context
            current.sink_path = 'grid_generation'
            current.source = context.getSparkContext().parallelize(['berlin'])
            current.execute()

        if self.end_name == 'grid_generation':
            activate = False

        if self.start_name == 'grid_labeling' or activate:
            activate = True
            previous = current
            current = GridLabeling()
            current.context = context
            current.source_path = self.source_path
            current.sink_path = 'grid_labeling'
            if current.source_path == None:
                current.source = previous.sink
            else:
                current.source = None
            current.execute()

        if self.end_name == 'grid_labeling':
            activate = False


        if self.start_name == 'clean' or activate:
            activate = True
            previous = current
            current = CleanTimeSeries()
            current.context = context
           # current.sink_path = 'clean_time_series'
            current.sink_path = None
            # TODO change the repartion for a configuration varaible
            current.source = context.getSparkContext().wholeTextFiles(self.source_path).repartition(12)
            current.execute()

        if self.end_name == 'clean':
            activate = False

        if self.start_name == 'tile_generation' or activate:
            activate = True
            previous = current
            current = TileGeneration()
            current.context = context
            current.sink_path = 'tile_generation'
            if previous is None:
                current.source_path = self.source_path
                current.source = None
            else:
                current.source = previous.sink
                current.source_path = None

            current.execute()

        if self.end_name == 'tile_generation':
            activate = False

        if self.start_name == 'tile_usage_calculation' or activate:
            activate = True
            previous = current
            current = TileUsageCalculation()
            current.context = context
            current.sink_path = 'tile_usage_calculation'
            if previous is None:
                current.source_path = self.source_path
                current.source = None
            else:
                current.source = previous.sink
                current.source_path = None

            current.execute()

        if self.end_name == 'tile_usage_calculation':
            activate = False
