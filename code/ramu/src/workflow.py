from typing import Mapping, Callable

from context import RamuContext
from phase import Phase
from phases.clean_timeseries import CleanTimeSeries
from phases.grid_generation import grid_generation_factory
from phases.grid_labeling import GridLabeling
from phases.tile_calculations import TileUsageCalculation
from phases.tile_generation import TileGeneration
from phases.tile_groupby import TileGroupBy


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

    stages: Mapping[str, Callable]

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

    def add_checkpoint(self, phase_name: str, path: str):
        self.step_paths.update(phase_name, path)

    def generate_stages(self):
        self.stages['grid_generation'] = grid_generation_factory




    def run(self):
        activate: bool = False
        current: Phase = None
        context: RamuContext = RamuContext()
        stages = {}


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
            stages['grid_labeling'] = current

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
            stages['clean'] = current

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
            stages['tile_generation'] = current

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
            stages['tile_usage_calculation'] = current

        if self.end_name == 'tile_usage_calculation':
            activate = False

        if self.start_name == 'tile_groupby' or activate:
            activate = True
            previous = current
            current = TileGroupBy()
            current.context = context
            current.sink_path = 'tile_groupby'
            if previous is None:
                current.source_path = self.source_path
                current.source = None
            else:
                current.source = previous.sink
                current.source_path = None
            current.execute()
            stages['tile_groupby'] = current

        if self.end_name == 'tile_groupby':
            activate = False
