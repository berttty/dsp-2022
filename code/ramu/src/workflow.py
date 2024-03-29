from typing import Mapping, Callable

from context import RamuContext
from phase import Phase
from phases.clean_timeseries import clean_timeseries_factory
from phases.grid_generation import grid_generation_factory
from phases.grid_labeling import grid_labeling_factory
from phases.tile_calculations import tile_usage_calculation_factory
from phases.tile_generation import tile_generation_factory
from phases.tile_groupby import tile_group_by_factory


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
        self.stages = {}
        self.generate_stages()
       # self.addCheckpoint(self.end_name, sink_path)

    def add_checkpoint(self, phase_name: str, path: str):
        self.step_paths.update(phase_name, path)

    def generate_stages(self):
        self.stages['grid_generation'] = grid_generation_factory
        self.stages['grid_labeling'] = grid_labeling_factory
        self.stages['clean_timeseries'] = clean_timeseries_factory
        self.stages['tile_generation'] = tile_generation_factory
        self.stages['tile_usage_calculation'] = tile_usage_calculation_factory
        self.stages['tile_group_by'] = tile_group_by_factory

    def run(self):
        context: RamuContext = RamuContext()
        stages = {}
        order_stages = [
            'grid_generation',
            'grid_labeling',
            'clean_timeseries',
            'tile_generation',
            'tile_usage_calculation',
            'tile_group_by'
        ]

        for st in order_stages:
            stages[st] = self.stages[st](context, stages)

        stages[self.start_name].execute()
