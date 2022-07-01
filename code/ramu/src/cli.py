"""Console script for ramu."""
import argparse
import logging
import sys

from context import RamuContext
from workflow import Workflow


def main():
    """Console script for ramu."""
    parser = argparse.ArgumentParser()
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into "
          "ramu.cli.main")
    return 0


if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%y/%m/%d %H:%M:%S', level=logging.INFO)
    logging.info('Started')


  #  Workflow('grid_generation', 'grid_generation', None, None).run()
 #   Workflow('grid_labeling', 'grid_labeling', None, None).run()
   # Workflow('grid_labeling', 'grid_labeling', 'grid_generation', None).run()

#    Workflow('clean', 'tile_generation', 'file:///Users/bertty/dataset_dsp/Berlin/Rides/2019/**/*', None).run()
#    Workflow('clean_timeseries', 'tile_generation', 'file:///Users/bertty/dataset_dsp/Berlin/Rides/test/*', None).run()
#    Workflow('tile_generation', 'tile_generation', 'clean_time_series', None).run()
#    Workflow('tile_usage_calculation', 'tile_usage_calculation', 'tile_generation', None).run()
#    Workflow('tile_groupby', 'tile_groupby', 'tile_usage_calculation', None).run()

    Workflow('clean_timeseries', 'clean_timeseries', None, None).run()

  #  print(RamuContext().get('.stages.grid_generation.inputs[0]'))

    logging.info('Finished')
    sys.exit(main())  # pragma: no covercontext.get('.stages.grid_generation.input[0].list')
