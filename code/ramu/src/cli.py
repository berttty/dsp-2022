"""Console script for ramu."""
import argparse
import sys

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
   # Workflow('grid_generation', 'grid_generation', None, None).run()
    Workflow('grid_labeling', 'grid_labeling', 'grid_generation', None).run()

#    Workflow('clean', '', 'file:///Users/bertty/dataset_dsp/Berlin/Rides/2019/**/*', None).run()
  #  Workflow('clean', '', 'file:///Users/bertty/dataset_dsp/Berlin/Rides/test/*', None).run()
    sys.exit(main())  # pragma: no cover
