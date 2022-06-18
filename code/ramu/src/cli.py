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
    Workflow('gridgeneration', '', None, None).run()
    sys.exit(main())  # pragma: no cover
