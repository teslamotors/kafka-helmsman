"""CLI entrypoint."""
import logging.config
import os

from fabric.main import program


def main():
    """Entry point."""
    os.chdir(os.path.dirname(__file__))
    logging.config.fileConfig("logging.ini")
    program.run()
