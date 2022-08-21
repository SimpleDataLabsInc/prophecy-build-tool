import subprocess


class Process:
    def __init__(
        self,
        process_args,
        current_working_directory,
        std_output=subprocess.PIPE,
        std_err=subprocess.PIPE,
        is_shell=False,
    ):
        self.process_args = process_args
        self.current_working_directory = current_working_directory
        self.std_output = std_output
        self.std_err = std_err
        self.is_shell = is_shell
