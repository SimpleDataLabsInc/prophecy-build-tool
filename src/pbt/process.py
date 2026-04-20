import subprocess
import time
from typing import List, Optional

from .runner import CommandRunner, default_runner


class Process:
    def __init__(
        self,
        process_args,
        current_working_directory,
        std_output=subprocess.PIPE,
        std_err=subprocess.PIPE,
        is_shell=False,
        running_message="",
    ):
        self.process_args = process_args
        self.current_working_directory = current_working_directory
        self.std_output = std_output
        self.std_err = std_err
        self.is_shell = is_shell
        self.running_message = running_message

    @staticmethod
    def process_sequential(
        processes: List["Process"],
        time_between_each_cmd: int = 1,
        runner: Optional[CommandRunner] = None,
    ) -> int:
        """Run ``processes`` one after another and return the last return code.

        An optional :class:`~pbt.runner.CommandRunner` can be injected so tests
        can swap in a :class:`~test.fakes.FakeRunner` instead of spawning real
        subprocesses. When omitted, the process-wide default is used.
        """

        active_runner = runner or default_runner
        return_code = 0
        for process in processes:
            if process.running_message:
                print(process.running_message)
            result = active_runner.run(
                process.process_args,
                cwd=process.current_working_directory,
                shell=process.is_shell,
                capture_output=True,
                check=False,
            )
            return_code, stdout, stderr = result.returncode, result.stdout, result.stderr

            if stdout:
                print("   ", "\n    ".join(stdout.splitlines()))
            if stderr:
                print("   ", "\n    ".join(stderr.splitlines()))

            if return_code != 0:
                break
            time.sleep(time_between_each_cmd)
        return return_code
