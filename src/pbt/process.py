import subprocess
import time


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
    def process_sequential(processes, time_between_each_cmd=1):
        return_code = 0
        for process in processes:
            if process.running_message:
                print(process.running_message)
            result = subprocess.run(
                process.process_args,
                stdout=process.std_output,
                stderr=process.std_err,
                shell=process.is_shell,
                cwd=process.current_working_directory,
            )
            return_code, stdout, stderr = (
                result.returncode,
                result.stdout,
                result.stderr,
            )

            if stdout is not None:
                if stderr is not None and len(stderr) > 0:
                    print(
                        "   ",
                        "\n    ".join([line.decode("utf-8") for line in stdout.splitlines()]),
                    )
                    print(
                        "   ",
                        "\n    ".join([line.decode("utf-8") for line in stderr.splitlines()]),
                    )
                else:
                    print(
                        "   ",
                        "\n    ".join([line.decode("utf-8") for line in stdout.splitlines()]),
                    )

            if return_code != 0:
                break
            time.sleep(time_between_each_cmd)
        return return_code
