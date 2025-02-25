from click import Command


def get_command_name(c: Command) -> str:
    return c.name
