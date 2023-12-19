import enum


class StatusWorkerRosemary(enum.Enum):
    WORKING = 'WORKING'
    KILLED = 'KILLED'
    CHECKING = 'CHECKING'


ESCAPE_ERROR = 'Escaped in status in_progress'
