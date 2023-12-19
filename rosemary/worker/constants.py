import enum


class StatusWorkerRosemary(enum.Enum):
    WORKING = 'WORKING'
    KILLED = 'KILLED'
    CHECKING = 'CHECKING'
