import enum


class StatusTaskRosemary(enum.Enum):
    NEW = "NEW"
    IN_PROGRESS = "RUNNING"
    FAILED = "FAILED"
    FATAL = "FATAL"
    CANCELED = "CANCELED"


class TypeTaskRosemary(enum.Enum):
    REPEATABLE = "REPEATABLE"
    CRON = "CRON"
    MANUAL = "MANUAL"
    NOT_SETUP = "NOT_SETUP"

    @classmethod
    def values(cls):
        return [member.value for member in cls]


class StatusWorkerRosemary(enum.Enum):
    WORKING = 'WORKING'
    KILLED = 'KILLED'
    CHECKING = 'CHECKING'
