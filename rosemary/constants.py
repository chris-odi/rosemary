import enum


class StatusTaskRosemary(enum.Enum):
    NEW = "NEW"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    FATAL = "FATAL"
    CANCELED = "CANCELED"
    FINISHED = 'FINISHED'


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
