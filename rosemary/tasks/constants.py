import enum


class TypeTaskRosemary(enum.Enum):
    REPEATABLE = "REPEATABLE"
    CRON = "CRON"
    MANUAL = "MANUAL"
    NOT_SETUP = "NOT_SETUP"

    @classmethod
    def values(cls):
        return [member.value for member in cls]


class StatusTaskRosemary(enum.Enum):
    NEW = "NEW"
    IN_PROGRESS = "IN_PROGRESS"
    FAILED = "FAILED"
    FATAL = "FATAL"
    CANCELED = "CANCELED"
    FINISHED = 'FINISHED'
