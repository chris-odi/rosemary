import enum


class StatusTask(enum.Enum):
    NEW = "NEW"
    IN_PROGRESS = "RUNNING"
    FAILED = "FAILED"
    FATAL = "FATAL"
    CANCELED = "CANCELED"


class TypeTask(enum.Enum):
    REPEATABLE = "REPEATABLE"
    CRON = "CRON"
    MANUAL = "MANUAL"

    @classmethod
    def values(cls):
        return [member.value for member in cls]
