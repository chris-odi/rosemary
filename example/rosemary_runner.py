from rosemary_config import rosemary
from tasks import SleepTask, CheckLastIdTask, RepeatableTask

rosemary.register_task(SleepTask)
rosemary.register_task(CheckLastIdTask)
rosemary.register_task(RepeatableTask)

if __name__ == '__main__':
    rosemary.run()
