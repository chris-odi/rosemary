from example.rosemary_config import rosemary
from tasks import SleepTask, WorkWithDBTask, RepeatableTask, ErrorTask

rosemary.register_task(SleepTask)
rosemary.register_task(WorkWithDBTask)
rosemary.register_task(RepeatableTask)
rosemary.register_task(ErrorTask)

if __name__ == '__main__':
    rosemary.run()
