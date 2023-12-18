from example.rosemary_config import rosemary
from tasks import SleepTask, WorkWithDBTask, RepeatableTask, ErrorTask, RepeatableTaskModel

rosemary.register_task(SleepTask)
rosemary.register_task(WorkWithDBTask)
# rosemary.register_task(RepeatableTask)
rosemary.register_task(ErrorTask)


if __name__ == '__main__':
    rosemary.run(
        # repeat_tasks=[RepeatableTask().create(data=RepeatableTaskModel(time_sleep=7))]
    )
