1. **Step 1: Install psycopg2**

    `poetry add psycopg2`
    
    OR

    `poetry add psycopg2-binary`

2. **Step 2: Install rosemary:**
    
    `poetry add rosemary`

How to use:

1. Create file `rosemary_config.py` for initial config for connect to DB and etc.
```python
from rosemary.rosemary import Rosemary

rosemary = Rosemary(
    # Your db host
    db_host='0.0.0.0',
    
    # Your db password
    db_password='postgres',
   
    # Your db port. Optional, default = 5432
    db_port=5432,
   
    # Your db user
    db_user='postgres',

    # Your db name
    db_name_db='postgres',  

    # Count of tasks which one worker can run in one time, Minimum 1
    max_tasks_per_worker=30, 
   
    # Count of your workers. If one of worker will be died, rosemary recreate it.
    # Minimum 1
    workers=3,  
)
```

2. How to create task:
Create file with your tasks. For example `tasks.py`. You can create many files for your tasks.
```python
from rosemary_config import rosemary
import random
import asyncio
from rosemary import RosemaryInterval
from rosemary.tasks.constants import TypeTaskRosemary
... other imports...


# Example manual task
class SleepTask(rosemary.ManualTask):
    # Count maximum repeat task if happened some exception 
    max_retry = 3
    
    # Delay before retry when happen exception
    delay_retry: RosemaryInterval = RosemaryInterval(seconds=5)
    
    # Delay before start task
    delay: RosemaryInterval = RosemaryInterval(seconds=10)
    
    # How much time waiting for one task
    timeout = 30
   
    async def run(self):
        sleep = random.randint(1, 10)
        await asyncio.sleep(sleep)
        return f"I slept {sleep} sec"

class ExampleTask(rosemary.ManualTask):

    # You can use optional params for your task:
    async def run(self, data, session):
       query = select(RosemaryTaskModel).where(
            RosemaryTaskModel.name == self.get_name()
        ).order_by(RosemaryTaskModel.id)
        result = await session.execute(query)
        task_db: list[RosemaryTaskModel] = result.scalars().all()
        result = task_db[-1].id
        logger.info(f'Task ID GET: {result}')
        return result
```

3. Create file `rosemary_runner.py`. You will use for run rosemary:
```python
from rosemary_config import rosemary
from tasks import ExampleTask, SleepTask


# Register your task in runner
rosemary.register_task(SleepTask)
rosemary.register_task(ExampleTask)

if __name__ == '__main__':
    # Run your rosemary
    rosemary.run()
```

4. Implement your task:
```python
from task import SleepTask


async def main():
   task_id = await SleepTask().create()
   print(f'You create task {task_id}')
   another_task_id = await SleepTask().create(data={123: 456, 'example': 'example123'})
   print(f'You create another task {another_task_id} with params')

```

**Requirements:**
* python = "^3.11"
* pydantic = "^2"
* sqlalchemy = "^2"
* asyncpg = "^0.29.0"
* asyncio = "^3"
* logger = "^1"
* importlib = "^1"
* setuptools = "^69"
* alembic = "^1"
* greenlet = "^3"

**Manual install:**
* psycopg2-binary or psycopg2