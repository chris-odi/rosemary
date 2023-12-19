from rosemary.rosemary import Rosemary

rosemary = Rosemary(
    db_host='0.0.0.0',
    db_password='postgres',
    db_port=5432,
    db_user='postgres',
    db_name_db='postgres',
    max_tasks_per_worker=30,
    workers=3,
)
