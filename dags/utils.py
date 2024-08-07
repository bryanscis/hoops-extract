import logging
from datetime import datetime

def log_task_start(task_id):
    logging.info(f"Task {task_id} started")

def log_task_end(task_id):
    logging.info(f"Task {task_id} finished")

def get_current_season():
    '''
    Gets current season as NBA season usually runs through October to April. Returns NBA year.
    '''
    now = datetime.now()
    if now.month >= 10:
        return now.year + 1
    return now.year