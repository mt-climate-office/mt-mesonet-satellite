from .Session import Session
from .Task import list_task, Task


def stop_all_tasks():
    session = Session(dot_env=False)
    tasks = list_task(session.token)
    tasks = [Task.from_response(x) for x in tasks]
    tasks = [x for x in tasks if x.status not in  ["done", "expired"]]

    if len(tasks == 0):
        return "No tasks to stop"
    
    for task in tasks:
        task.delete(session.token)

    return f"Deleted {len(tasks)} tasks"