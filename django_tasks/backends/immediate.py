from functools import partial
from inspect import iscoroutinefunction
from typing import TypeVar
from uuid import uuid4

from asgiref.sync import async_to_sync
from django.db import transaction
from django.utils import timezone
from typing_extensions import ParamSpec

from django_tasks.task import ResultStatus, Task, TaskResult
from django_tasks.utils import json_normalize

from .base import BaseTaskBackend

T = TypeVar("T")
P = ParamSpec("P")


class ImmediateBackend(BaseTaskBackend):
    supports_async_task = True

    def _execute_task(self, task_result: TaskResult) -> None:
        """
        Execute the task for the given `TaskResult`, mutating it with the outcome
        """
        calling_task_func = (
            async_to_sync(task_result.task.func)
            if iscoroutinefunction(task_result.task.func)
            else task_result.task.func
        )

        try:
            task_result._result = json_normalize(
                calling_task_func(*task_result.args, **task_result.kwargs)
            )
            task_result.status = ResultStatus.COMPLETE
        except Exception:
            task_result._result = None
            task_result.status = ResultStatus.FAILED

        task_result.finished_at = timezone.now()

    def enqueue(
        self, task: Task[P, T], args: P.args, kwargs: P.kwargs
    ) -> TaskResult[T]:
        self.validate_task(task)

        task_result = TaskResult[T](
            task=task,
            id=str(uuid4()),
            status=ResultStatus.NEW,
            enqueued_at=timezone.now(),
            finished_at=None,
            args=json_normalize(args),
            kwargs=json_normalize(kwargs),
            backend=self.alias,
        )

        if self._get_enqueue_on_commit_for_task(task) is not False:
            transaction.on_commit(partial(self._execute_task, task_result))
        else:
            self._execute_task(task_result)

        return task_result
