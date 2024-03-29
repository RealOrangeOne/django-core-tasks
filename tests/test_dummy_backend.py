import dataclasses
import inspect
from datetime import datetime

from django.test import SimpleTestCase, override_settings
from django.utils import timezone

from django_core_tasks import TaskCandidate, TaskStatus, default_task_backend, tasks
from django_core_tasks.backends.base import BaseTaskBackend
from django_core_tasks.backends.dummy import DummyBackend
from django_core_tasks.exceptions import InvalidTaskError, TaskDoesNotExist

from . import tasks as test_tasks


@override_settings(
    TASKS={"default": {"BACKEND": "django_core_tasks.backends.dummy.DummyBackend"}}
)
class DummyBackendTestCase(SimpleTestCase):
    def setUp(self):
        default_task_backend.clear()

    def test_using_correct_backend(self):
        self.assertEqual(default_task_backend, tasks["default"])
        self.assertIsInstance(tasks["default"], DummyBackend)

    def test_enqueue_task(self):
        self.assertTrue(default_task_backend.supports_enqueue())

        task = default_task_backend.enqueue(test_tasks.noop_task)

        self.assertEqual(task.status, TaskStatus.NEW)
        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018
        self.assertIsNone(task.when)
        self.assertEqual(task.func, test_tasks.noop_task)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    async def test_enqueue_task_async(self):
        task = await default_task_backend.aenqueue(test_tasks.noop_task)

        self.assertEqual(task.status, TaskStatus.NEW)
        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018
        self.assertEqual(task.func, test_tasks.noop_task)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    def test_executes_async_task(self):
        task = default_task_backend.enqueue(test_tasks.noop_task_async)

        self.assertEqual(task.status, TaskStatus.NEW)
        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018
        self.assertEqual(task.func, test_tasks.noop_task_async)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    async def test_executes_async_task_async(self):
        task = await default_task_backend.aenqueue(test_tasks.noop_task_async)

        self.assertEqual(task.status, TaskStatus.NEW)

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018

        self.assertEqual(task.func, test_tasks.noop_task_async)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    def test_defer_task(self):
        self.assertTrue(default_task_backend.supports_defer())

        when = timezone.now()

        task = default_task_backend.defer(test_tasks.noop_task, when=when)

        self.assertEqual(task.status, TaskStatus.NEW)

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018

        self.assertEqual(task.when, when)
        self.assertEqual(task.func, test_tasks.noop_task)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    async def test_defer_task_async(self):
        when = timezone.now()

        task = await default_task_backend.adefer(test_tasks.noop_task, when=when)

        self.assertEqual(task.status, TaskStatus.NEW)

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018

        self.assertEqual(task.func, test_tasks.noop_task)
        self.assertEqual(task.when, when)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    def test_defer_async_task(self):
        when = timezone.now()

        task = default_task_backend.defer(test_tasks.noop_task_async, when=when)

        self.assertEqual(task.status, TaskStatus.NEW)

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018

        self.assertEqual(task.func, test_tasks.noop_task_async)
        self.assertEqual(task.when, when)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    async def test_defer_async_task_async(self):
        when = timezone.now()

        task = await default_task_backend.adefer(test_tasks.noop_task_async, when=when)

        self.assertEqual(task.status, TaskStatus.NEW)

        with self.assertRaisesMessage(ValueError, "Task has not finished yet"):
            task.result  # noqa:B018

        self.assertEqual(task.func, test_tasks.noop_task_async)
        self.assertEqual(task.when, when)
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})

        self.assertEqual(default_task_backend.tasks, [task])

    def test_get_task(self):
        task = default_task_backend.enqueue(test_tasks.noop_task)

        new_task = default_task_backend.get_task(task.id)

        self.assertIs(task, new_task)

    async def test_get_task_async(self):
        task = await default_task_backend.aenqueue(test_tasks.noop_task)

        new_task = await default_task_backend.aget_task(task.id)

        self.assertIs(task, new_task)

    async def test_get_invalid_task(self):
        with self.assertRaises(TaskDoesNotExist):
            default_task_backend.get_task("123")

        with self.assertRaises(TaskDoesNotExist):
            await default_task_backend.aget_task("123")

    async def test_refresh_task(self):
        task = default_task_backend.enqueue(test_tasks.noop_task)

        original_task = dataclasses.asdict(task)

        task.refresh()

        self.assertEqual(dataclasses.asdict(task), original_task)

        await task.arefresh()

        self.assertEqual(dataclasses.asdict(task), original_task)

    def test_enqueue_signature(self):
        self.assertEqual(
            inspect.signature(DummyBackend.enqueue),
            inspect.signature(BaseTaskBackend.enqueue),
        )

    def test_defer_signature(self):
        self.assertEqual(
            inspect.signature(DummyBackend.defer),
            inspect.signature(BaseTaskBackend.defer),
        )

    async def test_naive_datetime(self):
        with self.assertRaisesMessage(
            TypeError, "can't compare offset-naive and offset-aware datetimes"
        ):
            default_task_backend.defer(test_tasks.noop_task, when=datetime.now())

        with self.assertRaisesMessage(
            TypeError, "can't compare offset-naive and offset-aware datetimes"
        ):
            await default_task_backend.adefer(test_tasks.noop_task, when=datetime.now())

    async def test_enqueue_invalid_task(self):
        with self.assertRaises(InvalidTaskError):
            default_task_backend.enqueue(lambda: True)

        with self.assertRaises(InvalidTaskError):
            await default_task_backend.aenqueue(lambda: True)

    async def test_defer_invalid_task(self):
        with self.assertRaises(InvalidTaskError):
            default_task_backend.defer(lambda: True, when=timezone.now())

        with self.assertRaises(InvalidTaskError):
            await default_task_backend.adefer(lambda: True, when=timezone.now())

    async def test_invalid_priority(self):
        with self.assertRaisesMessage(ValueError, "priority must be positive"):
            default_task_backend.enqueue(test_tasks.noop_task, priority=0)

        with self.assertRaisesMessage(ValueError, "priority must be positive"):
            await default_task_backend.aenqueue(test_tasks.noop_task, priority=0)

        with self.assertRaisesMessage(ValueError, "priority must be positive"):
            default_task_backend.defer(
                test_tasks.noop_task, when=timezone.now(), priority=0
            )

        with self.assertRaisesMessage(ValueError, "priority must be positive"):
            await default_task_backend.adefer(
                test_tasks.noop_task, when=timezone.now(), priority=0
            )

    def test_bulk_enqueue_tasks(self):
        created_tasks = default_task_backend.bulk_enqueue(
            [
                TaskCandidate(test_tasks.calculate_meaning_of_life),
                TaskCandidate(test_tasks.noop_task),
            ]
        )

        self.assertEqual(len(created_tasks), 2)
        self.assertEqual(default_task_backend.tasks, created_tasks)

        self.assertEqual(created_tasks[0].func, test_tasks.calculate_meaning_of_life)
        self.assertEqual(created_tasks[1].func, test_tasks.noop_task)

        self.assertEqual(
            created_tasks[0].status,
            TaskStatus.NEW,
        )
        self.assertEqual(
            created_tasks[1].status,
            TaskStatus.NEW,
        )

    async def test_bulk_enqueue_tasks_async(self):
        created_tasks = await default_task_backend.abulk_enqueue(
            [
                TaskCandidate(test_tasks.calculate_meaning_of_life),
                TaskCandidate(test_tasks.noop_task),
            ]
        )

        self.assertEqual(len(created_tasks), 2)
        self.assertEqual(default_task_backend.tasks, created_tasks)

        self.assertEqual(created_tasks[0].func, test_tasks.calculate_meaning_of_life)
        self.assertEqual(created_tasks[1].func, test_tasks.noop_task)

        self.assertEqual(created_tasks[0].status, TaskStatus.NEW)
        self.assertEqual(created_tasks[1].status, TaskStatus.NEW)

    def test_bulk_defer_tasks(self):
        created_tasks = default_task_backend.bulk_defer(
            [
                TaskCandidate(
                    test_tasks.calculate_meaning_of_life, when=timezone.now()
                ),
                TaskCandidate(test_tasks.noop_task, when=timezone.now()),
            ]
        )

        self.assertEqual(len(created_tasks), 2)
        self.assertEqual(default_task_backend.tasks, created_tasks)

        self.assertEqual(
            created_tasks[0].func,
            test_tasks.calculate_meaning_of_life,
        )
        self.assertEqual(created_tasks[1].func, test_tasks.noop_task)

        self.assertEqual(
            created_tasks[0].status,
            TaskStatus.NEW,
        )
        self.assertEqual(
            created_tasks[1].status,
            TaskStatus.NEW,
        )

    async def test_bulk_defer_tasks_async(self):
        created_tasks = await default_task_backend.abulk_defer(
            [
                TaskCandidate(
                    test_tasks.calculate_meaning_of_life, when=timezone.now()
                ),
                TaskCandidate(test_tasks.noop_task, when=timezone.now()),
            ]
        )

        self.assertEqual(len(created_tasks), 2)
        self.assertEqual(default_task_backend.tasks, created_tasks)

        self.assertEqual(created_tasks[0].func, test_tasks.calculate_meaning_of_life)
        self.assertEqual(created_tasks[1].func, test_tasks.noop_task)

        self.assertEqual(created_tasks[0].status, TaskStatus.NEW)
        self.assertEqual(created_tasks[1].status, TaskStatus.NEW)
