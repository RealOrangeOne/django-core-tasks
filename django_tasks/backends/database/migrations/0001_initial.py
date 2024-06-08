# Generated by Django 5.0.6 on 2024-06-08 08:18

import uuid

from django.db import migrations, models

import django_tasks.backends.database.models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="DBTaskResult",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("NEW", "New"),
                            ("RUNNING", "Running"),
                            ("FAILED", "Failed"),
                            ("COMPLETE", "Complete"),
                        ],
                        default="NEW",
                        max_length=8,
                    ),
                ),
                ("args_kwargs", models.JSONField()),
                ("priority", models.PositiveSmallIntegerField(null=True)),
                ("task_path", models.TextField()),
                ("queue_name", models.TextField(default="default")),
                ("backend_name", models.TextField()),
                ("run_after", models.DateTimeField(null=True)),
                ("result", models.JSONField(default=None, null=True)),
            ],
            options={
                "ordering": ["-priority", "run_after"],
            },
            bases=(django_tasks.backends.database.models.GenericBase, models.Model),
        ),
    ]
