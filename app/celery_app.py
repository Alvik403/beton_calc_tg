from __future__ import annotations

import os

from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

app = Celery(
    "beton",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.tasks"],
)

app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["json"]
app.conf.result_expires = 3600  # 1 hour
app.conf.task_acks_late = True
