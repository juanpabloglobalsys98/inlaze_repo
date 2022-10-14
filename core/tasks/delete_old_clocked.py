from betenlace.celery import app
from celery.utils.log import get_task_logger
from django.db.models import Q
from django.utils import timezone
from django_celery_beat.models import (
    ClockedSchedule,
    PeriodicTask,
)

logger_task = get_task_logger(__name__)


@app.task
def delete_old_clocked():
    """
    Delete all old tasks that was scheduled via clock based on current 
    time (always make comparision on UTC)
    """
    filters = [Q(clocked_time__lt=timezone.now())]
    clocked_ids = ClockedSchedule.objects.filter(
        *filters).values_list('id', flat=True)

    filters = [Q(clocked_id__in=clocked_ids)]
    PeriodicTask.objects.filter(*filters).delete()


@app.on_after_finalize.connect
def delete_old_clocked_signal(*args, **kwargs):
    filters = [Q(clocked_time__lt=timezone.now())]
    clocked_ids = ClockedSchedule.objects.filter(
        *filters).values_list('id', flat=True)

    filters = [Q(clocked_id__in=clocked_ids)]
    PeriodicTask.objects.filter(*filters).delete()
