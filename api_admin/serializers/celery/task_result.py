from django_celery_results.models import TaskResult
from rest_framework import serializers


class TaskResultSerializer(serializers.ModelSerializer):
    """
    Task result serializer with all fields
    """
    task_kwargs = serializers.SerializerMethodField(method_name="normalize_kwargs")

    def normalize_kwargs(self, task_result):
        kwargs = task_result.task_kwargs
        return eval(eval(kwargs)) if kwargs else {}

    class Meta:
        model = TaskResult
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return TaskResult.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return TaskResult.objects.db_manager(database).filter(id=id).first()

    def get_all(self, sort_by, database="default"):
        return TaskResult.objects.db_manager(database).order_by(sort_by)

    def delete(self, id, database="default"):
        return TaskResult.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return TaskResult.objects.db_manager(database).filter(title=title).first()
