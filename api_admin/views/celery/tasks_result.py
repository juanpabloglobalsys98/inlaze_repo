import logging

from api_admin.helpers import DB_ADMIN, TaskResultPaginator
from api_admin.serializers import TaskResultSerializer
from cerberus import Validator
from core.helpers import HavePermissionBasedView, StandardErrorHandler
from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class TaskResultAPI(APIView, TaskResultPaginator):

    permission_classes = (
        IsAuthenticated, 
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin gets the tasks results in the system
        """

        sort_by_regex = "\-?id|\-?task_id|\-?task_name|\-?task_args|\-?task_kwargs|\-?status|\-?worker" + \
            "|\-?content_type|\-?content_encoding|\-?result|\-?date_created|\-?date_done|\-?traceback" + \
            "|\-?meta"

        validator = Validator(
            {
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "regex": sort_by_regex
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        sort_by = validator.document.get("sort_by")
        if sort_by == None:
            sort_by = "-id"

        task_results = TaskResultSerializer().get_all(sort_by, DB_ADMIN)
        if task_results:
            task_results = self.paginate_queryset(task_results, request, view=self)
            task_results = TaskResultSerializer(instance=task_results, many=True)

        return Response(
            data={"task_results": task_results.data if task_results else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if task_results else None
        )
