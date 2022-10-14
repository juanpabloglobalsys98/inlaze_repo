from rest_framework import pagination


class AdviserManagementPaginator(pagination.LimitOffsetPagination):
    default_limit = 5
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10
