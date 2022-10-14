from rest_framework import pagination


class DefaultPAG(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10
