from rest_framework import pagination


class DefaultPAG(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class PartnersPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class BillsPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class PartnerBillingPaginator(pagination.LimitOffsetPagination):
    default_limit = 5
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class FXratePaginator(pagination.LimitOffsetPagination):
    default_limit = 50
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 50


class FXratePercentagePaginator(pagination.LimitOffsetPagination):
    default_limit = 50
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 50


class MinWithdrawalPartnerMoneyPaginator(pagination.LimitOffsetPagination):
    default_limit = 50
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 50


class ClockedSchedulePaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class IntervalSchedulePaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class CrontabSchedulePaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class PeriodicTaskPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class TaskResultPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class PartnersForClicPaginator(pagination.LimitOffsetPagination):
    default_limit = 5
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class CampaignsPaginator(pagination.LimitOffsetPagination):
    default_limit = 5
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class ClickPaginator(pagination.LimitOffsetPagination):
    default_limit = 1000
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 1000
