from rest_framework import pagination


class GetAllPermissionPaginator(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllAdminsPaginator(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllRolesPaginator(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllLinksPaginator(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllPermissionGroupsPaginator(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllUserPermissions(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllWithddrawalPaginator(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAccountsReports(pagination.LimitOffsetPagination):
    default_limit = 20
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 20


class GetAllmemberReport(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllQuestionCategories(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllQuestion(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllFeedback(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetHistorialFXTax(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class BillsPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class ClickPaginator(pagination.LimitOffsetPagination):
    default_limit = 100
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 100


class CampaignsPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class OwnCompanyPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10
