from rest_framework import pagination


class GetAllBookamkers(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllCampaigns(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllLinks(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllMemberReport(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllMemberReportMultiFx(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllBanUnbanCodeReasonPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllInactiveActiveCodeReasonPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class OwnCompanyPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllCpas(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class CpaPrevNotBillPag(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class RolesPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetTokensAuth(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class AdminsManagementPaginator(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class ReferredManagementPaginator(pagination.LimitOffsetPagination):
    default_limit = 5
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetInfoApplicants(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAllRelationPartnerCampaigns(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10


class GetAlltypeMessage(pagination.LimitOffsetPagination):
    default_limit = 10
    limit_query_param = 'lim'
    offset_query_param = 'offs'
    max_limit = 10
