from .bookmaker import BookmakerAPI
from .campaign import (
    CampaignAPI,
    CampaignFilterAPI,
    CampaignsAllAPI,
    HistoricCampaignAPI,
)
from .fx_conversions import FxPartnerCurrentFullConversionAPI
from .link import (
    LinkAPI,
    LinkAssignAPI,
    LinkByCampaign,
    LinksAdviserPartner,
    LinkUnassignAPI,
)
from .partner_link_accumulated import ModifyPorcentAPI
from .partnerlinkaccumulated import (
    GetUserCampaignSpecificAPI,
    PartnerLinkAccumAPI,
    RelationPartnerCampaignAPI,
    PartnerLinkAccumHistoricAPI,
)
from .partners_clicks import (
    ClicksAPI,
    PartnersForClicksAPI,
)
