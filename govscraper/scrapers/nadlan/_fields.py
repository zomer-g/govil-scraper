"""External API field names for www.nadlan.gov.il. Wire-format strings."""
from __future__ import annotations

# Endpoints
DEAL_DATA_URL = "https://api.nadlan.gov.il/deal-data"
DEAL_INFO_URL = "https://api.nadlan.gov.il/deal-info"
SETTLEMENT_TYPES_URL = "https://data.nadlan.gov.il/api/index/setl_types.json"

# Browser-visible URL pattern
PARCEL_VIEW_URL = "https://www.nadlan.gov.il/?view=kparcel_all&id={gush}-{chelka}&page=deals"

# Field names from /deal-data and /deal-info (camelCase as the server emits)
DEAL_KEY_GUSH = "gush"
DEAL_KEY_CHELKA = "chelka"
DEAL_KEY_SUBCHELKA = "subchelka"
DEAL_KEY_PARCEL_NUM = "parcelNum"
DEAL_KEY_SETTLEMENT_CODE = "settlementCode"
DEAL_KEY_SETTLEMENT_NAME = "settlementName"
DEAL_KEY_NEIGHBORHOOD_ID = "neighborhoodId"
DEAL_KEY_NEIGHBORHOOD_NAME = "neighborhoodName"
DEAL_KEY_ADDRESS = "address"
DEAL_KEY_STREET_CODE = "streetCode"
DEAL_KEY_DEAL_DATE = "dealDate"
DEAL_KEY_DEAL_AMOUNT = "dealAmount"
DEAL_KEY_PRICE_SM = "priceSM"
DEAL_KEY_ROOM_NUM = "roomNum"
DEAL_KEY_FLOOR = "floor"
DEAL_KEY_ASSET_AREA = "assetArea"
DEAL_KEY_YEAR_BUILT = "yearBuilt"
DEAL_KEY_BUILDING_FLOORS = "buildingFloors"
DEAL_KEY_DEAL_NATURE = "dealNature"
DEAL_KEY_HOK_HAMECHER = "hokHamecher"
DEAL_KEY_ASSET_ID = "assetId"
DEAL_KEY_ADDRESS_ID = "addressId"
DEAL_KEY_POLYGON_ID = "polygonId"

# Dedup composite key — assetId|dealDate
DEDUP_KEY_TEMPLATE = "{assetId}|{dealDate}"
