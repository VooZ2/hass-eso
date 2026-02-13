import logging
from datetime import timedelta, datetime
import asyncio
import homeassistant.helpers.config_validation as cv
import voluptuous as vol

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import StatisticMetaData, StatisticData
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    statistics_during_period,
)

from homeassistant.const import (
    CONF_ID,
    CONF_NAME,
    CONF_USERNAME,
    CONF_PASSWORD,
    UnitOfEnergy,
    EVENT_HOMEASSISTANT_STARTED,
)

from homeassistant.core import HomeAssistant, Event
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.typing import ConfigType
from homeassistant.util import dt as dt_util

from .eso_client import ESOClient

_LOGGER = logging.getLogger(__name__)

DOMAIN = "eso"
CONF_OBJECTS = "objects"
CONF_CONSUMED = "consumed"
CONF_RETURNED = "returned"
CONF_COST = "cost"
CONF_PRICE_ENTITY = "price_entity"
CONF_PRICE_CURRENCY = "price_currency"

POWER_CONSUMED = "P+"
POWER_RETURNED = "P-"

ENERGY_TYPE_MAP = {
    CONF_CONSUMED: POWER_CONSUMED,
    CONF_RETURNED: POWER_RETURNED,
}

OBJECT_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_NAME): cv.string,
        vol.Required(CONF_ID): cv.string,
        vol.Required(CONF_CONSUMED, default=True): cv.boolean,
        vol.Required(CONF_RETURNED, default=False): cv.boolean,
        vol.Optional(CONF_PRICE_ENTITY): cv.string,
        vol.Optional(CONF_PRICE_CURRENCY, default="EUR"): cv.string,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Required(CONF_USERNAME): cv.string,
                vol.Required(CONF_PASSWORD): cv.string,
                vol.Required(CONF_OBJECTS): cv.ensure_list(OBJECT_SCHEMA),
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)

RETRY_DELAY_SECONDS = 3 * 3600


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    if DOMAIN not in config:
        return True

    hass.data.setdefault(DOMAIN, config[DOMAIN])

    client = ESOClient(
        username=config[DOMAIN][CONF_USERNAME],
        password=config[DOMAIN][CONF_PASSWORD],
    )

    async def async_import_generation(now: datetime, retry: bool = False) -> None:
        if hass.is_stopping:
            return

        all_failed = False

        try:
            _LOGGER.info("Logging in to ESO...")
            await hass.async_add_executor_job(client.login)
        except Exception as e:
            _LOGGER.error("ESO login error: %s", e)
            all_failed = True

        for obj in config[DOMAIN][CONF_OBJECTS]:
            try:
                _LOGGER.info("Fetching ESO dataset [%s]", obj[CONF_NAME])
                await hass.async_add_executor_job(
                    client.fetch_dataset,
                    obj[CONF_ID],
                    now,
                )
            except Exception as e:
                _LOGGER.error("ESO fetch error [%s]: %s", obj[CONF_NAME], e)
                all_failed = True
                continue

            dataset = client.get_dataset(obj[CONF_ID])
            await async_insert_statistics(hass, obj, dataset)

            if CONF_PRICE_ENTITY in obj and obj[CONF_PRICE_ENTITY]:
                await async_insert_cost_statistics(hass, obj, dataset)

        if all_failed and not retry:
            _LOGGER.warning("ESO fetch failed, will retry in %s seconds", RETRY_DELAY_SECONDS)
            hass.loop.call_later(
                RETRY_DELAY_SECONDS,
                lambda: asyncio.create_task(
                    async_import_generation(dt_util.now(), retry=True)
                ),
            )

    async_track_time_change(hass, async_import_generation, hour=7, minute=00, second=0)

    async def _started_import(event: Event) -> None:
        await asyncio.sleep(30)
        _LOGGER.info("HA startavo: pradedamas ESO duomenų importas po restarto")
        await async_import_generation(dt_util.now())

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _started_import)

    return True


async def async_insert_statistics(hass: HomeAssistant, obj: dict, dataset: dict) -> None:
    for data_type in [CONF_CONSUMED, CONF_RETURNED]:
        if not obj.get(data_type):
            continue

        statistic_id = f"{DOMAIN}:energy_{data_type}_{obj[CONF_ID]}"
        mapped_type = ENERGY_TYPE_MAP[data_type]

        if not dataset or mapped_type not in dataset:
            continue

        generation_data = dataset[mapped_type]

        metadata = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name=f"{obj[CONF_NAME]} ({data_type})",
            source=DOMAIN,
            statistic_id=statistic_id,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
            device_class="energy",
        )

        statistics = await _async_get_statistics(hass, statistic_id, generation_data)
        async_add_external_statistics(hass, metadata, statistics)


async def _async_get_statistics(
    hass: HomeAssistant, statistic_id: str, generation_data: dict
) -> list[StatisticData]:

    statistics: list[StatisticData] = []
    sum_ = None

    for ts, kwh in generation_data.items():
        dt_object = datetime.fromtimestamp(ts, tz=dt_util.get_time_zone("Europe/Vilnius"))

        if sum_ is None:
            sum_ = await get_previous_sum(hass, statistic_id, dt_object)

        sum_ += kwh

        statistics.append(
            StatisticData(
                start=dt_object,
                state=kwh,
                sum=sum_,
            )
        )

    return statistics


async def get_previous_sum(
    hass: HomeAssistant, statistic_id: str, date: datetime
) -> float:

    start = date - timedelta(hours=1)
    end = date

    stat = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        start,
        end,
        {statistic_id},
        "hour",
        None,
        {"sum"},
    )

    if statistic_id not in stat or not stat[statistic_id]:
        return 0.0

    return stat[statistic_id][0].get("sum", 0.0)


async def async_insert_cost_statistics(
    hass: HomeAssistant, obj: dict, consumption_dataset: dict
) -> None:

    if not obj.get(CONF_CONSUMED):
        return

    cons_dataset = consumption_dataset[ENERGY_TYPE_MAP[CONF_CONSUMED]]

    start_time = datetime.fromtimestamp(
        min(cons_dataset.keys()), tz=dt_util.get_time_zone("Europe/Vilnius")
    )
    end_time = datetime.fromtimestamp(
        max(cons_dataset.keys()), tz=dt_util.get_time_zone("Europe/Vilnius")
    )

    prices = await _async_generate_price_dict(hass, obj, start_time, end_time)

    if not prices:
        return

    statistic_id = f"{DOMAIN}:energy_{CONF_COST}_{obj[CONF_ID]}"

    cost_metadata = StatisticMetaData(
        has_mean=False,
        has_sum=True,
        name=f"{obj[CONF_NAME]} ({CONF_COST})",
        source=DOMAIN,
        statistic_id=statistic_id,
        unit_of_measurement=obj[CONF_PRICE_CURRENCY],
        device_class="monetary",
    )

    cost_stats: list[StatisticData] = []
    cost_sum_ = None

    for ts, cons_kwh in cons_dataset.items():
        dt_object = datetime.fromtimestamp(
            ts, tz=dt_util.get_time_zone("Europe/Vilnius")
        )

        price = prices.get(ts, 0)
        cost = round(cons_kwh * price, 5)

        if cost_sum_ is None:
            cost_sum_ = await get_previous_sum(
                hass, statistic_id, start_time
            )

        cost_sum_ += cost

        cost_stats.append(
            StatisticData(
                start=dt_object,
                state=cost,
                sum=cost_sum_,
            )
        )

    async_add_external_statistics(hass, cost_metadata, cost_stats)


async def _async_generate_price_dict(
    hass: HomeAssistant, obj: dict, time_from: datetime, time_to: datetime
) -> dict:

    stat = await get_instance(hass).async_add_executor_job(
        statistics_during_period,
        hass,
        time_from,
        time_to,
        {obj[CONF_PRICE_ENTITY]},
        "hour",
        None,
        {"state"},
    )

    price_stats = stat.get(obj[CONF_PRICE_ENTITY])

    if not price_stats:
        return {}

    prices = {}

    for rec in price_stats:
        prices[int(rec["start"].timestamp())] = rec["state"]

    return prices
