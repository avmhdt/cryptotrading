from collections import namedtuple
from copy import copy

import numpy as np
import pandas as pd

from syscore.exceptions import missingData
from sysobjects.contract_dates_and_expiries import contractDate
from sysobjects.dict_of_futures_per_contract_prices import (
    dictFuturesContractFinalPrices,
)
from sysobjects.roll_parameters_with_price_data import (
    find_earliest_held_contract_with_price_data,
    contractWithRollParametersAndPrices,
)
from sysobjects.rolls import rollParameters, contractDateWithRollParameters


def generate_approximate_calendar(
    roll_parameters_object: rollParameters,
    dict_of_futures_contract_prices: dictFuturesContractFinalPrices,
) -> pd.DataFrame:
    """
    Using a rollData object we work out roughly what the rolls should be (in an ideal world with available prices all the time)
      for contracts held between start_date and end_date

    Called by __init__

    :param dict_of_futures_contract_prices: dict, keys are contract date ids 'yyyymmdd'
    :param roll_parameters_object: rollData

    :return: data frame ready to be rollCalendar
    """
    try:
        earliest_contract_with_roll_data = find_earliest_held_contract_with_price_data(
            roll_parameters_object, dict_of_futures_contract_prices
        )
    except missingData:
        raise Exception("Can't find any valid starting contract!")

    approx_calendar = _create_approx_calendar_from_earliest_contract(
        earliest_contract_with_roll_data,
    )

    return approx_calendar


INDEX_NAME = "current_roll_date"


class _rollCalendarRow(dict):
    def __init__(
        self,
        current_roll_date,
        current_contract: str,
        next_contract: str,
    ):
        # a dict because pd.DataFrame can handle those
        # plus a hidden storage of the actual contract

        super().__init__({})
        if current_roll_date is not None:
            self[INDEX_NAME] = current_roll_date
            self["current_contract"] = current_contract
            self["next_contract"] = next_contract

    @property
    def roll_date(self):
        return self[INDEX_NAME]


_bad_row = _rollCalendarRow(None, None, None)


class _listOfRollCalendarRows(list):
    def to_pd_df(self):
        result = pd.DataFrame(self)
        result.index = result[INDEX_NAME]
        result = result.drop(labels=INDEX_NAME, axis=1)

        return result

    def last_roll_date(self):
        last_row = self[-1]
        return last_row.roll_date


def _create_approx_calendar_from_earliest_contract(
    earliest_contract_with_roll_data: contractWithRollParametersAndPrices,
) -> pd.DataFrame:
    roll_calendar_as_list = _listOfRollCalendarRows()

    # On the roll date we stop holding the current contract, and end up holding the next one
    # The roll date is the last day we hold the current contract
    dict_of_futures_contract_prices = earliest_contract_with_roll_data.prices
    final_contract_date_str = dict_of_futures_contract_prices.last_contract_date_str()
    current_contract = earliest_contract_with_roll_data

    while current_contract.date_str < final_contract_date_str:
        current_contract.update_expiry_with_offset_from_parameters()
        next_contract, new_row = _get_new_row_of_roll_calendar(current_contract)
        if new_row is _bad_row:
            break

        roll_calendar_as_list.append(new_row)
        current_contract = copy(next_contract)
        print(current_contract)

    roll_calendar = roll_calendar_as_list.to_pd_df()

    return roll_calendar


def _get_new_row_of_roll_calendar(
    current_contract: contractWithRollParametersAndPrices,
) -> (contractWithRollParametersAndPrices, _rollCalendarRow):
    roll_parameters = current_contract.roll_parameters
    final_contract_date_str = current_contract.prices.last_contract_date_str()

    try:
        next_contract = current_contract.find_next_held_contract_with_price_data()
    except missingData:
        # This is a problem UNLESS for the corner case where:
        # The current contract isn't the last contract
        # But the remaining contracts aren't held contracts
        if current_contract.next_held_contract().date_str > final_contract_date_str:
            # We are done
            return current_contract, _bad_row
        else:
            raise Exception(
                "Can't find good next contract date %s from data when building roll calendar using hold calendar %s"
                % (
                    current_contract.date_str,
                    str(roll_parameters.hold_rollcycle),
                )
            )

    current_roll_date = current_contract.desired_roll_date
    new_row = _rollCalendarRow(
        current_roll_date,
        current_contract.date_str,
        next_contract.date_str,
    )

    # output initial approx roll calendar to console - gives something to work with if manual adjustment
    # is needed
    print(
        f"{current_roll_date.strftime('%Y-%m-%d %H:%M:00')},{current_contract.date_str},{next_contract.date_str}"
    )
    # print(new_row)

    return next_contract, new_row


localRowData = namedtuple(
    "localRowData", ["current_row", "prev_row", "next_row", "first_row_in_data"]
)
_last_row = object()


def adjust_to_price_series(
    approx_calendar: pd.DataFrame,
    dict_of_futures_contract_prices: dictFuturesContractFinalPrices,
) -> pd.DataFrame:
    """
    Adjust an approximate roll calendar so that we have matching dates on each expiry for price, carry and next contract

    :param approx_calendar: Approximate roll calendar pd.dataFrame with columns current_contract, next_contract, carry_contract
    :param dict_of_futures_contract_prices: dict of futuresContractPrices, keys contract date eg yyyymmdd

    :return: pd.dataFrame with columns current_contract, next_contract
    """

    adjusted_roll_calendar_as_list = _listOfRollCalendarRows()
    idx_of_last_row_in_data = len(approx_calendar.index) - 1

    for row_number in range(len(approx_calendar.index)):
        local_row_data = _get_local_data_for_row_number(
            approx_calendar, row_number, idx_of_last_row_in_data
        )
        if local_row_data is _last_row:
            break

        adjusted_row = _adjust_row_of_approx_roll_calendar(
            local_row_data, dict_of_futures_contract_prices
        )

        if adjusted_row is _bad_row:
            # No suitable roll date was found for this entry. Let's try again but this time
            # without requiring prices for carry contracts to be available (Even though carry
            # contract is present the price  might not necessarily be available on otherwise
            # suitable roll dates)
            adjusted_row = _adjust_row_of_approx_roll_calendar(
                local_row_data, dict_of_futures_contract_prices
            )

            if adjusted_row is _bad_row:
                _print_roll_date_error(local_row_data)
                have_some_data_already = len(adjusted_roll_calendar_as_list) > 0
                if have_some_data_already:
                    break
                else:
                    ## not at the start yet, let's keep trying for valid data
                    _print_data_at_start_not_valid_flag(local_row_data)
                    continue

        adjusted_roll_calendar_as_list.append(adjusted_row)
        _print_adjustment_message(local_row_data, adjusted_row)

    if len(adjusted_roll_calendar_as_list) == 0:
        raise Exception(
            "Error! Empty roll calendar after adjustment! Most likely corrupted roll calendar or maybe using old roll calendar .csv files with new price data?"
        )

    new_calendar = adjusted_roll_calendar_as_list.to_pd_df()

    return new_calendar


def _get_local_data_for_row_number(
    approx_calendar: pd.DataFrame, row_number: int, idx_of_last_row_in_data: int
) -> localRowData:
    last_row_in_data = row_number == idx_of_last_row_in_data
    if last_row_in_data:
        return _last_row

    first_row_in_data = row_number == 0

    approx_row = approx_calendar.iloc[row_number, :]
    if not first_row_in_data:
        prev_approx_row = approx_calendar.iloc[row_number - 1,]
    else:
        prev_approx_row = _bad_row

    next_approx_row = approx_calendar.iloc[row_number + 1, :]

    local_row_data = localRowData(
        approx_row, prev_approx_row, next_approx_row, first_row_in_data
    )

    return local_row_data


setOfPrices = namedtuple(
    "setOfPrices",
    ["current_prices", "next_prices"],
)


def _adjust_row_of_approx_roll_calendar(
    local_row_data: localRowData,
    dict_of_futures_contract_prices: dictFuturesContractFinalPrices,
):
    roll_date, date_to_avoid = _get_roll_date_and_date_to_avoid(local_row_data)
    set_of_prices = _get_set_of_prices(
        local_row_data, dict_of_futures_contract_prices
    )
    if set_of_prices is _bad_row:
        _print_roll_date_error(local_row_data)
        return _bad_row
    try:
        adjusted_roll_date = _find_best_matching_roll_date(
            roll_date,
            set_of_prices,
            avoid_date=date_to_avoid,
        )
    except LookupError:
        return _bad_row

    adjusted_row = _get_adjusted_row(local_row_data, adjusted_roll_date)

    return adjusted_row


def _get_roll_date_and_date_to_avoid(local_row_data: localRowData):
    # This is needed to avoid double rolls
    approx_row = local_row_data.current_row
    prev_approx_row = local_row_data.prev_row
    first_row_in_data = local_row_data.first_row_in_data

    roll_date = approx_row.name
    if not first_row_in_data:
        date_to_avoid = prev_approx_row.name
    else:
        date_to_avoid = None

    return roll_date, date_to_avoid


def _get_set_of_prices(
    local_row_data: localRowData,
    dict_of_futures_contract_prices: dictFuturesContractFinalPrices,
) -> setOfPrices:
    approx_row = local_row_data.current_row

    current_contract = str(approx_row.current_contract)
    next_contract = str(approx_row.next_contract)

    try:
        current_prices = dict_of_futures_contract_prices[current_contract]
        next_prices = dict_of_futures_contract_prices[next_contract]
    except KeyError:
        return _bad_row

    set_of_prices = setOfPrices(
        current_prices, next_prices
    )

    return set_of_prices


def _print_roll_date_error(local_row_data: localRowData):
    approx_row = local_row_data.current_row
    next_approx_row = local_row_data.next_row
    current_contract = approx_row.current_contract
    next_contract = approx_row.next_contract

    print(
        "Couldn't find matching roll date for contracts %s, %s"
        % (current_contract, next_contract)
    )
    print(
        "OK if happens at the end or beginning of a roll calendar, otherwise problematic"
    )


def _find_best_matching_roll_date(
    roll_date, set_of_prices: setOfPrices, avoid_date=None
):
    """
    Find the closest valid roll date for which we have overlapping prices
    If avoid_date is passed, get the next date after that

    :param roll_date: datetime.datetime
    :param set_of_prices:
    :param avoid_date: datetime.datetime or None

    :return: datetime.datetime or
    """
    roll_date = roll_date.tz_localize(set_of_prices.current_prices.index[0].tz)

    # Get the list of dates for which a roll is possible
    paired_prices = _required_paired_prices(set_of_prices)
    valid_dates = _valid_dates_from_paired_prices(paired_prices, avoid_date)

    if len(valid_dates) == 0:
        # no matching prices
        raise LookupError("No date with a matching price")

    adjusted_date = _find_closest_valid_date_to_approx_roll_date(valid_dates, roll_date)

    return adjusted_date


def _required_paired_prices(set_of_prices: setOfPrices) -> pd.DataFrame:
    paired_prices = pd.concat(
        [set_of_prices.current_prices, set_of_prices.next_prices], axis=1
    )

    return paired_prices


def _valid_dates_from_paired_prices(paired_prices: pd.DataFrame, avoid_date):
    paired_prices_matching = _matching_prices_from_paired_prices(paired_prices)
    valid_dates = _valid_dates_from_matching_prices(paired_prices_matching, avoid_date)

    return valid_dates


def _matching_prices_from_paired_prices(paired_prices):
    paired_prices_check_match = paired_prices.apply(
        lambda xlist: not any(pd.isnull(xlist)), axis=1
    )
    paired_prices_matching = paired_prices_check_match[paired_prices_check_match]

    return paired_prices_matching


def _valid_dates_from_matching_prices(paired_prices_matching, avoid_date):
    valid_dates = paired_prices_matching.index
    valid_dates.sort_values()

    if avoid_date is not None:
        # Remove matching dates before avoid dates
        avoid_date = avoid_date.tz_localize(paired_prices_matching.index[0].tz)
        valid_dates = valid_dates[valid_dates > avoid_date]

    return valid_dates


def _find_closest_valid_date_to_approx_roll_date(valid_dates, roll_date):
    distance_to_roll = valid_dates - roll_date
    distance_to_roll_days = [
        abs(distance_item.days) for distance_item in distance_to_roll
    ]
    closest_date_index = distance_to_roll_days.index(min(distance_to_roll_days))
    adjusted_date = valid_dates[closest_date_index]

    return adjusted_date


def _get_adjusted_row(
    local_row_data: localRowData, adjusted_roll_date
) -> _rollCalendarRow:
    approx_row = local_row_data.current_row
    current_contract = approx_row.current_contract
    next_contract = approx_row.next_contract

    adjusted_row = _rollCalendarRow(
        adjusted_roll_date, current_contract, next_contract
    )

    return adjusted_row


def _print_data_at_start_not_valid_flag(local_row_data: localRowData):
    approx_row = local_row_data.current_row
    print(
        "Couldn't get good data for roll date %s but at start so truncating"
        % str(approx_row.name)
    )


def _print_adjustment_message(
    local_row_data: localRowData, adjusted_row: _rollCalendarRow
):
    print(
        "Changed date from %s to %s for row with contracts %s"
        % (
            str(local_row_data.current_row.name),
            str(adjusted_row.roll_date),
            str(adjusted_row.items()),
        )
    )

