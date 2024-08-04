from dataclasses import dataclass
import pandas as pd

from sysdata.crypto.crypto import cryptoPricesData
from sysobjects.crypto_prices import cryptoPrices, PRICE_DATA_COLUMNS
from syscore.fileutils import (
    resolve_path_and_filename_for_package,
    files_with_extension_in_pathname,
)
from syscore.constants import arg_not_supplied
from syscore.pandas.pdutils import pd_readcsv, DEFAULT_DATE_FORMAT_FOR_CSV
from syslogging.logger import *

CRYPTO_PRICES_DIRECTORY = "data.crypto"


@dataclass
class ConfigCsvCryptoPrices:
    """
        :param price_column: Column where spot crypto prices are
    :param date_column: Column where dates are
    :param date_format: Format for dates https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior

    """

    date_column: str = "DATETIME"
    date_format: str = DEFAULT_DATE_FORMAT_FOR_CSV


class csvCryptoPricesData(cryptoPricesData):
    """

    Class for crypto prices write / to from csv
    """

    def __init__(
        self,
        datapath=arg_not_supplied,
        log=get_logger("csvCryptoPricesData"),
        config: ConfigCsvCryptoPrices = arg_not_supplied,
    ):
        """
        Get crypto data from a .csv file

        :param datapath: Path where csv files are located
        :param log: logging object
        """

        super().__init__(log=log)

        if datapath is arg_not_supplied:
            datapath = CRYPTO_PRICES_DIRECTORY

        if config is arg_not_supplied:
            config = ConfigCsvCryptoPrices()

        self._datapath = datapath
        self._config = config

    def __repr__(self):
        return "csvCryptoPricesData accessing %s" % self._datapath

    @property
    def datapath(self):
        return self._datapath

    @property
    def config(self):
        return self._config

    def get_list_of_crypto_codes(self) -> list:
        return files_with_extension_in_pathname(self._datapath, ".csv")

    def _get_crypto_prices_without_checking(self, code: str) -> cryptoPrices:
        filename = self._filename_given_crypto_code(code)
        config = self.config
        price_data_columns = PRICE_DATA_COLUMNS
        date_column = config.date_column
        date_format = config.date_format

        try:
            crypto_data = pd_readcsv(
                filename, date_format=date_format, date_index_name=date_column
            )
        except OSError:
            self.log.warning(
                "Can't find currency price file %s" % filename,
                **{CURRENCY_CODE_LOG_LABEL: code},
            )
            return cryptoPrices.create_empty()

        crypto_data = pd.DataFrame(crypto_data[price_data_columns])

        crypto_data = cryptoPrices(crypto_data.sort_index())

        return crypto_data

    def _delete_crypto_prices_without_any_warning_be_careful(self, code: str):
        raise NotImplementedError(
            "You can't delete adjusted prices stored as a csv - Add to overwrite existing or delete file manually"
        )

    def _add_crypto_prices_without_checking_for_existing_entry(
        self, code: str, crypto_price_data: cryptoPrices
    ):
        filename = self._filename_given_crypto_code(code)
        config = self.config
        price_data_columns = PRICE_DATA_COLUMNS
        date_column = config.date_column
        date_format = config.date_format

        crypto_price_data.name = code
        crypto_price_data.to_csv(
            filename, index_label=date_column, date_format=date_format, header=True
        )

        self.log.debug(
            "Wrote currency prices to %s for %s" % (filename, code),
            **{CURRENCY_CODE_LOG_LABEL: code},
        )

    def _filename_given_crypto_code(self, code: str):
        return resolve_path_and_filename_for_package(self._datapath, "%s.csv" % (code))
