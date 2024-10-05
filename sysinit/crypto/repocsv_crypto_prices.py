"""
Copy from csv repo files to arctic for adjusted prices
"""
from syscore.constants import arg_not_supplied

from sysdata.sim.db_crypto_sim_data import dbCryptoSimData
from sysdata.csv.csv_crypto import csvCryptoPricesData

if __name__ == "__main__":
    input("Will overwrite existing prices are you sure?! CTL-C to abort")
    db_crypto_prices = dbCryptoSimData().db_crypto_prices_data

    ## MODIFY PATH TO USE SOMETHING OTHER THAN DEFAULT
    csv_crypto_datapath = arg_not_supplied
    csv_crypto_prices = csvCryptoPricesData(csv_crypto_datapath)

    instrument_code = input("Instrument code? <return for ALL instruments> ")
    if instrument_code == "":
        instrument_list = csv_crypto_prices.get_list_of_instruments()
    else:
        instrument_list = [instrument_code]

    for instrument_code in instrument_list:
        print(instrument_code)

        crypto_prices = csv_crypto_prices.get_crypto_prices(instrument_code)

        print(crypto_prices)

        db_crypto_prices.add_crypto_prices(
            instrument_code, crypto_prices, ignore_duplication=True
        )
