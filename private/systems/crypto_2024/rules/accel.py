from systems.provided.rules.ewmac import ewmac


def accel(price, vol, Lfast=4, invert: bool=False):
    Lslow = Lfast * 4
    ewmac_signal = ewmac(price, vol, Lfast, Lslow)

    accel = ewmac_signal - ewmac_signal.shift(Lfast)

    if invert:
        accel *= -1

    return accel
