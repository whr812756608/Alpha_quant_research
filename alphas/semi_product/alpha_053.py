import pandas as pd
from alpha_utl.alpha_helper import delta
from alpha_utl.alpha_to_position import*
# Alpha#53	 (-1 * delta((((close - low) - (high - close)) / (close - low)), 9))
# postive for long
# negative for short
# switch long short if switch_positions = Ture
def alpha_053(data, period, switch_positions = False):
    # Calculate the Alpha#53
    inner = (data['close'] - data['low']).replace(0, 0.0001)
    data['alpha_val'] = -1 * delta((((data['close'] - data['low']) - (data['high'] - data['close'])) / inner), period)

    pos = calculate_position(data, switch_positions)

    return pos