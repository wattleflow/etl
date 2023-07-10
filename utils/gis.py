import re

POINT_MASK = r'POINT \(([-\d.]+) ([-\d.]+)\)'

class WattleGis:
    def __init__(self, data, pattern=POINT_MASK):
        self._pattern = pattern
        self._longitude = 0; self._latitude = 0

        match = re.match(self._pattern, data)

        if match:
            self._longitude = float(match.group(1))
            self._latitude  = float(match.group(2))

    def longitude(self):
        return self._longitude

    def latitude(self):
        return self._latitude