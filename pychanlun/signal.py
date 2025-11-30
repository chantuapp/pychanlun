from enum import IntEnum
from typing import Dict, Optional, List

import pandas as pd

from pychanlun.pivot import Pivot, Range


class SignalType(IntEnum):
    FIRST_BUY = 1
    SECOND_BUY = 2
    THIRD_BUY = 3
    FIRST_SELL = -1
    SECOND_SELL = -2
    THIRD_SELL = -3


class Signal(Pivot):

    def __init__(self, symbol: str, source: Dict[str, pd.DataFrame]):
        self.stroke_signals: Dict[str, Optional[pd.DataFrame]] = {}
        self.segment_signals: Dict[str, Optional[pd.DataFrame]] = {}
        super().__init__(symbol, source)

    def _process_interval(self, interval: str) -> None:
        super()._process_interval(interval)

        stroke_df = self.strokes[interval]
        stroke_pivot_df = self.stroke_pivots[interval]
        if stroke_df is not None or stroke_pivot_df is not None:
            self.stroke_signals[interval] = self._generate_signals(stroke_df, stroke_pivot_df)
        else:
            self.stroke_signals[interval] = None

        segment_df = self.segments[interval]
        segment_pivot_df = self.segment_pivots[interval]
        if segment_df is not None or segment_pivot_df is not None:
            self.segment_signals[interval] = self._generate_signals(segment_df, segment_pivot_df)
        else:
            self.segment_signals[interval] = None

    def _generate_signals(self, segment_df: pd.DataFrame, pivot_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        segment_df = pd.DataFrame(segment_df)
        segment_df['signal'] = 0

        pivots = list(pivot_df.itertuples())
        rows = []
        for index in range(0, len(pivots) - 3, 2):
            curr_pivot = pivots[index]
            if curr_pivot.divergence <= 0:
                continue

            next_range = self._get_range(pivots, index + 2)
            self._check_first_second_signals(rows, segment_df, next_range)
            self._check_third_signals(rows, segment_df, next_range)

        return self.to_dataframe(rows, ['high', 'low', 'signal'])

    def _check_first_second_signals(self, rows: List, segment_df: pd.DataFrame, next_range: Range) -> None:
        segments = list(segment_df.loc[next_range.start.Index:].itertuples())
        if len(segments) < 3:
            return

        next_segment_1 = segments[0]
        next_segment_3 = segments[2]

        if self.is_top(next_segment_1) and next_segment_3.high < next_segment_1.high:
            rows.append(next_segment_1._replace(signal=SignalType.FIRST_SELL))
            rows.append(next_segment_3._replace(signal=SignalType.SECOND_SELL))
        elif self.is_bottom(next_segment_1) and next_segment_3.low > next_segment_1.low:
            rows.append(next_segment_1._replace(signal=SignalType.FIRST_BUY))
            rows.append(next_segment_3._replace(signal=SignalType.SECOND_BUY))

    def _check_third_signals(self, rows: List, segment_df: pd.DataFrame, next_range: Range) -> None:
        segments = list(segment_df.loc[next_range.end.Index:].itertuples())
        if len(segments) < 3:
            return

        next_segment_2 = segments[1]
        next_segment_3 = segments[2]

        if self.is_top(next_segment_2) and next_segment_3.low > next_range.high:
            rows.append(next_segment_3._replace(signal=SignalType.THIRD_BUY))
        elif self.is_bottom(next_segment_2) and next_segment_3.low < next_range.low:
            rows.append(next_segment_3._replace(signal=SignalType.THIRD_SELL))
