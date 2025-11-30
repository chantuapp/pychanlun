from dataclasses import dataclass
from typing import Dict, Optional, Any, List, Tuple

import pandas as pd

from pychanlun.segment import Segment


@dataclass
class Range:
    start: Any
    end: Any
    high: float
    low: float


class Pivot(Segment):

    def __init__(self, symbol: str, source: Dict[str, pd.DataFrame]):
        self.stroke_pivots: Dict[str, Optional[pd.DataFrame]] = {}
        self.segment_pivots: Dict[str, Optional[pd.DataFrame]] = {}
        super().__init__(symbol, source)

    def _process_interval(self, interval: str) -> None:
        super()._process_interval(interval)

        source_df = self.sources[interval]
        stroke_df = self.strokes[interval]
        if source_df is not None and stroke_df is not None:
            self.stroke_pivots[interval] = self._identify_pivots(source_df, stroke_df)
        else:
            self.stroke_pivots[interval] = None

        segment_df = self.segments[interval]
        if source_df is not None and segment_df is not None:
            self.segment_pivots[interval] = self._identify_pivots(source_df, segment_df)
        else:
            self.segment_pivots[interval] = None

    def _identify_pivots(self, source_df: pd.DataFrame, segment_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        segment_df = pd.DataFrame(segment_df)
        segment_df['price'] = segment_df['macd'] = segment_df['trend'] = segment_df['divergence'] = 0

        segments = list(segment_df.itertuples())
        rows = self._find_pivots(segments)

        self._merge_overlapping_pivots(rows)
        self._set_pivot_metrics(rows, source_df)
        return self.to_dataframe(rows, ['high', 'low', 'price', 'macd', 'trend', 'divergence'])

    def _find_pivots(self, segments: List) -> List:
        rows = []
        index = 0
        while index < len(segments) - 4:
            curr_segment = segments[index]
            range_1 = self._get_range(segments, index + 1)
            range_2 = self._get_range(segments, index + 3)
            pivot = self._calculate_pivot_zone(range_1, range_2)

            if not self._can_initiate_pivot(pivot, curr_segment):
                index += 1
                continue
            if not self._ranges_overlap(range_1, range_2):
                index += 1
                continue

            steps = self._extend_pivots(segments, index, pivot)

            last_segment = segments[index + steps + 4]
            next_segment = self._update_pivot_start(range_1.start, pivot)
            last_segment = self._update_pivot_end(last_segment, pivot)

            rows.append(next_segment)
            rows.append(last_segment)
            index += steps + 4
        return rows

    def _extend_pivots(self, segments: List, index: int, pivot: Range) -> int:
        step = 0
        while index + step < len(segments) - 7:
            next_range = self._get_range(segments, index + step + 5)
            if self._is_outside_pivot(pivot, next_range):
                break
            step += 2
        return step

    def _merge_overlapping_pivots(self, rows: List) -> None:
        index = 0
        while index < len(rows) - 3:
            pivot_1 = self._get_range(rows, index)
            pivot_2 = self._get_range(rows, index + 2)

            if not self._ranges_overlap(pivot_1, pivot_2):
                index += 2
                continue

            merged_start = self._merge_pivot_start(pivot_1, pivot_2)
            merged_end = self._merge_pivot_end(pivot_1, pivot_2)

            rows[index] = merged_start
            rows[index + 3] = merged_end
            rows.remove(pivot_2.start)
            rows.remove(pivot_1.end)

    def _set_pivot_metrics(self, rows: List, source_df: pd.DataFrame) -> None:
        level = 0
        for i in range(0, len(rows) - 3, 2):
            pivot_1 = self._get_range(rows, i)
            pivot_2 = self._get_range(rows, i + 2)

            self._set_pivot_macd(pivot_1, pivot_2, source_df)
            level = self._set_pivot_trend(pivot_1, pivot_2, level)

            rows[i] = pivot_1.start
            rows[i + 1] = pivot_1.end
            rows[i + 2] = pivot_2.start
            rows[i + 3] = pivot_2.end

        if len(rows) > 0:
            last = self._get_range(rows, -2)
            self._set_pivot_macd(last, None, source_df)
            rows[-1] = last.end

    @staticmethod
    def _set_pivot_macd(pivot_1: Optional[Range], pivot_2: Optional[Range], source_df: pd.DataFrame) -> None:
        mask = pd.Series([True] * len(source_df), index=source_df.index)
        if pivot_1 is not None:
            mask &= source_df.index >= pivot_1.end.Index
        if pivot_2 is not None:
            mask &= source_df.index <= pivot_2.start.Index

        macd_sum = source_df[mask].macd_dif.sum()
        if pivot_1 is not None:
            pivot_1.end = pivot_1.end._replace(macd=macd_sum)
        if pivot_2 is not None:
            pivot_2.start = pivot_2.start._replace(macd=macd_sum)

    def _set_pivot_trend(self, pivot_1: Range, pivot_2: Range, trend: int) -> int:
        if trend == 0:
            pivot_1.start = pivot_1.start._replace(trend=0)
            pivot_1.end = pivot_1.end._replace(trend=1)

        if pivot_2.high > pivot_1.high:
            pivot_1.end = pivot_1.end._replace(price=pivot_1.low)
            if trend < 0:
                trend = 0
            trend += 1
            pivot_2.start = pivot_2.start._replace(trend=trend, price=pivot_2.high)
            pivot_2.end = pivot_2.end._replace(trend=trend + 1)
        else:
            pivot_1.end = pivot_1.end._replace(price=pivot_1.high)
            if trend > 0:
                trend = 0
            trend -= 1
            pivot_2.start = pivot_2.start._replace(trend=trend, price=pivot_2.low)
            pivot_2.end = pivot_2.end._replace(trend=trend - 1)

        self._detect_pivot_divergence(pivot_1, pivot_2)
        return trend

    @staticmethod
    def _detect_pivot_divergence(pivot_1: Range, pivot_2: Range) -> None:
        divergence = 0
        if pivot_1.start.trend < pivot_2.start.trend > 0:
            divergence = 1 if pivot_1.start.macd > pivot_1.end.macd else 0
        elif pivot_1.start.trend > pivot_2.start.trend < 0:
            divergence = 1 if pivot_1.start.macd < pivot_1.end.macd else 0

        pivot_1.start = pivot_1.start._replace(divergence=divergence)
        pivot_1.end = pivot_1.end._replace(divergence=divergence)

    @staticmethod
    def _calculate_pivot_zone(range_1: Range, range_2: Range) -> Range:
        return Range(
            start=range_1.start,
            end=range_2.end,
            high=min(range_1.high, range_2.high),
            low=max(range_1.low, range_2.low)
        )

    def _can_initiate_pivot(self, pivot: Range, curr_segment: Tuple) -> bool:
        if self.is_top(curr_segment):
            return curr_segment.high > pivot.high
        elif self.is_bottom(curr_segment):
            return curr_segment.low < pivot.low
        return False

    @staticmethod
    def _ranges_overlap(range_1: Range, range_2: Range) -> bool:
        return range_2.high > range_1.low and range_2.low < range_1.high

    @staticmethod
    def _is_outside_pivot(pivot: Range, range: Range) -> bool:
        return range.high < pivot.low or range.low > pivot.high

    def _update_pivot_start(self, segment: Tuple, pivot: Range) -> Tuple:
        if self.is_top(segment):
            return segment._replace(high=pivot.high)
        else:
            return segment._replace(low=pivot.low)

    def _update_pivot_end(self, segment: Tuple, pivot: Range) -> Tuple:
        if self.is_top(segment):
            return segment._replace(low=pivot.low)
        else:
            return segment._replace(high=pivot.high)

    def _merge_pivot_start(self, pivot_1: Range, pivot_2: Range) -> Tuple:
        if self.is_top(pivot_1.start):
            high = min(pivot_1.start.high, pivot_2.start.high)
            return pivot_1.start._replace(high=high)
        else:
            low = max(pivot_1.start.low, pivot_2.start.low)
            return pivot_1.start._replace(low=low)

    def _merge_pivot_end(self, pivot_1: Range, pivot_2: Range) -> Tuple:
        if self.is_top(pivot_1.end):
            low = max(pivot_1.end.low, pivot_2.end.low)
            return pivot_2.end._replace(low=low)
        else:
            high = min(pivot_1.end.high, pivot_2.end.high)
            return pivot_2.end._replace(high=high)

    def _get_range(self, rows: List, index: int) -> Range:
        start, end = rows[index], rows[index + 1]
        high = start.high if self.is_top(start) else end.high
        low = start.low if self.is_bottom(start) else end.low
        return Range(start, end, high, low)
