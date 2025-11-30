import yfinance as yf

from pychanlun import Chan


def test_source(chan, interval):
    source = chan.get_sources(interval)
    print(f'********** Sources[\'{interval}\']: {len(source)} **********')
    print(source.head())
    print(source.columns.values.tolist())


def test_stick(chan, interval):
    stick = chan.get_sticks(interval)
    print(f'********** Sticks[\'{interval}\']: {len(stick)} **********')
    print(stick.head())
    print(stick.columns.values.tolist())


def test_fractal(chan, interval):
    fractal = chan.get_fractals(interval)
    print(f'********** Fractals[\'{interval}\']: {len(fractal)} **********')
    print(fractal.head())
    print(fractal.columns.values.tolist())


def test_stroke(chan, interval):
    stroke = chan.get_strokes(interval)
    print(f'********** Strokes[\'{interval}\']: {len(stroke)} **********')
    print(stroke.head())
    print(stroke.columns.values.tolist())


def test_stroke_pivots(chan, interval):
    stroke_pivot = chan.get_stroke_pivots(interval)
    print(f'********** Stroke Pivots[\'{interval}\']: {len(stroke_pivot)} **********')
    print(stroke_pivot.head())
    print(stroke_pivot.columns.values.tolist())


def test_stroke_pivot_trends(chan, interval):
    stroke_pivot_trend = chan.get_stroke_pivot_trends(interval)
    print(f'********** Stroke Pivot Trends[\'{interval}\']: {len(stroke_pivot_trend)} **********')
    print(stroke_pivot_trend.head())
    print(stroke_pivot_trend.columns.values.tolist())


def test_stroke_pivot_signals(chan, interval):
    stroke_pivot_signal = chan.get_stroke_pivot_signals(interval)
    print(f'********** Stroke Signals[\'{interval}\']: {len(stroke_pivot_signal)} **********')
    print(stroke_pivot_signal.head())
    print(stroke_pivot_signal.columns.values.tolist())


def test_segment(chan, interval):
    segment = chan.get_segments(interval)
    print(f'********** Segments[\'{interval}\']: {len(segment)} **********')
    print(segment.head())
    print(segment.columns.values.tolist())


def test_segment_pivots(chan, interval):
    segment_pivot = chan.get_segment_pivots(interval)
    print(f'********** Segment Pivots[\'{interval}\']: {len(segment_pivot)} **********')
    print(segment_pivot.head())
    print(segment_pivot.columns.values.tolist())


def test_segment_pivot_trends(chan, interval):
    segment_pivot_trend = chan.get_segment_pivot_trends(interval)
    print(f'********** Segment Pivot Trends[\'{interval}\']: {len(segment_pivot_trend)} **********')
    print(segment_pivot_trend.head())
    print(segment_pivot_trend.columns.values.tolist())


def test_segment_pivot_signals(chan, interval):
    segment_pivot_signal = chan.get_segment_pivot_signals(interval)
    print(f'********** Segment Signals[\'{interval}\']: {len(segment_pivot_signal)} **********')
    print(segment_pivot_signal.head())
    print(segment_pivot_signal.columns.values.tolist())


if __name__ == '__main__':
    try:
        symbol = 'AAPL'
        interval = '1d'
        ticker = yf.Ticker(symbol)
        df = ticker.history(
            interval=interval,
            period='max',
            actions=False,
            prepost=False
        )
        chan = Chan(symbol, {interval: df})
        test_source(chan, interval)
        test_stick(chan, interval)
        test_fractal(chan, interval)
        test_stroke(chan, interval)
        test_stroke_pivots(chan, interval)
        test_stroke_pivot_trends(chan, interval)
        test_stroke_pivot_signals(chan, interval)
        test_segment(chan, interval)
        test_segment_pivots(chan, interval)
        test_segment_pivot_trends(chan, interval)
        test_segment_pivot_signals(chan, interval)
    except Exception as e:
        print(f'download source data error：{e}')
