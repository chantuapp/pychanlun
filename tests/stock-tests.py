import yfinance as yf

from pychanlun.signal import Signal


def test_sources(stock, interval):
    source = stock.sources[interval]
    print(f'********** Sources[\'{interval}\']: {len(source)} **********')
    print(source.head())
    print(source.columns.values.tolist())


def test_sticks(stock, interval):
    stick = stock.sticks[interval]
    print(f'********** Sticks[\'{interval}\']: {len(stick)} **********')
    print(stick.head())
    print(stick.columns.values.tolist())


def test_fractals(stock, interval):
    fractal = stock.fractals[interval]
    print(f'********** Fractals[\'{interval}\']: {len(fractal)} **********')
    print(fractal.head())
    print(fractal.columns.values.tolist())


def test_strokes(stock, interval):
    stroke = stock.strokes[interval]
    print(f'********** Strokes[\'{interval}\']: {len(stroke)} **********')
    print(stroke.head())
    print(stroke.columns.values.tolist())


def test_stroke_pivots(stock, interval):
    stroke_pivot = stock.stroke_pivots[interval]
    print(f'********** Stroke Pivots[\'{interval}\']: {len(stroke_pivot)} **********')
    print(stroke_pivot.head())
    print(stroke_pivot.columns.values.tolist())


def test_stroke_signals(stock, interval):
    stroke_signal = stock.stroke_signals[interval]
    print(f'********** Stroke Signals[\'{interval}\']: {len(stroke_signal)} **********')
    print(stroke_signal.head())
    print(stroke_signal.columns.values.tolist())


def test_segments(stock, interval):
    segment = stock.segments[interval]
    print(f'********** Segments[\'{interval}\']: {len(segment)} **********')
    print(segment.head())
    print(segment.columns.values.tolist())


def test_segment_pivots(stock, interval):
    segment_pivot = stock.segment_pivots[interval]
    print(f'********** Segment Pivots[\'{interval}\']: {len(segment_pivot)} **********')
    print(segment_pivot.head())
    print(segment_pivot.columns.values.tolist())


def test_segment_signals(stock, interval):
    segment_signal = stock.segment_signals[interval]
    print(f'********** Segment Signals[\'{interval}\']: {len(segment_signal)} **********')
    print(segment_signal.head())
    print(segment_signal.columns.values.tolist())

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
        stock = Signal(symbol, {interval: df})
        test_sources(stock, interval)
        test_sticks(stock, interval)
        test_fractals(stock, interval)
        test_strokes(stock, interval)
        test_stroke_pivots(stock, interval)
        test_stroke_signals(stock, interval)
        test_segments(stock, interval)
        test_segment_pivots(stock, interval)
        test_segment_signals(stock, interval)
    except Exception as e:
        print(f'download source data error：{e}')
