import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

class CandleAggregator:
    def __init__(self, interval_minutes=5):
        self.interval = interval_minutes
        self.data = {}
    
    def process_tick(self, symbol, tick):
        if symbol not in self.data:
            self.data[symbol] = deque()
        
        self.data[symbol].append({
            'time': datetime.now(),
            'price': tick['last_price']
        })
        
        cutoff = datetime.now() - timedelta(minutes=self.interval)
        while self.data[symbol] and self.data[symbol][0]['time'] < cutoff:
            self.data[symbol].popleft()
    
    def get_candle(self, symbol):
        if symbol not in self.data or not self.data[symbol]:
            return {'data': []}

        ticks = sorted(self.data[symbol], key=lambda t: t['time'])
        ist_offset = timezone(timedelta(hours=5, minutes=30))
        candles = defaultdict(lambda: {'prices': [], 'volumes': []})

        for t in ticks:
            candle_time = t['time'].replace(tzinfo=ist_offset)
            start = candle_time.replace(minute=(candle_time.minute // 5) * 5, second=0, microsecond=0)
            candles[start]['prices'].append(float(t['price']))
            candles[start]['volumes'].append(float(t.get('volume', 0)))

        data_list = []
        for start, info in sorted(candles.items()):
            prices = info['prices']
            if prices:
                data_list.append({
                    'date': start,
                    'open': float(prices[0]),
                    'high': float(max(prices)),
                    'low': float(min(prices)),
                    'close': float(prices[-1]),
                    'volume': int(sum(info['volumes']))
                })

        return {'data': data_list}

# aggregator = LightweightCandleAggregator(interval_minutes=1)

# def on_tick(symbol, tick):
#     aggregator.process_tick(symbol, tick)

# # ws = websocket(['TCS', 'INFY'], tick_callback=on_tick)


# time.sleep(10)
# while True:
#     candle = aggregator.get_candle('TCS')
#     print("candle", candle)
#     time.sleep(10)
