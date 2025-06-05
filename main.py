import asyncio
import yfinance as yf
import redis
import json
import os
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-0s %(message)s',
    level=logging.INFO,
    datefmt='%Y/%m/%d %H:%M:%S')

r = redis.Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], db=0)

def message_handler(message):
    msg = json.dumps({
        "s": message['id'],
        "p": message['price'],
        "t": int(message['time']),
    })
    logging.info(msg)
    r.publish("trades", msg)

async def main():
    ws = yf.AsyncWebSocket()
    symbols = os.environ['SYMBOLS'].split(',')
    await ws.subscribe(symbols)
    await ws.listen(message_handler)

asyncio.run(main())