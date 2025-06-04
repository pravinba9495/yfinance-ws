import asyncio
import yfinance as yf
import redis
import json
import os

r = redis.Redis(host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], db=0)

def message_handler(message):
    print(">> ", message['id'], message['price'],)
    r.publish("trades", json.dumps({
        "s": message['id'],
        "p": message['price']
    }))

async def main():
    ws = yf.AsyncWebSocket()
    symbols = os.environ['SYMBOLS'].split(',')
    await ws.subscribe(symbols)
    await ws.listen(message_handler)

asyncio.run(main())