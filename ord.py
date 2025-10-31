from kiteconnect import KiteConnect
import logging
from datetime import datetime, timedelta
import time
from kiteconnect import KiteConnect, KiteTicker
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()
API_KEY = os.getenv("API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

def place_hdfcbank_amo_limit_buy(kite, quantity=1, limit_price=1002.95):
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_AMO,
            exchange=kite.EXCHANGE_NSE,
            tradingsymbol="HDFCBANK",
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=quantity,
            product=kite.PRODUCT_CNC,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=limit_price
        )
        print(f"✓ AMO Limit Order placed! Order ID: {order_id} | Buy {quantity} HDFCBANK @ Limit Price ₹{limit_price} (CNC)")
        return order_id
    except Exception as e:
        print(f"❌ Order failed: {e}")
        logging.error(f"Order placement error: {e}")
        return None

if __name__ == "__main__":
    order_id = place_hdfcbank_amo_limit_buy(kite, quantity=1, limit_price=1002.95)
    if order_id:
        order_details = kite.order_history(order_id)
        print(f"Order details: {order_details[-1]}")