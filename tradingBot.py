from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import asyncio
import queue
import time
import yaml
import logging
from telethon import TelegramClient, events
import yaml
import asyncio
from datetime import datetime
from openai import OpenAI
import json
import pytz



class IBKRWrapper(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextValidOrderId = None
        self.order_queue = queue.Queue()
        self.order_status = {}
        self.contract_details = {}  # ✅ Add this to store contract details
        
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print(f"Error {errorCode}: {errorString}")
        
    def nextValidId(self, orderId):
        self.nextValidOrderId = orderId
        
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, 
                   permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        self.order_status[orderId] = {
            'status': status,
            'filled': filled,
            'remaining': remaining,
            'avgFillPrice': avgFillPrice
        }


class TradingBot:
    def __init__(self):
        self.ibkr_app = None
        self.ibkr_thread = None
        self.order_queue = asyncio.Queue()
        
    async def init_ibkr(self):
        """Initialize IBKR connection"""
        self.ibkr_app = IBKRWrapper()
        self.ibkr_app.connect("127.0.0.1", 7497, clientId=1)
        
        # Start IBKR in a separate thread
        self.ibkr_thread = threading.Thread(target=self.ibkr_app.run)
        self.ibkr_thread.start()
        
        # Wait for nextValidOrderId
        while self.ibkr_app.nextValidOrderId is None:
            await asyncio.sleep(0.1)

    async def place_trade(self, trade_info, start_time):
        """Place trade based on the processed information"""
        print("placing trade with values ", trade_info)
        try:
            if self.ibkr_app is None:
                await self.init_ibkr()

            # Create contract
            contract = Contract()
            contract.symbol = trade_info['symbol']
            contract.secType = trade_info.get('secType', 'STK')
            contract.exchange = trade_info.get('exchange', 'SMART')
            contract.currency = trade_info.get('currency', 'USD')
            
            # For options
            if trade_info.get('secType') == 'OPT':
                contract.lastTradeDateOrContractMonth = trade_info['expiry']
                contract.strike = trade_info['strike']
                contract.right = trade_info['right']
                contract.multiplier = trade_info.get('multiplier', '100')

            if trade_info.get('secType') == 'FUT':
                contract.lastTradeDateOrContractMonth = trade_info['lastTradeDateOrContractMonth']
            
        
            # Create order
            order = Order()
            order.action = trade_info['action']  # 'BUY' or 'SELL'
            order.totalQuantity = trade_info['quantity']
            order.orderType = 'MKT'  # Market order
            order.outsideRth = True  # Allow outside regular trading hours
            
            order_id = self.ibkr_app.nextValidOrderId
            self.ibkr_app.nextValidOrderId += 1

            # Place the order
            self.ibkr_app.placeOrder(order_id, contract, order)
            
            # Wait for order status
            max_wait = 10  # Maximum wait time in seconds
            start = time.time()
            
            while time.time() - start < max_wait:
                if order_id in self.ibkr_app.order_status:
                    status = self.ibkr_app.order_status[order_id]
                    if status['status'] == 'Filled':
                        fill_price = status['avgFillPrice']
                        print(f"Order {order_id} filled at {fill_price}")
                        break
                await asyncio.sleep(0.1)

            if order_id not in self.ibkr_app.order_status or self.ibkr_app.order_status[order_id]['status'] != 'Filled':
                print(f"Order {order_id} not filled within {max_wait} seconds")
                return {'order_id': order_id, 'status': 'Not filled'}

            if fill_price is None:
                print(f"Market order {order_id} not filled within {max_wait} seconds. Exiting")
                return {'order_id': order_id, 'status': 'Not filled'}
            

            raw_target_price = fill_price*1.2 
            target_price = round(raw_target_price/0.10)*0.10
            print(f"Target price: {raw_target_price} -> adjusted to valid tick {target_price}")
            limit_order = Order()
            limit_order.action = "SELL"  if trade_info['action'] == "BUY" else "BUY"
            limit_order.totalQuantity = trade_info['quantity']
            limit_order.orderType = 'LMT'  
            limit_order.lmtPrice = target_price
            limit_order.outsideRth = True  # Allow outside regular trading hours


            limit_order_id = self.ibkr_app.nextValidOrderId
            self.ibkr_app.nextValidOrderId += 1
            self.ibkr_app.placeOrder(limit_order_id, contract, limit_order)
            # **Confirm limit order placement**
            limit_wait = 5  # Wait up to 5 seconds for IBKR to acknowledge
            start = time.time()
            confirmed = False

            while time.time() - start < limit_wait:
                if limit_order_id in self.ibkr_app.order_status:
                    confirmed = True
                    print(f"Limit sell order {limit_order_id} placed at {target_price}")
                    break
                await asyncio.sleep(0.1)
            
            if not confirmed:
                print(f"Limit order {limit_order_id} might not have been placed. Check IBKR logs.")
                return {'order_id': order_id, 'status': 'Limit order not placed'}
            
            
            execution_time = time.time() - start_time
            print(f"Trade executed in {execution_time:.2f} seconds")
            
            return {
                'order_id': order_id,
                'limit_order_id': limit_order_id if confirmed else None,   
                'status' : "placed" if confirmed else "limit order not placed",
                # 'status': self.ibkr_app.order_status.get(order_id, {}).get('status'),
                'execution_time': execution_time
            }

        except Exception as e:
            print(f"Error placing trade: {str(e)}")
            logging.error(f"Error placing trade: {str(e)}")
            raise

    async def cleanup(self):
        """Cleanup IBKR connection"""
        if self.ibkr_app:
            self.ibkr_app.disconnect()
        if self.ibkr_thread:
            self.ibkr_thread.join()


class TelegramTrader:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.telegram_client = TelegramClient(
            'test_session',
            self.config['telegram']['api_id'],
            self.config['telegram']['api_hash']
        )
        
        
        
        self.openai_client = OpenAI(api_key=self.config['openai']['api_key'])
        self.message_queue = asyncio.Queue()
        self.processed_messages = set()  # Track processed messages
        self.lock = asyncio.Lock()  # Add lock for thread safety

        self.trade_config = {
            'quantity': 1,  # Fixed quantity
            'stop_loss_percent': 0.10,  
        }
        self.active_positions = {}

        self.trading_bot = TradingBot()
    
    async def start(self):
      try:
        
        await self.telegram_client.start()
        await self.trading_bot.init_ibkr()
      
        # Start message processor
        processor_task = asyncio.create_task(self.process_messages())
      
        @self.telegram_client.on(events.NewMessage(chats=self.config['telegram']['group_id']))
        async def handler(event):
            if event.message.id not in self.processed_messages:
                logging.info(f"New message received: {event.message.text}")
                print("message added to queue", event.message.text)
                await self.message_queue.put(event)
              

        await self.telegram_client.run_until_disconnected()
      except Exception as e:
        logging.error(f"Error in start: {str(e)}")
      finally:
        await self.cleanup()


    async def process_messages(self):
        """Process messages from the queue"""
        while True:
            try:
                event = await self.message_queue.get()
                
                # Check if message was already processed
                if event.message.id in self.processed_messages:
                    continue
                
                # Mark message as processed
                self.processed_messages.add(event.message.id)
                start_time = time.time()
                print("processing message from the queue", event.message.text)
                
                # Process the message
                await self.handle_message(event, start_time)
                
            except Exception as e:
                logging.error(f"Error processing message: {str(e)}")
            finally:
                self.message_queue.task_done()


    async def handle_message(self, event, start_time):
        """Handle individual messages"""
        try:
            async with self.lock:  # Use lock to prevent concurrent processing
                message_text = event.message.text
                structured_message = message_text

    
                logging.info(f"Processing message:{structured_message}")
                print("got message", structured_message)
                # Process with OpenAI directly instead of creating a new task
                trade_info = await self.process_with_openai(structured_message)
                if trade_info:
                  ibkr_order = await self.convert_to_ibkr_format(trade_info)
                  logging.info(f"sending order details to ibkr place_order: {ibkr_order}")

                  if not self.trading_bot.ibkr_app:
                    logging.warning("IBKR not initialized. Initializing now...")
                    await self.trading_bot.init_ibkr()
                  # Place trade using TradingBot
                try:
                    print("placing trade with values", ibkr_order)
                    result = await self.trading_bot.place_trade(ibkr_order, start_time)
                    logging.info(f"Trade result: {result}")
                    
                except Exception as e:
                    print(f"Error executing trade: {str(e)}")
                    logging.error(f"Error executing trade: {str(e)}")
                else:
                    print(f"Trade result: {result}")
        except Exception as e:
            logging.error(f"Error handling message: {str(e)}")


    

  
    
      
    async def process_with_openai(self, message):
        """Process message with OpenAI in a non-blocking way"""
        try:
            # Run OpenAI processing in a thread pool
            trade_info = await asyncio.get_event_loop().run_in_executor(
                None, self.parse_with_openai, message
            )
            print("openai responed with trade info", trade_info)
            
            if trade_info and trade_info.get('action') and trade_info.get('symbol'):
                logging.info(f"Valid trade alert detected: {trade_info}")
                return trade_info
            else:
                logging.info("Message not recognized as a valid trade alert")
                return None
                
        except Exception as e:
            logging.error(f"Error processing with OpenAI: {str(e)}")
            return None

    def parse_with_openai(self, message_text):
        """Parse trading alert using OpenAI API"""
        est = pytz.timezone("America/New_York")
        today_est = datetime.now(est).strftime('%Y-%m-%d')
        
        try:
            system_prompt = f"""
                You are a trading alert parser for stocks, options, and futures. Extract relevant trading details from messages. If an option is given, use the stricter one always.

                ### **Parsing Rules:**
                #### **1️⃣ Stock Options:**
                
                - **Extract Ticker and Strike Price:**  
                - The **ticker** will be extracted from the message if it contains a recognizable stock symbol.  
                - `"C"` or `"c"` **after a number** means a **Call Option**, and the number is the **strike price**.  
                - `"P"` or `"p"` **after a number** means a **Put Option**, and the number is the **strike price**.

                - **Expiry Date Extraction:**
                - If a date is provided in `MM/DD` or `M/D` format (e.g., `"8/19"` or `"12/5"`), interpret it as **the expiration date**.
                - **Assume the expiry year is 2025** unless a year is explicitly stated.
                - Convert the expiry date to **ISO format (`YYYY-MM-DD`)**.
                - **Default expiry date** (if missing): **"{today_est}"** (today’s date in EST).

                - **Risk Identification:**
                - If the message contains words like `"risky"` or `"lotto"`, set `"risk": true`.

                - **BUY-Only Filtering:**
                - Only process messages that clearly state **"buy"** or **"bought"**.
                - Ignore any messages related to **"sell"**, **"sold"**, or **"short"**.

                ---

                #### **2️⃣ Futures Contracts (ES, NQ, etc.):**
                - If a message contains **"ES"** or **"NQ"** followed by `"long"`, it is a **futures buy trade**.
                - Extract:
                - **Action** (`"BUY"` for long positions).
                - **Symbol** (e.g., `"ES"`, `"NQ"`).
                - **Entry price** (the number following `"long"`).
                - **Stop loss** (if mentioned around X/Y, e.g., `"stop loss around 6058/6054, pick the stricter stop loss, higher value, stop loss = 6058"`).

                ---

                ### **Return Format (JSON Only)**
                You must return a JSON object with the following fields:
                ```json
                {{
                "action": "BUY",
                "symbol": "Stock ticker (e.g., AAPL, SPX, TSLA)",
                "strike_price": "Strike price for options (if applicable)",
                "option_type": "CALL" or "PUT" (if applicable)",
                "expiry": "Expiration date in YYYYMMDD format (if applicable)",
                "entry_price": "Entry price (if specified)",
                "stop_loss": "Stop loss price for futures trades (if applicable)",
                "risk": true (if the trade is marked as risky or a lotto play)
                }}
                ```
            """


            start = time.time()
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": message_text}
                ],
                response_format={ "type": "json_object" },
                max_tokens=100,
                temperature=1
            )
            end = time.time()
            print(f"API Response Time: {end - start:.2f} seconds")
            
            parsed_data = json.loads(response.choices[0].message.content)
            logging.info(f"OpenAI parsed data: {parsed_data}")
            print("openai parsed data", parsed_data)
            return parsed_data

        except Exception as e:
            logging.error(f"Error parsing with OpenAI: {str(e)}")
            return None

    async def convert_to_ibkr_format(self, trade_info):
        """Convert OpenAI parsed data to IBKR format"""
     
        if trade_info['action'].upper() != "BUY":
            return None

        ibkr_order = {
            'symbol': trade_info['symbol'],
            'action': trade_info['action'],
            'quantity': self.trade_config['quantity'],
            'exchange': 'SMART',
            'currency': 'USD'
      }

      # Handle options
        if trade_info.get('option_type'):
            ibkr_order.update({
                'secType': 'OPT',
                'right': 'C' if trade_info['option_type'] == 'CALL' else 'P',
                'strike': float(trade_info['strike_price']),
                'expiry': trade_info['expiry'].replace('-', '')
            })
      # Handle futures
    #   elif trade_info['symbol'] in ['ES', 'NQ']:
    #       ibkr_order.update({
    #           'secType': 'FUT',
    #           'exchange': 'CME'
    #       })
      # Handle stocks
        else:
            ibkr_order.update({
                'secType': 'STK'
            })

        return ibkr_order


    async def cleanup(self):
        """Cleanup resources"""
        try:
            await self.trading_bot.cleanup()
            await self.telegram_client.disconnect()
        except Exception as e:
            logging.error(f"Error in cleanup: {str(e)}")
if __name__ == "__main__":
    # Create and run the trading bot
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('trading_bot.log'),
            logging.StreamHandler()
        ]
    )
    bot = TelegramTrader()
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Bot stopped due to error: {str(e)}")
    # asyncio.run(test_trades())
    


async def test_trades():
    """Function to test trade execution manually"""
    bot = TradingBot()
    
    # Initialize IBKR
    await bot.init_ibkr()
    
    # Define hardcoded trade orders (stocks, options, futures)
    test_trades = [
        # {
        #     "symbol": "BIVI",
        #     "action": "SELL",
        #     "quantity": 200,
        #     "exchange": "SMART",
        #     "currency": "USD",
        # },
        # {
        #     "symbol": "TSLL",
        #     "action": "SELL",
        #     "quantity": 300,
        #     "exchange": "SMART",
        #     "currency": "USD",
        # },
        # {
        #     "symbol": "MSTR",
        #     "action": "SELL",
        #     "quantity": 300,
        #     "exchange": "SMART",
        #     "currency": "USD",
        # },
        # {
        #     "symbol": "NVDA",
        #     "action": "SELL",
        #     "quantity": 300,
        #     "exchange": "SMART",
        #     "currency": "USD",
        # },
        # {
        #     "symbol": "AAPL",
        #     "action": "SELL",
        #     "quantity": 2,
        #     "exchange": "SMART",
        #     "currency": "USD",
        # },
        
        {
            "symbol": "SPX",  # SPX Call Option
            "action": "BUY",
            "quantity": 3,
            "secType": "OPT",
            "expiry": "20250211",  
            "strike": 6070,
            "right": "C",
            "multiplier": "100",
            "exchange": "SMART",
            "currency": "USD"
        },
        # {
        #     "symbol": "SPX",  # SPX Call Option
        #     "action": "BUY",
        #     "quantity": 3,
        #     "secType": "OPT",
        #     "expiry": "20250211",  
        #     "strike": 6050,
        #     "right": "P",
        #     "multiplier": "100",
        #     "exchange": "SMART",
        #     "currency": "USD"
        # },
        #  {
        #     "symbol": "AMZN",  # SPX Call Option
        #     "action": "BUY",
        #     "quantity": 6,
        #     "secType": "OPT",
        #     "expiry": "20250221",  
        #     "strike": 240.0,
        #     "right": "C",
        #     "multiplier": "100",
        #     "exchange": "SMART",
        #     "currency": "USD"
        # },

        # {
        #     "symbol": "ES",  # E-mini S&P 500 Futures
        #     "action": "BUY",
        #     "quantity": 2,
        #     "secType": "FUT",
        #     "exchange": "CME",
        #     "currency": "USD",
        #     "lastTradeDateOrContractMonth": "202503",
        # },
        # {
        #     "symbol": "SPX",  # SPX Call Option
        #     "action": "BUY",
        #     "quantity": 3,
        #     "secType": "OPT",
        #     "expiry": "20250211",  
        #     "strike": 6000,
        #     "right": "C",
        #     "multiplier": "100",
        #     "exchange": "SMART",
        #     "currency": "USD"
        # },
    ]
    
    # Execute test trades
    for trade_info in test_trades:
        start_time = time.time()
        print(f"\nPlacing trade: {trade_info}")
        
        try:
            result = await bot.place_trade(trade_info, start_time)
            print(f"Trade result: {result}")
        except Exception as e:
            print(f"Error executing trade: {str(e)}")

    # Cleanup
    await bot.cleanup()