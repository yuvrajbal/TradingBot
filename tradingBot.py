from email import message
import re
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
from datetime import datetime, timedelta, timezone
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

            # if trade_info.get('secType') == 'FUT':
            #     contract.lastTradeDateOrContractMonth = trade_info['lastTradeDateOrContractMonth']
            
            print("Creating order with contract", contract)
            # Create order
            order = Order()
            order.action = 'BUY'  # 'BUY' ONLY
            order.totalQuantity = trade_info['quantity']
            order.orderType = 'MKT'  # MKT OR LMT order
            # order.lmtPrice = trade_info.get('entry_price')
            # order.outsideRth = True  # Allow outside regular trading hours
            
            order_id = self.ibkr_app.nextValidOrderId
            self.ibkr_app.nextValidOrderId += 1
            print("PLACING ORDER with order=", order)
            # Place the order
            self.ibkr_app.placeOrder(order_id, contract, order)
            print("placed buy order", order_id)

            async def monitor_order(order_id,contract):
                while True:
                    if order_id in self.ibkr_app.order_status:
                        status = self.ibkr_app.order_status[order_id]
                        if status['status'] == "Filled":
                            fill_price = status["avgFillPrice"]
                            print(f"Order {order_id} filled at {fill_price}")
                            profit_targets = [
                            (0.25, 0.50),  # 25% profit, 50% quantity
                            (0.35, 0.30),  # 35% profit, 30% quantity
                            (0.50, 0.10),  # 50% profit, 10% quantity
                            (0.75, 0.10)   # 75% profit, 10% quantity
                            ]
                            limit_order_ids = []
                            for profit, qty_fraction in profit_targets:
                                raw_target_price = round(fill_price * (1 + profit), 2)
                                target_price = round(raw_target_price/0.10)*0.10
                                limit_order = Order()
                                limit_order.action = "SELL"
                                limit_order.totalQuantity = round(trade_info['quantity'] * qty_fraction)
                                limit_order.orderType = 'LMT'
                                limit_order.lmtPrice = target_price
                                limit_order.outsideRth = True
                                if limit_order.totalQuantity > 0:  # Ensure valid order
                                    limit_order_id = self.ibkr_app.nextValidOrderId
                                    self.ibkr_app.nextValidOrderId += 1
                                    self.ibkr_app.placeOrder(limit_order_id, contract, limit_order)
                                    limit_order_ids.append((limit_order_id, target_price))

                                    print(f"Limit sell order {limit_order_id} placed for {limit_order.totalQuantity} at {target_price}")
                            # # calculate profit target price
                            # raw_target_price = fill_price*1.2
                            # target_price = round(raw_target_price/0.10)*0.10
                            # limit_order = Order()
                            # limit_order.action = "SELL" # sell the contract
                            # limit_order.totalQuantity = trade_info['quantity']
                            # limit_order.orderType = 'LMT'  
                            # limit_order.lmtPrice = target_price
                            # limit_order.outsideRth = True  # Allow outside regular trading hours
                            # limit_order_id = self.ibkr_app.nextValidOrderId
                            # self.ibkr_app.nextValidOrderId += 1
                            # self.ibkr_app.placeOrder(limit_order_id, contract, limit_order)
                            # print(f"Limit sell order {limit_order_id} placed at {target_price}")
                            execution_time = time.time() - start_time
                            print(f"Trade executed in {execution_time:.2f} seconds")
                            return {
                                'order_id': order_id,
                                'limit_orders': limit_order_ids,
                                'status': "Filled and staggered sell orders placed",
                                'fill_price': fill_price,
                                'execution_time': execution_time
                            }
                        elif status['status'] == "Cancelled":
                            return {
                                'order_id': order_id,
                                'status': "Cancelled"
                            }
                    await asyncio.sleep(1) #check every second

            try:
                print("monitoring order", order_id)
                initial_timeout = 10
                monitoring_task = asyncio.create_task(monitor_order(order_id,contract))
                result = await asyncio.wait_for(monitoring_task, timeout=initial_timeout)
                return result
            except asyncio.TimeoutError:
                print(f"Initial {initial_timeout} seconds timeout reached. Continuing to monitor in background")
                
                async def background_monitor():
                    try:
                        result = await monitor_order(order_id,contract)
                        print(f"Background monitor result: {result}")
                    except Exception as e:
                        print(f"Error in background monitor: {str(e)}")
                
                asyncio.create_task(background_monitor())
                
                return{
                    'order_id': order_id,
                    'status': "monitoring in background",
                    "message" : f"Order not filled within intial {initial_timeout} seconds. Monitoring in background"
                }

        except Exception as e:
            print(f"Error placing trade: {str(e)}")
            logging.error(f"Error placing trade: {str(e)}")
            raise            

            # Wait for order status
            # max_wait = 10  # Maximum wait time in seconds
            # start = time.time()
            
            # while time.time() - start < max_wait:
            #     if order_id in self.ibkr_app.order_status:
            #         status = self.ibkr_app.order_status[order_id]
            #         if status['status'] == 'Filled':
            #             fill_price = status['avgFillPrice']
            #             print(f"Order {order_id} filled at {fill_price}")
            #             break
            #     await asyncio.sleep(0.1)

            # if order_id not in self.ibkr_app.order_status or self.ibkr_app.order_status[order_id]['status'] != 'Filled':
            #     print(f"Order {order_id} not filled within {max_wait} seconds")
            #     return {'order_id': order_id, 'status': 'Not filled'}

            # if fill_price is None:
            #     print(f"Market order {order_id} not filled within {max_wait} seconds. Exiting")
            #     return {'order_id': order_id, 'status': 'Not filled'}
            

            # raw_target_price = fill_price*1.2 
            # target_price = round(raw_target_price/0.10)*0.10
            # print(f"Target price: {raw_target_price} -> adjusted to valid tick {target_price}")
            # limit_order = Order()
            # limit_order.action = "SELL"  if trade_info['action'] == "BUY" else "BUY"
            # limit_order.totalQuantity = trade_info['quantity']
            # limit_order.orderType = 'LMT'  
            # limit_order.lmtPrice = target_price
            # limit_order.outsideRth = True  # Allow outside regular trading hours


            # limit_order_id = self.ibkr_app.nextValidOrderId
            # self.ibkr_app.nextValidOrderId += 1
            # self.ibkr_app.placeOrder(limit_order_id, contract, limit_order)
            # # **Confirm limit order placement**
            # limit_wait = 5  # Wait up to 5 seconds for IBKR to acknowledge
            # start = time.time()
            # confirmed = False

            # while time.time() - start < limit_wait:
            #     if limit_order_id in self.ibkr_app.order_status:
            #         confirmed = True
            #         print(f"Limit sell order {limit_order_id} placed at {target_price}")
            #         break
            #     await asyncio.sleep(0.1)
            
            # if not confirmed:
            #     print(f"Limit order {limit_order_id} might not have been placed. Check IBKR logs.")
            #     return {'order_id': order_id, 'status': 'Limit order not placed'}
            
            
            # execution_time = time.time() - start_time
            # print(f"Trade executed in {execution_time:.2f} seconds")
            
            # return {
            #     'order_id': order_id,
            #     'limit_order_id': limit_order_id if confirmed else None,   
            #     'status' : "placed" if confirmed else "limit order not placed",
            #     # 'status': self.ibkr_app.order_status.get(order_id, {}).get('status'),
            #     'execution_time': execution_time
            # }

   

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
            'quantity': 10,  # Fixed quantity
            'stop_loss_percent': 0.10,  
        }
        self.active_positions = {}

        self.trading_bot = TradingBot()
    
    async def start(self):
      try:
        
        await self.telegram_client.start()
        await self.trading_bot.init_ibkr()
        self.start_timestamp = datetime.now(timezone.utc)
        async for message in self.telegram_client.iter_messages(self.config['telegram']['primary_group_id'], limit=1):
            self.last_message_id = message.id

        # Start message processor
        processor_task = asyncio.create_task(self.process_messages())
        high_conviction_alerts = self.config['telegram']['primary_group_id']
        low_conviction_alerts = self.config['telegram']['secondary_group_id']

        @self.telegram_client.on(events.NewMessage(chats=[high_conviction_alerts,low_conviction_alerts]))
        async def handler(event):
            # and event.message.id > self.last_message_id
            if event.message.date >= self.start_timestamp and event.message.id  not in self.processed_messages:
                low_priority = event.chat_id == low_conviction_alerts
                message_data = {
                    "event": event,
                    "low_priority": low_priority
                }

                logging.info(f"New message received from {'Secondary' if low_priority else 'Primary'} channel: {self.clean_message(event.message.text)}")
                print(f"Message added to queue ({'Low Priority' if low_priority else 'High Priority'}):", event.message.text)
                await self.message_queue.put(message_data)
              

        await self.telegram_client.run_until_disconnected()
      except Exception as e:
        logging.error(f"Error in start: {str(e)}")
      finally:
        await self.cleanup()


    async def process_messages(self):
        """Process messages from the queue"""
        while True:
            try:
                message_data = await self.message_queue.get()
                event = message_data['event']
                low_priority = message_data['low_priority']
                
                # Check if message was already processed
                if event.message.id in self.processed_messages:
                    continue
                
                # Mark message as processed
                self.processed_messages.add(event.message.id)
                start_time = time.time()
                print("processing message from the queue", event.message.text)
                
                # Process the message
                await self.handle_message(event, start_time, low_priority)
                
            except Exception as e:
                logging.error(f"Error processing message: {str(e)}")
            finally:
                self.message_queue.task_done()
    
    def clean_message(self,message):
        # Remove emojis and non-standard symbols
        cleaned_message = re.sub(r'[^\w\s.,:!-]', '', message)
        return cleaned_message

    async def handle_message(self, event, start_time, low_priority):
        """Handle individual messages"""
        try:
            async with self.lock:  # Use lock to prevent concurrent processing
                message_text = event.message.text
                structured_message = self.clean_message(message_text)
                print("Cleaned message", structured_message)
    
                logging.info(f"Processing message:{structured_message}")
                print("Starting to process message", structured_message)
                # Process with OpenAI directly instead of creating a new task
                trade_info = await self.process_with_openai(structured_message)
                if trade_info:
                  ibkr_order = await self.convert_to_ibkr_format(trade_info, low_priority)
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
            
            if trade_info and trade_info.get('action') and trade_info.get('action')!="IGNORE" and trade_info.get('symbol'):
                logging.info(f"Valid trade alert detected: {trade_info}")
                return trade_info
            else:
                logging.info("Message not recognized as a valid trade alert")
                return None
                
        except Exception as e:
            logging.error(f"Error processing with OpenAI: {str(e)}")
            return None

    def get_next_friday(self):
        """Returns the next Friday's date in YYYYMMDD format."""
        today = datetime.now(pytz.timezone("America/New_York"))
        days_until_friday = (4 - today.weekday()) % 7  # Friday is weekday 4
        next_friday = today + timedelta(days=days_until_friday)
        return next_friday.strftime('%Y%m%d')
    
    def parse_with_openai(self, message_text):
        """Parse trading alert using OpenAI API"""
        est = pytz.timezone("America/New_York")
        today_est = datetime.now(est).strftime('%Y-%m-%d')
        
        try:
            system_prompt = f"""
                You are a trading alert parser for options only. Your primary goal is to NEVER miss a buy alert.

                ### **CORE TRADE IDENTIFICATION:**
                - **Buy Signal Keywords (MUST HAVE ONE):**
                    - "buy", "Buy", "BUY", "bought", "Bought", "BOUGHT"
                    - These can appear anywhere in the message

                - **Valid Trade Components:**
                    - Ticker (e.g., SPX, AAPL, TSLA)
                    - Strike Price (number followed by C/c/P/p)
                    - Entry Price (if provided, usually after "at" or "@")

                ### **PARSING PRIORITY:**
                1. First, identify if message contains any buy signal
                2. Then, extract the first valid trade components that follow
                3. Ignore everything after "roll" or "using profit from"

                ### **REJECTION CRITERIA:**
                Only reject if:
                1. Message contains NO buy signal
                2. Message is about ES or NQ futures
                3. Message contains "credit spread" or "debit spread"
                4. Message is purely about selling/exiting

                ### **PARSING RULES:**
                - **Strike Price Format:**
                    - NUMBER + C/c = CALL Option (e.g., "5800C", "6100c")
                    - NUMBER + P/p = PUT Option (e.g., "5800P", "6100p")

                - **Entry Price:**
                    - Look for number after "at" or "@"
                    - Convert to decimal if needed

                - **Expiration:**
                    - For SPX: Use "{today_est}" (today)
                    - For stocks: Use "{self.get_next_friday()}" (next Friday)
                    - If date provided in MM/DD format, use it with year 2025

                - **Risk Flag:**
                    - Set true if contains "risky", "lotto", "lottery"

                ### **SPECIAL HANDLING:**
                - **ALWAYS PARSE** the first valid buy signal even if message contains:
                    - Roll mentions
                    - Profit taking
                    - Multiple trades (take the first one)
                    - Complex commentary

                ### **OUTPUT FORMAT:**
                Return JSON for ANY valid buy signal:
                {{
                    "action": "BUY",
                    "symbol": "Ticker",
                    "strike_price": "Strike price",
                    "option_type": "CALL" or "PUT",
                    "expiry": "YYYYMMDD",
                    "entry_price": "Price if given",
                    "stop_loss": "If mentioned",
                    "risk": boolean
                }}

                ### **Example Valid Trades:**
                - "Bought SPX 5800C at 1.80 Roll using profit from 6100"
                    Should parse as: {{
                        "action": "BUY",
                        "symbol": "SPX",
                        "strike_price": "5800",
                        "option_type": "CALL",
                        "expiry": "[today's date]",
                        "entry_price": "1.80"
                    }}
                """


            start = time.time()
            response = self.openai_client.chat.completions.create(
                model="gpt-4o",
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
            response_content = response.choices[0].message.content.strip()
            print("response content", response_content)
            if not response_content:
                response_content = "{}"
        
            parsed_data = json.loads(response_content)
            
            if not isinstance(parsed_data, dict):
                logging.error("Invalid response format from OpenAI")
                print("Invalid response format from OpenAI")
                return {}
            
            if parsed_data and parsed_data.get('option_type') and not parsed_data.get('expiry'):
                
                if parsed_data['symbol'] == 'SPX':
                    parsed_data['expiry'] = today_est  # Use today's date for SPX
                else:
                    parsed_data['expiry'] = self.get_next_friday()  # Use Friday for stock options
            
            if parsed_data:
                logging.info(f"OpenAI response: {parsed_data}")
                print("openai response =", parsed_data)
                return parsed_data
            else:
                logging.info("No valid trade detected,returning empty JSON object")
                return {}

        except Exception as e:
            logging.error(f"Error parsing with OpenAI: {str(e)}")
            return {}


    
    
    async def convert_to_ibkr_format(self, trade_info, low_priority):
        """Convert OpenAI parsed data to IBKR format"""
     
        if trade_info['action'].upper() != "BUY":
            return None

        base_quantity  = self.trade_config['quantity']
        quantity = base_quantity if not low_priority else base_quantity // 2
        ibkr_order = {
            'symbol': trade_info['symbol'],
            'action': trade_info['action'],
            'quantity': quantity,
            'exchange': 'SMART',
            'currency': 'USD',
            'entry_price': trade_info.get('entry_price') or 0.0,
      }
        print("ibkr order before", ibkr_order)
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