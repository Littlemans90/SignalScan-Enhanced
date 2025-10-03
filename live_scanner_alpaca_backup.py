"""
SignalScan - Real-Time Stock Scanner with WebSocket Integration
Version 2.2 - Phase 2: WebSocket + 500 Stocks
"""

from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button  
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.uix.popup import Popup
from kivy.graphics import Color, Rectangle
from kivy.clock import Clock
from kivy.config import Config
import datetime
import pytz
import os
from dotenv import load_dotenv
import asyncio
import threading
from alpaca.data.live import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
import requests

load_dotenv()

Config.set('graphics', 'fullscreen', 'auto')
Config.set('graphics', 'borderless', '1')
Config.set('graphics', 'resizable', '0')
Config.write()


class NewsManager:
    """Manages real-time breaking news via Benzinga API"""
    
    def __init__(self, callback):
        self.callback = callback
        self.benzinga_key = os.getenv('BENZINGA_API_KEY')
        self.running = False
        self.news_cache = {}
        self.breaking_keywords = [
            'FDA approval', 'FDA approves', 'halted', 'halt', 'offering',
            'acquisition', 'merger', 'bankruptcy', 'earnings beat',
            'earnings miss', 'guidance', 'buyout'
        ]
    
    def start_news_stream(self):
        """Start polling Benzinga for breaking news"""
        self.running = True
        thread = threading.Thread(target=self._news_loop, daemon=True)
        thread.start()
    
    def _news_loop(self):
        """Continuously poll for breaking news"""
        while self.running:
            try:
                self.fetch_breaking_news()
                threading.Event().wait(10)
            except Exception as e:
                print(f"News fetch error: {e}")
                threading.Event().wait(30)
    
    def fetch_breaking_news(self):
        """Fetch latest breaking news from Benzinga"""
        try:
            url = "https://api.benzinga.com/api/v2/news"
            time_from = (datetime.datetime.now() - datetime.timedelta(minutes=15)).isoformat()
            params = {
                'token': self.benzinga_key,
                'displayOutput': 'full',
                'pageSize': 20,
                'dateFrom': time_from
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if isinstance(data, list):
                for article in data:
                    self.process_news_article(article)
                    
        except Exception as e:
            print(f"Benzinga news error: {e}")
    
    def process_news_article(self, article):
        """Process and categorize news article"""
        try:
            title = article.get('title', '')
            content = article.get('body', '')
            symbols = article.get('stocks', [])
            timestamp = article.get('created', '')
            article_id = article.get('id', '')
            
            if article_id in self.news_cache:
                return
            
            self.news_cache[article_id] = True
            
            is_breaking = any(keyword.lower() in title.lower() or keyword.lower() in content.lower() 
                            for keyword in self.breaking_keywords)
            
            for stock_info in symbols:
                symbol = stock_info.get('name', '')
                if symbol and self.callback:
                    news_data = {
                        'symbol': symbol,
                        'title': title,
                        'content': content,
                        'is_breaking': is_breaking,
                        'timestamp': timestamp
                    }
                    Clock.schedule_once(lambda dt: self.callback(news_data), 0)
                    
        except Exception as e:
            print(f"Error processing article: {e}")
    
    def stop(self):
        """Stop news stream"""
        self.running = False


class MarketDataManager:
    """Manages real-time WebSocket connections and REST API fallbacks"""
    
    def __init__(self, callback):
        self.callback = callback
        self.alpaca_api_key = os.getenv('ALPACA_API_KEY')
        self.alpaca_secret = os.getenv('ALPACA_SECRET_KEY')
        self.data_stream = None
        self.running = False
        self.stock_data = {}
        self.watchlist = []
        self.all_stocks = []
        self.last_watchlist_update = None
        
        self.stock_client = StockHistoricalDataClient(
            self.alpaca_api_key, 
            self.alpaca_secret
        )
    
    def fetch_all_tradable_stocks(self):
        """Fetch 500+ tradable US stocks from Alpaca"""
        try:
            print("Fetching tradable stock list...")
            
            trading_client = TradingClient(self.alpaca_api_key, self.alpaca_secret, paper=False)
            
            search_params = GetAssetsRequest(
                asset_class=AssetClass.US_EQUITY,
                status=AssetStatus.ACTIVE
            )
            
            assets = trading_client.get_all_assets(search_params)
            
            self.all_stocks = [
                asset.symbol for asset in assets 
                if asset.tradable and asset.exchange in ['NASDAQ', 'NYSE', 'ARCA']
            ]
            
            # Limit to 500 most liquid stocks for WebSocket efficiency
            self.watchlist = self.all_stocks[:500]
            self.last_watchlist_update = datetime.datetime.now()
            
            print(f"Loaded {len(self.all_stocks)} total stocks, using top {len(self.watchlist)} for WebSocket")
            
        except Exception as e:
            print(f"Error fetching stocks: {e}")
            print("Using expanded fallback watchlist...")
            # Expanded fallback list - top 100 most liquid stocks
            self.watchlist = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'UNH', 'JNJ',
                'V', 'XOM', 'WMT', 'LLY', 'JPM', 'PG', 'MA', 'HD', 'CVX', 'ABBV',
                'MRK', 'AVGO', 'KO', 'PEP', 'COST', 'ADBE', 'MCD', 'CSCO', 'CRM', 'ACN',
                'TMO', 'NFLX', 'ABT', 'DHR', 'LIN', 'VZ', 'TXN', 'INTC', 'NKE', 'AMD',
                'QCOM', 'PM', 'WFC', 'UPS', 'RTX', 'NEE', 'ORCL', 'HON', 'SPGI', 'INTU',
                'LOW', 'IBM', 'CAT', 'BA', 'GE', 'ELV', 'AMGN', 'NOW', 'GS', 'ISRG',
                'DE', 'BLK', 'SYK', 'AXP', 'BKNG', 'GILD', 'ADI', 'MDLZ', 'TJX', 'MMC',
                'VRTX', 'PLD', 'CB', 'SCHW', 'LRCX', 'SBUX', 'CI', 'AMT', 'SLB', 'TMUS',
                'ZTS', 'MO', 'REGN', 'PYPL', 'CVS', 'BMY', 'DUK', 'FI', 'SO', 'EQIX',
                'PGR', 'BDX', 'ITW', 'AON', 'APD', 'CL', 'ETN', 'MU', 'BSX', 'SHW'
            ]
    
    def start_websocket(self, symbols=None):
        """Start WebSocket connection for real-time updates"""
        if symbols is None:
            self.fetch_all_tradable_stocks()
        else:
            self.watchlist = symbols
            
        self.running = True
        
        thread = threading.Thread(target=self._run_websocket_loop, daemon=True)
        thread.start()
    
    def _run_websocket_loop(self):
        """Run asyncio event loop for WebSocket"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            self.data_stream = StockDataStream(self.alpaca_api_key, self.alpaca_secret)
            
            async def trade_handler(trade):
                symbol = trade.symbol
                self.stock_data[symbol] = {
                    'price': float(trade.price),
                    'volume': int(trade.size),
                    'timestamp': trade.timestamp
                }
                if self.callback:
                    Clock.schedule_once(lambda dt: self.callback(symbol, self.stock_data[symbol]), 0)
            
            async def quote_handler(quote):
                symbol = quote.symbol
                if symbol not in self.stock_data:
                    self.stock_data[symbol] = {}
                self.stock_data[symbol].update({
                    'bid': float(quote.bid_price),
                    'ask': float(quote.ask_price),
                    'bid_size': int(quote.bid_size),
                    'ask_size': int(quote.ask_size)
                })
            
            async def bar_handler(bar):
                symbol = bar.symbol
                if symbol not in self.stock_data:
                    self.stock_data[symbol] = {}
                    
                prev_close = self.stock_data[symbol].get('prev_close', bar.open)
                current_price = bar.close
                change_pct = ((current_price - prev_close) / prev_close) * 100 if prev_close else 0
                
                self.stock_data[symbol].update({
                    'open': float(bar.open),
                    'high': float(bar.high),
                    'low': float(bar.low),
                    'close': float(bar.close),
                    'volume': int(bar.volume),
                    'change_pct': change_pct
                })
            
            print(f"Subscribing to WebSocket feeds for {len(self.watchlist)} stocks...")
            for symbol in self.watchlist:
                self.data_stream.subscribe_trades(trade_handler, symbol)
                self.data_stream.subscribe_quotes(quote_handler, symbol)
                self.data_stream.subscribe_bars(bar_handler, symbol)
            
            loop.run_until_complete(self.data_stream._run_forever())
            
        except Exception as e:
            print(f"WebSocket error: {e}")
    
    def get_snapshot_data(self, symbols):
        """Get current snapshot data for symbols (REST fallback)"""
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=symbols)
            quotes = self.stock_client.get_stock_latest_quote(request)
            
            snapshot_data = {}
            for symbol, quote in quotes.items():
                snapshot_data[symbol] = {
                    'price': float(quote.ask_price),
                    'bid': float(quote.bid_price),
                    'ask': float(quote.ask_price),
                    'timestamp': quote.timestamp
                }
            return snapshot_data
        except Exception as e:
            print(f"Snapshot fetch error: {e}")
            return {}
    
    def get_index_data(self, symbol):
        """Get index data (NASDAQ, S&P) using AlphaVantage"""
        try:
            api_key = os.getenv('ALPHAVANTAGE_API_DATA')
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"
            response = requests.get(url, timeout=5)
            data = response.json()
            
            if 'Global Quote' in data:
                quote = data['Global Quote']
                price = float(quote.get('05. price', 0))
                change_pct = float(quote.get('10. change percent', '0').replace('%', ''))
                return price, change_pct
        except:
            pass
        return 0.0, 0.0
    
    def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        if self.data_stream:
            self.data_stream.stop()


class SignalScanApp(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.orientation = "vertical"
        self.spacing = 0
        self.padding = 0
        
        with self.canvas.before:
            Color(0.08, 0.08, 0.08, 1)
            self.bg_rect = Rectangle(size=self.size, pos=self.pos)
        self.bind(size=self._update_bg, pos=self._update_bg)

        self.live_data = {
            "PreGap": [],
            "HOD": [],
            "RunUp": [],
            "RunDown": [],
            "Rvsl": [],
            "Halts": []
        }
        
        self.stock_news = {}

        self.current_channel = "PreGap"
        self.nasdaq_last, self.nasdaq_pct = 0.0, 0.0
        self.sp_last, self.sp_pct = 0.0, 0.0
        
        self.market_data = MarketDataManager(callback=self.on_websocket_update)
        self.news_manager = NewsManager(callback=self.on_news_update)
        
        self.build_header()
        main_content = BoxLayout(orientation="vertical", spacing=0, padding=0)
        self.tabs_container = self.build_channel_tabs()
        main_content.add_widget(self.tabs_container)
        self.data_container = self.build_data_section()
        main_content.add_widget(self.data_container)
        self.add_widget(main_content)
        
        Clock.schedule_interval(self.update_times, 1)
        Clock.schedule_interval(self.update_indices, 60)
        Clock.schedule_once(self.start_market_data, 2)
        Clock.schedule_once(self.start_news_feed, 3)

    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size

    def start_market_data(self, dt=None):
        """Initialize market data streams"""
        print("Starting WebSocket connection for 500+ stocks...")
        self.market_data.start_websocket()
        self.fetch_snapshot_data()
        self.update_indices()
    
    def start_news_feed(self, dt=None):
        """Initialize news feed"""
        print("Starting breaking news feed...")
        self.news_manager.start_news_stream()

    def on_websocket_update(self, symbol, data):
        """Callback when WebSocket receives new data"""
        self.process_stock_update(symbol, data)
    
    def on_news_update(self, news_data):
        """Callback when breaking news arrives"""
        symbol = news_data['symbol']
        title = news_data['title']
        is_breaking = news_data['is_breaking']
        
        self.stock_news[symbol] = {
            'title': title,
            'is_breaking': is_breaking,
            'content': news_data['content']
        }
        
        print(f"{'🚨 BREAKING' if is_breaking else 'News'}: {symbol} - {title}")
        
        if is_breaking:
            self.show_breaking_news_alert(symbol, title)
        
        Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)
    
    def show_breaking_news_alert(self, symbol, title):
        """Show full-screen breaking news popup"""
        content = BoxLayout(orientation="vertical", padding=30, spacing=20)
        
        alert_label = Label(text="🚨 BREAKING NEWS 🚨", font_size=24, bold=True,
                           color=(1, 0, 0, 1), size_hint=(1, None), height=50)
        content.add_widget(alert_label)
        
        ticker_label = Label(text=symbol, font_size=32, bold=True,
                            color=(0, 1, 0, 1), size_hint=(1, None), height=60)
        content.add_widget(ticker_label)
        
        news_label = Label(text=title, font_size=18, text_size=(600, None),
                          halign="center", valign="middle", color=(1, 1, 1, 1))
        content.add_widget(news_label)
        
        close_btn = Button(text="ACKNOWLEDGE", size_hint=(1, None), height=50,
                          background_color=(1, 0, 0, 1), font_size=16, bold=True)
        
        popup = Popup(title="", content=content, size_hint=(None, None), size=(700, 400),
                     background_color=(0.1, 0.1, 0.1, 1), auto_dismiss=False)
        close_btn.bind(on_release=popup.dismiss)
        content.add_widget(close_btn)
        popup.open()

    def fetch_snapshot_data(self, dt=None):
        """Fetch snapshot data for all watchlist stocks"""
        snapshot = self.market_data.get_snapshot_data(self.market_data.watchlist)
        
        for symbol, data in snapshot.items():
            self.process_stock_update(symbol, data)

    def process_stock_update(self, symbol, data):
        """Process stock data update and categorize"""
        try:
            price = data.get('price', data.get('ask', 0))
            change_pct = data.get('change_pct', 0)
            volume = data.get('volume', 0)
            
            news_icon = "📰"
            if symbol in self.stock_news:
                news_icon = "🚨" if self.stock_news[symbol]['is_breaking'] else "📰"
            
            formatted_data = [
                symbol,
                f"${price:.2f}",
                f"{change_pct:+.1f}%",
                self.format_volume(volume),
                "50M",
                "3.2x",
                news_icon
            ]
            
            self.categorize_stock(formatted_data, change_pct, volume)
            Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)
            
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    def categorize_stock(self, stock_data, change_pct, volume):
        """Categorize stock into scanner channels"""
        ticker = stock_data[0]
        
        for channel in self.live_data:
            self.live_data[channel] = [s for s in self.live_data[channel] if s[0] != ticker]
        
        if abs(change_pct) > 5:
            self.live_data["PreGap"].append(stock_data)
        if change_pct > 2:
            self.live_data["HOD"].append(stock_data)
        if change_pct > 0:
            self.live_data["RunUp"].append(stock_data)
        if change_pct < 0:
            self.live_data["RunDown"].append(stock_data)
        if volume > 1000000:
            self.live_data["Rvsl"].append(stock_data)

    def update_indices(self, dt=None):
        """Update NASDAQ and S&P 500 data"""
        self.nasdaq_last, self.nasdaq_pct = self.market_data.get_index_data("^IXIC")
        self.sp_last, self.sp_pct = self.market_data.get_index_data("^GSPC")
        
        self.nasdaq_label.text = f"NASDAQ  {self.nasdaq_pct:+.1f}%"
        self.nasdaq_label.color = (0, 1, 0, 1) if self.nasdaq_pct >= 0 else (1, 0, 0, 1)
        
        self.sp_label.text = f"S&P     {self.sp_pct:+.1f}%"
        self.sp_label.color = (0, 1, 0, 1) if self.sp_pct >= 0 else (1, 0, 0, 1)

    def format_volume(self, volume):
        if volume >= 1000000:
            return f"{volume/1000000:.1f}M"
        elif volume >= 1000:
            return f"{volume/1000:.1f}K"
        return str(int(volume))

    def build_header(self):
        header = BoxLayout(orientation="horizontal", size_hint=(1, None), height=80, padding=[15, 10, 15, 10])
        with header.canvas.before:
            Color(0.12, 0.12, 0.12, 1)
            header.bg_rect = Rectangle(size=header.size, pos=header.pos)
        header.bind(size=lambda inst, val: setattr(header.bg_rect, "size", inst.size))
        header.bind(pos=lambda inst, val: setattr(header.bg_rect, "pos", inst.pos))
        
        title = Label(text="SignalScan", font_size=28, color=(1, 1, 1, 1), bold=True, size_hint=(None, 1), width=180)
        header.add_widget(title)
        
        times_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=160)
        self.local_time_label = Label(text="Local Time    7:00 PM", font_size=14, color=(0.8, 0.8, 0.8, 1))
        self.nyc_time_label = Label(text="NYC Time     10:00 PM", font_size=14, color=(0.8, 0.8, 0.8, 1))
        times_section.add_widget(self.local_time_label)
        times_section.add_widget(self.nyc_time_label)
        header.add_widget(times_section)
        
        center_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=160)
        self.market_state_label = Label(text="Weekend", font_size=16, color=(1, 0.6, 0, 1), bold=True)
        self.countdown_label = Label(text="00:00:00", font_size=14, color=(1, 0.6, 0, 1))
        center_section.add_widget(self.market_state_label)
        center_section.add_widget(self.countdown_label)
        header.add_widget(center_section)
        
        indicators_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=140)
        self.nasdaq_label = Label(text=f"NASDAQ  +0.0%", font_size=13, color=(0, 1, 0, 1), bold=True)
        self.sp_label = Label(text=f"S&P     +0.0%", font_size=13, color=(0, 1, 0, 1), bold=True)
        indicators_section.add_widget(self.nasdaq_label)
        indicators_section.add_widget(self.sp_label)
        header.add_widget(indicators_section)
        
        header.add_widget(Label(text="", size_hint=(1, 1)))
        
        exit_btn = Button(text="✖", font_size=18, size_hint=(None, None), size=(35, 35), 
                         background_color=(0.6, 0.2, 0.2, 1), color=(1, 1, 1, 1))
        exit_btn.bind(on_release=self.exit_app)
        header.add_widget(exit_btn)
        
        self.add_widget(header)

    def build_channel_tabs(self):
        tabs_container = BoxLayout(orientation="horizontal", size_hint=(1, None), height=45, spacing=0, padding=0)
        with tabs_container.canvas.before:
            Color(0.1, 0.1, 0.1, 1)
            tabs_container.bg_rect = Rectangle(size=tabs_container.size, pos=tabs_container.pos)
        tabs_container.bind(size=lambda inst, val: setattr(tabs_container.bg_rect, "size", inst.size))
        tabs_container.bind(pos=lambda inst, val: setattr(tabs_container.bg_rect, "pos", inst.pos))
        
        channels = ["PreGap", "HOD", "RunUp", "RunDown", "Rvsl", "Halts"]
        self.channel_buttons = {}
        
        for channel in channels:
            btn = Button(text=channel, font_size=16, size_hint=(1, 1), bold=True)
            if channel == self.current_channel:
                btn.background_color = (0, 0.8, 0, 1)
                btn.color = (1, 1, 1, 1)
            else:
                btn.background_color = (0.25, 0.25, 0.25, 1)
                btn.color = (0.7, 0.7, 0.7, 1)
            btn.bind(on_release=lambda x, ch=channel: self.select_channel(ch))
            self.channel_buttons[channel] = btn
            tabs_container.add_widget(btn)
        
        return tabs_container

    def build_data_section(self):
        data_section = BoxLayout(orientation="vertical", spacing=0, padding=[15, 10, 15, 10])
        
        header_layout = BoxLayout(orientation="horizontal", size_hint=(1, None), height=35)
        with header_layout.canvas.before:
            Color(0.15, 0.15, 0.15, 1)
            header_layout.bg_rect = Rectangle(size=header_layout.size, pos=header_layout.pos)
        header_layout.bind(size=lambda inst, val: setattr(header_layout.bg_rect, "size", inst.size))
        header_layout.bind(pos=lambda inst, val: setattr(header_layout.bg_rect, "pos", inst.pos))
        
        headers = ["TICKER", "PRICE", "GAP%", "VOL", "FLOAT", "RVOL", "NEWS"]
        for header in headers:
            label = Label(text=header, font_size=15, color=(0.7, 0.7, 0.7, 1), bold=True)
            header_layout.add_widget(label)
        data_section.add_widget(header_layout)
        
        self.scroll_view = ScrollView(size_hint=(1, 1))
        self.rows_container = BoxLayout(orientation="vertical", size_hint_y=None, spacing=2)
        self.rows_container.bind(minimum_height=self.rows_container.setter('height'))
        
        self.scroll_view.add_widget(self.rows_container)
        data_section.add_widget(self.scroll_view)
        
        return data_section

    def refresh_data_table(self):
        """Refresh the data table with current channel stocks"""
        self.rows_container.clear_widgets()
        
        stocks = self.live_data.get(self.current_channel, [])
        
        for stock_data in stocks[:50]:
            row = self.create_stock_row(stock_data)
            if row:
                self.rows_container.add_widget(row)

    def create_stock_row(self, stock_data):
        """Create a single stock row"""
        row = BoxLayout(orientation="horizontal", size_hint=(1, None), height=40)
        
        try:
            change_str = stock_data[2].replace('%', '').replace('+', '')
            change_pct = float(change_str)
            text_color = (0, 1, 0, 1) if change_pct >= 0 else (1, 0, 0, 1)
        except:
            text_color = (0.9, 0.9, 0.9, 1)
        
        for i, value in enumerate(stock_data):
            if i == 6:
                btn = Button(text=value, font_size=16, size_hint=(1, 1),
                           background_color=(0.2, 0.2, 0.2, 1))
                ticker = stock_data[0]
                news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
                btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
                row.add_widget(btn)
            else:
                label = Label(text=str(value), font_size=14, color=text_color if i > 0 else (1, 1, 1, 1))
                row.add_widget(label)
        
        return row

    def show_news_popup(self, ticker, news_text):
        content = BoxLayout(orientation="vertical", padding=20, spacing=15)
        title_label = Label(text=f"{ticker} - NEWS", font_size=18, bold=True, 
                           size_hint=(1, None), height=40, color=(0, 1, 0, 1))
        content.add_widget(title_label)
        
        news_label = Label(text=news_text, font_size=14, text_size=(400, None), 
                          halign="left", valign="middle", color=(0.9, 0.9, 0.9, 1))
        content.add_widget(news_label)
        
        close_btn = Button(text="Close", size_hint=(1, None), height=40, 
                          background_color=(0, 0.8, 0, 1))
        popup = Popup(title="", content=content, size_hint=(None, None), size=(450, 250), 
                     background_color=(0.15, 0.15, 0.15, 1))
        close_btn.bind(on_release=popup.dismiss)
        content.add_widget(close_btn)
        popup.open()

    def select_channel(self, channel):
        for ch, btn in self.channel_buttons.items():
            btn.background_color = (0.25, 0.25, 0.25, 1)
            btn.color = (0.7, 0.7, 0.7, 1)
        self.channel_buttons[channel].background_color = (0, 0.8, 0, 1)
        self.channel_buttons[channel].color = (1, 1, 1, 1)
        self.current_channel = channel
        self.refresh_data_table()

    def update_times(self, dt):
        now = datetime.datetime.now()
        self.local_time_label.text = "Local Time    " + now.strftime("%I:%M %p")
        
        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        self.nyc_time_label.text = "NYC Time     " + now_est.strftime("%I:%M %p")
        
        state, color = self.get_market_state_and_color(now_est)
        self.market_state_label.text = state
        self.market_state_label.color = color
        self.countdown_label.color = color
        
        next_time = self.get_next_change(now_est, state)
        if next_time:
            remaining = next_time - now_est
            seconds = int(remaining.total_seconds())
            h = seconds // 3600
            m = (seconds % 3600) // 60
            s = seconds % 60
            self.countdown_label.text = f"{h:02d}:{m:02d}:{s:02d}"
        else:
            self.countdown_label.text = "00:00:00"

    def get_market_state_and_color(self, now_est):
        market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        premkt_start = now_est.replace(hour=7, minute=0, second=0, microsecond=0)
        close_start = now_est.replace(hour=14, minute=0, second=0, microsecond=0)
        after_end = now_est.replace(hour=19, minute=0, second=0, microsecond=0)
        
        weekday = now_est.weekday()
        state = "Closed"
        color = (1, 0, 0, 1)
        
        if weekday == 5 or weekday == 6 or (weekday == 4 and now_est >= market_close):
            state = "Weekend"
            color = (1, 0.6, 0, 1)
        elif now_est >= premkt_start and now_est < market_open:
            state = "PreMarket"
            color = (0.2, 0.5, 1, 1)
        elif now_est >= market_open and now_est < close_start:
            state = "Market Open"
            color = (0, 1, 0, 1)
        elif now_est >= close_start and now_est < market_close:
            state = "Market Closes"
            color = (1, 1, 0, 1)
        elif now_est >= market_close and now_est < after_end:
            state = "After Hours"
            color = (0.2, 0.5, 1, 1)
        
        return state, color

    def get_next_change(self, now_est, state):
        times = []
        weekday = now_est.weekday()
        
        if state == "PreMarket":
            times.append(now_est.replace(hour=9, minute=30, second=0, microsecond=0))
        elif state == "Market Open":
            times.append(now_est.replace(hour=14, minute=0, second=0, microsecond=0))
        elif state == "Market Closes":
            times.append(now_est.replace(hour=16, minute=0, second=0, microsecond=0))
        elif state == "After Hours":
            times.append(now_est.replace(hour=19, minute=0, second=0, microsecond=0))
        elif state == "Weekend":
            if weekday == 5:
                monday = now_est + datetime.timedelta(days=2)
                times.append(monday.replace(hour=7, minute=0, second=0, microsecond=0))
            elif weekday == 6:
                monday = now_est + datetime.timedelta(days=1)
                times.append(monday.replace(hour=7, minute=0, second=0, microsecond=0))
            elif weekday == 4 and now_est.hour >= 16:
                monday = now_est + datetime.timedelta(days=3)
                times.append(monday.replace(hour=7, minute=0, second=0, microsecond=0))
        else:
            if now_est.hour >= 19:
                next_day = now_est + datetime.timedelta(days=1)
                while next_day.weekday() >= 5:
                    next_day = next_day + datetime.timedelta(days=1)
                times.append(next_day.replace(hour=7, minute=0, second=0, microsecond=0))
            else:
                times.append(now_est.replace(hour=7, minute=0, second=0, microsecond=0))
        
        times = [t for t in times if t > now_est]
        return times[0] if times else None

    def exit_app(self, instance):
        self.market_data.stop()
        self.news_manager.stop()
        App.get_running_app().stop()


class SignalScanMainApp(App):
    def build(self):
        return SignalScanApp()


if __name__ == '__main__':
    SignalScanMainApp().run()