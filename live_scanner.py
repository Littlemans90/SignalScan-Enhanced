"""
SignalScan Enhanced - Full US Market Scanner (8,000+ Stocks)
Version 5.0 - yfinance Bulk Scanning + Optional Finnhub WebSocket
Scans entire US market every 5 minutes via yfinance multithreading
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
import threading
import time
import requests
import pandas as pd
import yfinance as yf

load_dotenv()

# PRODUCTION MODE: Auto fullscreen kiosk
Config.set('graphics', 'fullscreen', 'auto')
Config.set('graphics', 'borderless', '1')
Config.set('graphics', 'resizable', '0')
Config.write()


class NewsManager:
    """Manages breaking news from Finnhub"""

    def __init__(self, callback, watchlist=None):
        self.callback = callback
        self.watchlist = set([x.upper() for x in (watchlist or [])])
        self.finnhub_key = os.getenv('FINNHUB_API_KEY')
        self.running = False
        self.news_cache = {}
        self.company_news_fetched = set()
        self.breaking_keywords = [
            'breaking', 'just announced', 'unconfirmed', 'exclusive',
            'soars', 'record', 'beat', 'expands', 'strong demand',
            'plunges', 'recall', 'layoffs', 'bankruptcy', 'downgrade',
            'fda approval', 'phase 3 results', 'patent granted', 'new product launch',
            'breakthrough technology', 'ai integration',
            'acquisition', 'merger', 'buyout', 'takeover offer', 'partnership',
            'strategic investment',
            'ceo resignation', 'ceo appointment', 'insider buying', 'insider selling',
            'board shakeup',
            'earnings beat', 'earnings miss', 'revenue up', 'revenue down',
            'guidance raised', 'guidance cut', 'eps above expectations',
            'eps below expectations', 'record profit', 'record loss', 'upgrade',
            'sec investigation', 'lawsuit settlement', 'antitrust', 'sanctions',
            'rate hike', 'rate cut',
            'jumps', 'drops', 'hits new 52-week', 'unusual volume',
            'crypto', 'cryptocurrency'
        ]

    def start_news_stream(self):
        self.running = True
        threading.Thread(target=self._finnhub_news_loop, daemon=True).start()
        print("Starting Finnhub news monitoring...")

    def _finnhub_news_loop(self):
        while self.running:
            try:
                self.fetch_finnhub_news()
                time.sleep(10)
            except Exception as e:
                print(f"Finnhub news error: {e}")
                time.sleep(15)

    def fetch_finnhub_news(self):
        """Fetch general market news"""
        try:
            if not self.finnhub_key:
                return
            url = f"https://finnhub.io/api/v1/news?category=general&token={self.finnhub_key}"
            response = requests.get(url, timeout=5)
            data = response.json()

            if isinstance(data, list):
                for article in data[:20]:
                    self.process_news_article(article)

        except Exception as e:
            print(f"Finnhub general news fetch error: {e}")

    def fetch_company_news_on_demand(self, symbol):
        """Fetch company-specific news when stock becomes channel candidate"""
        symbol = symbol.upper()
        if symbol in self.company_news_fetched or not self.finnhub_key:
            return

        try:
            to_date = datetime.datetime.now().strftime('%Y-%m-%d')
            from_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

            url = (
                f"https://finnhub.io/api/v1/company-news?"
                f"symbol={symbol}&from={from_date}&to={to_date}&token={self.finnhub_key}"
            )
            response = requests.get(url, timeout=5)
            data = response.json()

            if isinstance(data, list) and len(data) > 0:
                for article in data[:3]:
                    # Ensure 'related' includes the symbol
                    related = article.get('related')
                    if not related:
                        article['related'] = symbol
                    else:
                        # If related is a list, join it; if string, ensure symbol is included
                        if isinstance(related, list):
                            related_list = [r.strip().upper() for r in related if r]
                            if symbol not in related_list:
                                related_list.append(symbol)
                            article['related'] = ",".join(related_list)
                        else:
                            related_str = str(related)
                            if symbol not in related_str.upper():
                                article['related'] = f"{related_str},{symbol}"

                    self.process_news_article(article, source='company')

            self.company_news_fetched.add(symbol)

        except Exception as e:
            print(f"Company news error for {symbol}: {e}")

    def process_news_article(self, article, source='general'):
        """Process and allocate news to related symbols"""
        try:
            title = article.get('headline') or article.get('title') or ''
            content = article.get('summary') or article.get('description') or ''
            related_raw = article.get('related') or article.get('symbols') or ''
            article_id = article.get('id') or article.get('url') or ''
            timestamp = article.get('datetime') or article.get('time') or 0

            # Normalize article id
            if not article_id:
                article_id = f"{title[:80]}::{timestamp}"

            if article_id in self.news_cache:
                return
            self.news_cache[article_id] = True

            headline_content = (title + " " + content).lower()
            is_breaking = any(keyword.lower() in headline_content for keyword in self.breaking_keywords)

            # Normalize related symbols into list
            symbols = []
            if related_raw:
                if isinstance(related_raw, list):
                    symbols = [s.strip().upper() for s in related_raw if s]
                else:
                    symbols = [s.strip().upper() for s in str(related_raw).split(',') if s.strip()]

            matched_symbols = [s for s in symbols if s in self.watchlist]

            # If no matched symbols, but breaking crypto news, broadcast to a few watchlist symbols
            if not matched_symbols:
                is_crypto_news = "crypto" in headline_content or "cryptocurrency" in headline_content
                if is_breaking and is_crypto_news and self.watchlist:
                    # schedule a small set of fake 'crypto' news for a few watchlist symbols
                    for symbol in list(self.watchlist)[:3]:
                        news_data = {
                            'symbol': symbol,
                            'title': f"[CRYPTO] {title}",
                            'content': content,
                            'is_breaking': True,
                            'timestamp': timestamp
                        }
                        # Schedule UI callback safely on Kivy's clock
                        Clock.schedule_once(lambda dt, nd=news_data: self.callback(nd), 0)
                return

            # Send news to matched symbols
            for symbol in matched_symbols:
                if symbol.startswith("CRYPTO:") or symbol.startswith("FOREX:"):
                    continue

                news_data = {
                    'symbol': symbol,
                    'title': title,
                    'content': content,
                    'is_breaking': is_breaking,
                    'timestamp': timestamp
                }
                Clock.schedule_once(lambda dt, nd=news_data: self.callback(nd), 0)

        except Exception as e:
            print(f"Error processing article: {e}")

    def stop(self):
        self.running = False


class MarketDataManager:
    """Manages yfinance bulk scanning for 8,000+ stocks"""

    def __init__(self, callback, news_manager_ref=None):
        self.callback = callback
        self.news_manager_ref = news_manager_ref
        self.running = False
        self.stock_data = {}
        self.all_us_tickers = []
        self.market_open_time = None
        self.scan_count = 0

    def fetch_all_us_tickers(self):
        """Fetch ALL US stock tickers from NASDAQ FTP"""
        print("Fetching complete US stock universe from NASDAQ...")
        try:
            # NASDAQ listed stocks
            nasdaq_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
            nasdaq_df = pd.read_csv(nasdaq_url, sep='|', dtype=str, skiprows=1, engine='python', error_bad_lines=False)
            nasdaq_tickers = nasdaq_df[nasdaq_df.get('Test Issue') == 'N']['Symbol'].tolist()

            # NYSE/AMEX stocks
            other_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt'
            other_df = pd.read_csv(other_url, sep='|', dtype=str, skiprows=1, engine='python', error_bad_lines=False)
            # 'ACT Symbol' column used in otherlisted file
            other_tickers = other_df[other_df.get('Test Issue') == 'N']['ACT Symbol'].tolist()

            # Combine and clean
            all_tickers = list(set((nasdaq_tickers or []) + (other_tickers or [])))
            # Remove invalid symbols
            all_tickers = [t for t in all_tickers if t and not str(t).startswith('$') and len(str(t)) <= 5]

            self.all_us_tickers = sorted(all_tickers)
            print(f"Loaded {len(self.all_us_tickers)} US stock tickers")

        except Exception as e:
            print(f"Error fetching ticker universe: {e}")
            print("Falling back to top 500 stocks...")
            self.all_us_tickers = self._get_fallback_tickers()

    def _get_fallback_tickers(self):
        """Fallback to curated list if NASDAQ FTP fails"""
        return [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'UNH', 'JNJ',
            'V', 'XOM', 'WMT', 'LLY', 'JPM', 'PG', 'MA', 'HD', 'CVX', 'ABBV',
        ]

    def start_bulk_scanner(self):
        """Start the bulk yfinance scanner"""
        self.fetch_all_us_tickers()
        self.running = True

        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        self.market_open_time = now_est.replace(hour=9, minute=30, second=0, microsecond=0)

        # Start scanning loop
        threading.Thread(target=self._bulk_scan_loop, daemon=True).start()
        print("Starting yfinance bulk scanner (5-minute refresh)...")

    def _bulk_scan_loop(self):
        """Continuously scan all stocks every 5 minutes"""
        while self.running:
            try:
                self.scan_count += 1
                print(f"\n=== SCAN #{self.scan_count} - Downloading {len(self.all_us_tickers)} stocks ===")
                start_time = time.time()

                self.bulk_download_and_process()

                elapsed = time.time() - start_time
                print(f"Scan completed in {elapsed:.1f} seconds")

                # Wait 5 minutes before next scan
                time.sleep(300)

            except Exception as e:
                print(f"Bulk scan error: {e}")
                time.sleep(60)

    def bulk_download_and_process(self):
        """Download all stocks and process into channels"""
        try:
            if not self.all_us_tickers:
                print("No tickers available to download")
                return

            # Bulk download with multithreading (yfinance)
            print("Downloading bulk data from Yahoo Finance...")
            try:
                data = yf.download(
                    tickers=self.all_us_tickers,
                    period="1d",
                    interval="1m",
                    group_by="ticker",
                    threads=True,
                    progress=False
                )
            except Exception:
                data = None

            processed_count = 0

            # Process each stock individually using Ticker object
            for symbol in self.all_us_tickers:
                try:
                    ticker_obj = yf.Ticker(symbol)

                    # Get intraday data
                    hist = ticker_obj.history(period="1d", interval="1m")
                    if hist is None or hist.empty:
                        continue

                    # Get current price and volume
                    current_price = float(hist['Close'].iloc[-1])
                    current_volume = int(hist['Volume'].sum())  # Total volume for the day

                    if current_price == 0 or current_volume == 0:
                        continue

                    # Get info for prev_close, avg_volume, float
                    info = {}
                    try:
                        info = ticker_obj.info or {}
                    except Exception:
                        info = {}

                    prev_close = float(info.get('previousClose', info.get('regularMarketPreviousClose', current_price)))
                    avg_volume = float(info.get('averageVolume', info.get('averageDailyVolume10Day', 50_000_000)))
                    shares_outstanding = float(info.get('sharesOutstanding', 0))
                    float_shares = (shares_outstanding / 1_000_000) if shares_outstanding else 0.0  # Convert to millions

                    # Calculate metrics
                    if prev_close > 0:
                        change_pct = ((current_price - prev_close) / prev_close) * 100
                    else:
                        change_pct = 0.0

                    # Calculate RVOL
                    rvol = self.calculate_rvol(current_volume, avg_volume)

                    # Store data
                    self.stock_data[symbol] = {
                        'price': current_price,
                        'volume': current_volume,
                        'prev_close': prev_close,
                        'change_pct': change_pct,
                        'avg_volume': avg_volume,
                        'float': float_shares,
                        'rvol': rvol,
                        'timestamp': time.time()
                    }

                    # Trigger news fetch for strong candidates
                    if self.is_strong_channel_candidate(symbol, change_pct, rvol):
                        if self.news_manager_ref:
                            threading.Thread(
                                target=self.news_manager_ref.fetch_company_news_on_demand,
                                args=(symbol,),
                                daemon=True
                            ).start()

                    # Send to UI callback
                    if self.callback:
                        print(f"[CALLBACK SCHEDULED] {symbol}: ${current_price:.2f}, change={change_pct:+.1f}%, rvol={rvol:.2f}x")
                        Clock.schedule_once(
                            lambda dt, s=symbol, d=self.stock_data[symbol]: self.callback(s, d),
                            0
                        )

                    processed_count += 1

                    # Print progress every 100 stocks
                    if processed_count % 100 == 0:
                        print(f"Processed {processed_count} stocks...")

                except Exception:
                    # Skip problematic symbols silently (keeps scanner robust)
                    continue

            print(f"Processed {processed_count} stocks successfully")

        except Exception as e:
            print(f"Bulk download error: {e}")

    def calculate_rvol(self, current_volume, avg_volume):
        """Calculate time-adjusted RVOL"""
        try:
            if avg_volume <= 0 or current_volume <= 0:
                return 0.0

            est = pytz.timezone('US/Eastern')
            now_est = datetime.datetime.now(est)

            if self.market_open_time and now_est >= self.market_open_time:
                elapsed_minutes = (now_est - self.market_open_time).total_seconds() / 60

                if elapsed_minutes < 60:
                    expected_ratio = 0.40 * (elapsed_minutes / 60)
                elif elapsed_minutes < 120:
                    expected_ratio = 0.40 + 0.25 * ((elapsed_minutes - 60) / 60)
                else:
                    expected_ratio = min(elapsed_minutes / 390, 1.0)

                expected_volume = avg_volume * expected_ratio

                if expected_volume > 0:
                    return current_volume / expected_volume

            # Fallback to simple ratio
            return current_volume / avg_volume

        except Exception:
            return 0.0

    def is_strong_channel_candidate(self, symbol, change_pct, rvol):
        """Determine if stock needs company news"""
        if abs(change_pct) > 5:
            return True
        if change_pct > 2:
            return True
        if rvol > 3.0:
            return True
        return False

    def get_index_data(self, symbol):
        """Get index data using Yahoo Finance"""
        try:
            yahoo_symbol_map = {
                ".IXIC": "^IXIC",
                ".SPX": "^GSPC"
            }
            yahoo_symbol = yahoo_symbol_map.get(symbol, symbol)

            ticker = yf.Ticker(yahoo_symbol)
            hist = ticker.history(period="1d", interval="1m")

            if hist is not None and not hist.empty:
                current_price = float(hist['Close'].iloc[-1])
                info = {}
                try:
                    info = ticker.info or {}
                except Exception:
                    info = {}
                prev_close = float(info.get('previousClose', info.get('regularMarketPreviousClose', current_price)))

                if prev_close > 0:
                    change_pct = ((current_price - prev_close) / prev_close) * 100
                    return current_price, change_pct
                else:
                    return current_price, 0.0

        except Exception as e:
            print(f"Yahoo Finance error for {symbol}: {e}")

        return 0.0, 0.0

    def stop(self):
        """Stop scanner"""
        self.running = False


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
        self.current_channel = "RunUp"
        self.nasdaq_last, self.nasdaq_pct = 0.0, 0.0
        self.sp_last, self.sp_pct = 0.0, 0.0

        self.news_manager = None
        self.market_data = MarketDataManager(callback=self.on_data_update, news_manager_ref=None)

        self.build_header()
        main_content = BoxLayout(orientation="vertical", spacing=0, padding=0)
        self.tabs_container = self.build_channel_tabs()
        main_content.add_widget(self.tabs_container)
        self.data_container = self.build_data_section()
        main_content.add_widget(self.data_container)
        self.add_widget(main_content)

        Clock.schedule_interval(self.update_times, 1)
        Clock.schedule_once(self.start_market_data, 2)
        Clock.schedule_once(self.start_news_feed, 3)
        Clock.schedule_once(self.update_indices, 5)

    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size

    def start_market_data(self, dt=None):
        """Initialize market data streams"""
        print("Starting yfinance bulk market scanner...")
        self.market_data.start_bulk_scanner()

    def start_news_feed(self, dt=None):
        """Initialize news feed"""
        wl = self.market_data.all_us_tickers or []
        self.news_manager = NewsManager(callback=self.on_news_update, watchlist=wl)
        self.market_data.news_manager_ref = self.news_manager
        self.news_manager.start_news_stream()

    def on_data_update(self, symbol, data):
        """Callback when scanner processes new stock data"""
        print(f"[UI CALLBACK RECEIVED] {symbol}: {data}")
        self.process_stock_update(symbol, data)

    def on_news_update(self, news_data):
        """Callback when breaking news arrives"""
        symbol = news_data.get('symbol', '')
        title = news_data.get('title', '')
        is_breaking = news_data.get('is_breaking', False)

        if symbol not in self.stock_news:
            self.stock_news[symbol] = {'title': title, 'is_breaking': is_breaking}
        else:
            self.stock_news[symbol].update({'title': title, 'is_breaking': is_breaking})

        self.stock_news[symbol]['tier'] = 2 if is_breaking else 3

        Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)

    def process_stock_update(self, symbol, data):
        """Process stock data and categorize"""
        print(f"[PROCESSING] {symbol}")
        try:
            price = data.get('price', 0)
            volume = data.get('volume', 0)
            change_pct = data.get('change_pct', 0)
            float_shares = data.get('float', 0)
            rvol = data.get('rvol', 0)

            if price == 0:
                return

            # Format float
            if float_shares > 0:
                if float_shares >= 1000:
                    float_str = f"{float_shares/1000:.1f}B"
                else:
                    float_str = f"{float_shares:.1f}M"
            else:
                float_str = "N/A"

            rvol_str = f"{rvol:.2f}x" if rvol > 0 else "0.00x"

            formatted_data = [
                symbol,
                f"${price:.2f}",
                f"{change_pct:+.1f}%",
                self.format_volume(volume),
                float_str,
                rvol_str,
                ""
            ]

            self.categorize_stock(formatted_data, change_pct, volume, rvol)

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    def categorize_stock(self, stock_data, change_pct, volume, rvol):
        """Categorize stock into scanner channels"""
        ticker = stock_data[0]
        print(f"[CATEGORIZE] {ticker}: change={change_pct:+.1f}%, rvol={rvol:.2f}x")

        # Remove existing entries from all channels
        for channel in self.live_data:
            self.live_data[channel] = [s for s in self.live_data[channel] if s[0] != ticker]

        # Categorize (additive)
        if abs(change_pct) > 5:
            self.live_data["PreGap"].append(stock_data)
        if change_pct > 2:
            self.live_data["HOD"].append(stock_data)
        if change_pct > 0:
            self.live_data["RunUp"].append(stock_data)
        if change_pct < 0:
            self.live_data["RunDown"].append(stock_data)
        if rvol > 2.0:
            self.live_data["Rvsl"].append(stock_data)

        # Sort by RVOL (column 5, e.g., '1.23x')
        for channel in self.live_data:
            try:
                self.live_data[channel].sort(key=lambda x: float(str(x[5]).replace('x', '')), reverse=True)
            except Exception:
                pass

        Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)

    def update_indices(self, dt=None):
        """Update NASDAQ and S&P 500"""
        self.nasdaq_last, self.nasdaq_pct = self.market_data.get_index_data(".IXIC")
        self.sp_last, self.sp_pct = self.market_data.get_index_data(".SPX")

        try:
            self.nasdaq_label.text = f"NASDAQ  {self.nasdaq_pct:+.1f}%"
            self.nasdaq_label.color = (0, 1, 0, 1) if self.nasdaq_pct > 0 else (1, 0, 0, 1)

            self.sp_label.text = f"S&P     {self.sp_pct:+.1f}%"
            self.sp_label.color = (0, 1, 0, 1) if self.sp_pct > 0 else (1, 0, 0, 1)
        except Exception:
            pass

        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        state, _ = self.get_market_state_and_color(now_est)

        if state == "Market Open":
            next_interval = 300
        elif state in ["PreMarket", "After Hours"]:
            next_interval = 900
        else:
            next_interval = 1800

        Clock.unschedule(self.update_indices)
        Clock.schedule_once(self.update_indices, next_interval)

    def format_volume(self, volume):
        """Format volume in millions/billions"""
        try:
            volume = int(volume)
        except Exception:
            return str(volume)

        if volume >= 1_000_000_000:
            return f"{volume/1_000_000_000:.1f}B"
        elif volume >= 1_000_000:
            return f"{volume/1_000_000:.1f}M"
        elif volume >= 1_000:
            return f"{volume/1_000:.1f}K"
        return str(int(volume))

    def build_header(self):
        header = BoxLayout(orientation="horizontal", size_hint=(1, None), height=80, padding=[15, 10, 15, 10])
        with header.canvas.before:
            Color(0.12, 0.12, 0.12, 1)
            header.bg_rect = Rectangle(size=header.size, pos=header.pos)
        header.bind(size=lambda inst, val: setattr(header.bg_rect, "size", inst.size))
        header.bind(pos=lambda inst, val: setattr(header.bg_rect, "pos", inst.pos))

        title = Label(text="SignalScan ENHANCED", font_size=26, color=(0, 1, 0, 1), size_hint=(None, 1), width=240)
        header.add_widget(title)

        times_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=160)
        self.local_time_label = Label(text="Local Time    --:--", font_size=14, color=(0.8, 0.8, 0.8, 1))
        self.nyc_time_label = Label(text="NYC Time     --:--", font_size=14, color=(0.8, 0.8, 0.8, 1))
        times_section.add_widget(self.local_time_label)
        times_section.add_widget(self.nyc_time_label)
        header.add_widget(times_section)

        center_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=160)
        self.market_state_label = Label(text="Weekend", font_size=16, color=(1, 0.6, 0, 1))
        self.countdown_label = Label(text="00:00:00", font_size=14, color=(1, 0.6, 0, 1))
        center_section.add_widget(self.market_state_label)
        center_section.add_widget(self.countdown_label)
        header.add_widget(center_section)

        indicators_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=140)
        self.nasdaq_label = Label(text=f"NASDAQ  +0.0%", font_size=13, color=(0, 1, 0, 1))
        self.sp_label = Label(text=f"S&P     +0.0%", font_size=13, color=(0, 1, 0, 1))
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
            btn = Button(text=channel, font_size=16, size_hint=(1, 1))
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
            label = Label(text=header, font_size=15, color=(0.7, 0.7, 0.7, 1))
            header_layout.add_widget(label)
        data_section.add_widget(header_layout)

        self.scroll_view = ScrollView(size_hint=(1, 1))
        self.rows_container = BoxLayout(orientation="vertical", size_hint_y=None, spacing=2)
        self.rows_container.bind(minimum_height=self.rows_container.setter('height'))

        self.scroll_view.add_widget(self.rows_container)
        data_section.add_widget(self.scroll_view)

        return data_section

    def refresh_data_table(self):
        """Refresh data table"""
        self.rows_container.clear_widgets()

        stocks = self.live_data.get(self.current_channel, [])

        for stock_data in stocks[:50]:
            row = self.create_stock_row(stock_data)
            if row:
                self.rows_container.add_widget(row)

    def create_stock_row(self, stock_data):
        """Create stock row with news buttons"""
        row = BoxLayout(orientation="horizontal", size_hint=(1, None), height=40)

        try:
            change_str = stock_data[2].replace('%', '').replace('+', '')
            change_pct = float(change_str)
            text_color = (0, 1, 0, 1) if change_pct >= 0 else (1, 0, 0, 1)
        except Exception:
            text_color = (0.9, 0.9, 0.9, 1)

        for i, value in enumerate(stock_data):
            if i == 6:
                ticker = stock_data[0]

                if ticker in self.stock_news:
                    tier = self.stock_news[ticker].get('tier', 3)
                    if tier == 2:
                        btn_color = (1, 1, 0, 1)
                        btn_text = "BREAKING"
                        text_color_btn = (0, 0, 0, 1)
                    else:
                        btn_color = (0, 0.8, 1, 1)
                        btn_text = "NEWS"
                        text_color_btn = (0, 0, 0, 1)
                else:
                    btn_color = (1, 0.3, 0.3, 1)
                    btn_text = "NO NEWS"
                    text_color_btn = (1, 1, 1, 1)

                btn = Button(text=btn_text, font_size=12, size_hint=(1, 1),
                             background_color=btn_color, color=text_color_btn)

                news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
                btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
                row.add_widget(btn)
            else:
                label = Label(text=str(value), font_size=14, color=text_color if i > 0 else (1, 1, 1, 1))
                row.add_widget(label)

        return row

    def show_news_popup(self, ticker, news_text):
        content = BoxLayout(orientation="vertical", padding=20, spacing=15)
        title_label = Label(text=f"{ticker} - NEWS", font_size=18, size_hint=(1, None), height=40, color=(0, 1, 0, 1))
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
        if channel in self.channel_buttons:
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
        if self.news_manager:
            self.news_manager.stop()
        App.get_running_app().stop()


class SignalScanMainApp(App):
    def build(self):
        return SignalScanApp()


if __name__ == '__main__':
    SignalScanMainApp().run()