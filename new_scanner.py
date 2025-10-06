"""
SignalScan Enhanced - Full US Market Scanner
Version 7.3 - Time-Restricted Sound Alerts + PyGame Audio
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
import pygame.mixer
import datetime
import pytz
import os
from dotenv import load_dotenv
import threading
import time
import requests
import pandas as pd
import yfinance as yf
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

load_dotenv()

Config.set('graphics', 'fullscreen', 'auto')
Config.set('graphics', 'borderless', '1')
Config.set('graphics', 'resizable', '0')
Config.write()

NY_TZ = pytz.timezone('America/New_York')


class SoundManager:
    """Manages sound alerts using PyGame mixer with time restrictions"""
    def __init__(self):
        # Initialize PyGame mixer for audio playback
        pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=2048)
        pygame.mixer.set_num_channels(8)
        
        self.sounds = {}
        self.sound_dir = "sounds"
        self.bell_played_open = False
        self.bell_played_close = False
        self.premarket_played = False
        self._load_sounds()

    def _load_sounds(self):
        sound_files = {
            'bell': 'nyse_bell.wav',
            'news': 'iphone_news_flash.wav',
            'premarket': 'woke_up_this_morning.wav',
            'candidate': 'morse_code_alert.wav'
        }
        
        for sound_name, filename in sound_files.items():
            filepath = os.path.join(self.sound_dir, filename)
            if os.path.exists(filepath):
                try:
                    self.sounds[sound_name] = pygame.mixer.Sound(filepath)
                    print(f"✓ Loaded: {filename}")
                except pygame.error as e:
                    print(f"✗ Failed to load {filename}: {e}")
            else:
                print(f"✗ Missing: {filepath}")

    def is_sound_allowed(self):
        """Check if sounds are allowed based on time (7 AM - 8 PM ET)"""
        now_est = datetime.datetime.now(NY_TZ)
        start_hour = 7
        end_hour = 20
        
        if start_hour <= now_est.hour < end_hour:
            return True
        else:
            return False

    def play_sound(self, sound_name):
        if not self.is_sound_allowed():
            print(f"[SOUND] {sound_name} muted (quiet hours)")
            return
        
        if sound_name in self.sounds:
            try:
                self.sounds[sound_name].play()
                print(f"♪ Playing: {sound_name}")
            except Exception as e:
                print(f"✗ Sound playback error for {sound_name}: {e}")
        else:
            print(f"✗ Sound not loaded: {sound_name}")

    def play_bell(self):
        self.play_sound('bell')
        print("[SOUND] NYSE Bell")

    def play_news_alert(self):
        """MUTED - Breaking news alerts disabled"""
        print("[SOUND] Breaking News alert (muted)")

    def play_premarket_alert(self):
        self.play_sound('premarket')
        if 'premarket' in self.sounds:
            print("[SOUND] Pre-market alert (Sopranos)")
        else:
            print("[SOUND] Pre-market alert (file missing)")

    def play_candidate_alert(self):
        self.play_sound('candidate')
        print("[SOUND] HOD Candidate (Morse code)")

    def check_market_bells(self, now_est):
        market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        premarket_alert = now_est.replace(hour=7, minute=0, second=0, microsecond=0)

        if now_est.hour == 0 and now_est.minute == 0:
            self.bell_played_open = False
            self.bell_played_close = False
            self.premarket_played = False

        if (not self.premarket_played
            and premarket_alert <= now_est < premarket_alert + datetime.timedelta(seconds=10)):
            self.play_premarket_alert()
            self.premarket_played = True

        if (not self.bell_played_open
            and market_open <= now_est < market_open + datetime.timedelta(seconds=10)):
            self.play_bell()
            self.bell_played_open = True

        if (not self.bell_played_close
            and market_close <= now_est < market_close + datetime.timedelta(seconds=10)):
            self.play_bell()
            self.bell_played_close = True


class HaltManager:
    """Fetches real trading halts from Nasdaq RSS feed (today only)"""
    def __init__(self, callback):
        self.callback = callback
        self.running = False
        self.halt_data = {}
        self.rss_url = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"

    def start_halt_monitor(self):
        self.running = True
        threading.Thread(target=self._halt_loop, daemon=True).start()
        print("Starting Nasdaq halt monitor...")

    def _halt_loop(self):
        while self.running:
            try:
                self.fetch_halts()
                time.sleep(60)
            except Exception as e:
                print(f"Halt fetch error: {e}")
                time.sleep(60)

    def fetch_halts(self):
        try:
            r = requests.get(self.rss_url, timeout=10)
            r.raise_for_status()
            
            root = ET.fromstring(r.content)
            today_et = datetime.datetime.now(NY_TZ).date()
            self.halt_data = {}
            
            for item in root.findall('.//item'):
                try:
                    title = item.find('title').text or ''
                    description = item.find('description').text or ''
                    pub_date = item.find('pubDate').text or ''
                    
                    if 'table' in title.lower() or 'table' in description.lower():
                        continue
                    if len(title) < 2 or title.startswith('-'):
                        continue
                    
                    try:
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_datetime_et = pub_datetime.astimezone(NY_TZ)
                        
                        if pub_datetime_et.date() != today_et:
                            continue
                    except Exception:
                        continue
                    
                    parts = description.split('::')
                    if len(parts) >= 4:
                        symbol = parts[0].strip()
                        halt_time = parts[1].strip()
                        reason = parts[2].strip()
                        resume_time = parts[3].strip() if len(parts) > 3 else 'Pending'
                    else:
                        words = description.split()
                        symbol = words[0] if words else 'UNK'
                        halt_time = pub_datetime_et.strftime('%I:%M %p ET')
                        reason = description[:50]
                        resume_time = 'Pending'
                    
                    if not symbol.isalpha() or len(symbol) > 5:
                        continue
                    
                    exchange = 'NASDAQ'
                    if 'NYSE' in description.upper() or 'NYSE' in title.upper():
                        exchange = 'NYSE'
                    elif 'AMEX' in description.upper():
                        exchange = 'AMEX'
                    
                    self.halt_data[symbol] = {
                        'symbol': symbol,
                        'halt_time': halt_time,
                        'reason': reason,
                        'resume_time': resume_time,
                        'exchange': exchange
                    }
                    
                except Exception:
                    continue
            
            if self.callback:
                Clock.schedule_once(lambda dt: self.callback(self.halt_data), 0)
            
            if len(self.halt_data) > 0:
                print(f"[HALTS] {len(self.halt_data)} active halts today")
            else:
                print(f"[HALTS] No active halts")
            
        except Exception as e:
            print(f"Halt RSS fetch error: {e}")

    def stop(self):
        self.running = False


class NewsManager:
    """Manages breaking news - sound exempt for Halts channel"""
    def __init__(self, callback, sound_manager_ref, watchlist=None):
        self.callback = callback
        self.sound_manager_ref = sound_manager_ref
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
        try:
            if not self.finnhub_key:
                return
            url = f"https://finnhub.io/api/v1/news?category=general&token={self.finnhub_key}"
            r = requests.get(url, timeout=5)
            data = r.json()
            if isinstance(data, list):
                for article in data[:20]:
                    self.process_news_article(article)
        except Exception as e:
            print(f"Finnhub general news fetch error: {e}")

    def fetch_company_news_on_demand(self, symbol):
        symbol = symbol.upper()
        if symbol in self.company_news_fetched or not self.finnhub_key:
            return
        try:
            to_date = datetime.datetime.now(NY_TZ).strftime('%Y-%m-%d')
            from_date = (datetime.datetime.now(NY_TZ) - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={to_date}&token={self.finnhub_key}"
            r = requests.get(url, timeout=5)
            data = r.json()
            if isinstance(data, list):
                for article in data[:3]:
                    related = article.get('related')
                    if not related:
                        article['related'] = symbol
                    else:
                        if isinstance(related, list):
                            rel = [x.strip().upper() for x in related if x]
                            if symbol not in rel:
                                rel.append(symbol)
                            article['related'] = ",".join(rel)
                        else:
                            rs = str(related)
                            if symbol not in rs.upper():
                                article['related'] = f"{rs},{symbol}"
                    self.process_news_article(article, source='company')
            self.company_news_fetched.add(symbol)
        except Exception as e:
            print(f"Company news error for {symbol}: {e}")

    def process_news_article(self, article, source='general'):
        try:
            title = article.get('headline') or article.get('title') or ''
            content = article.get('summary') or article.get('description') or ''
            related_raw = article.get('related') or article.get('symbols') or ''
            article_id = article.get('id') or article.get('url') or ''
            ts = article.get('datetime') or article.get('time') or 0

            if not article_id:
                article_id = f"{title[:80]}::{ts}"
            
            if article_id in self.news_cache:
                return
            
            self.news_cache[article_id] = True

            text = (title + " " + content).lower()
            is_breaking = any(k in text for k in self.breaking_keywords)

            symbols = []
            if related_raw:
                if isinstance(related_raw, list):
                    symbols = [s.strip().upper() for s in related_raw if s]
                else:
                    symbols = [s.strip().upper() for s in str(related_raw).split(',') if s.strip()]

            matched = [s for s in symbols if s in self.watchlist]

            if not matched:
                return

            for sym in matched:
                if sym.startswith("CRYPTO:") or sym.startswith("FOREX:"):
                    continue
                
                nd = {'symbol': sym, 'title': title, 'content': content,
                      'is_breaking': is_breaking, 'timestamp': ts}
                Clock.schedule_once(lambda dt, d=nd: self.callback(d), 0)
                
                if is_breaking:
                    Clock.schedule_once(lambda dt, s=sym, t=title: self._check_and_play_sound(s, t), 0.2)

        except Exception as e:
            print(f"Error processing article: {e}")

    def _check_and_play_sound(self, symbol, title):
        """Check if ticker is on scan (EXCLUDING Halts) before playing sound"""
        try:
            app = App.get_running_app()
            if not hasattr(app.root, 'live_data'):
                return
            
            live_data = app.root.live_data
            ticker_on_channel = False
            
            for channel_name, channel_stocks in live_data.items():
                if channel_name == "Halts":
                    continue
                if any(stock[0] == symbol for stock in channel_stocks):
                    ticker_on_channel = True
                    break
            
            if ticker_on_channel and self.sound_manager_ref:
                self.sound_manager_ref.play_news_alert()
        except Exception as e:
            print(f"Sound check error: {e}")

    def stop(self):
        self.running = False


class MarketDataManager:
    """Manages yfinance bulk scanning"""
    def __init__(self, callback, news_manager_ref=None):
        self.callback = callback
        self.news_manager_ref = news_manager_ref
        self.running = False
        self.stock_data = {}
        self.price_history = {}
        self.all_us_tickers = []
        self.market_open_time = None
        self.scan_count = 0

    def fetch_all_us_tickers(self):
        print("Fetching US stock universe...")
        try:
            nasdaq_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
            other_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt'

            nasdaq_df = pd.read_csv(nasdaq_url, sep='|')
            other_df = pd.read_csv(other_url, sep='|')

            nasdaq_list = nasdaq_df[nasdaq_df['Test Issue'] == 'N']['Symbol'].tolist()
            other_list = other_df[other_df['Test Issue'] == 'N']['ACT Symbol'].tolist()

            combined = list(set((nasdaq_list or []) + (other_list or [])))

            clean = []
            for t in combined:
                s = str(t).strip() if t is not None else ""
                if not s or s.lower() == 'nan' or s.startswith('$') or len(s) > 5:
                    continue
                clean.append(s)

            self.all_us_tickers = sorted(set(clean))
            print(f"Loaded {len(self.all_us_tickers)} tickers")
        except Exception as e:
            print(f"Error fetching tickers: {e}")
            self.all_us_tickers = ['AAPL', 'MSFT', 'GOOGL']

    def start_bulk_scanner(self):
        self.fetch_all_us_tickers()
        self.running = True
        now_est = datetime.datetime.now(NY_TZ)
        self.market_open_time = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        threading.Thread(target=self._bulk_scan_loop, daemon=True).start()

    def _bulk_scan_loop(self):
        while self.running:
            try:
                self.scan_count += 1
                print(f"\n=== SCAN #{self.scan_count} ===")
                start_time = time.time()
                self.bulk_download_and_process()
                print(f"Scan completed in {time.time() - start_time:.1f}s")
                time.sleep(300)
            except Exception as e:
                print(f"Scan error: {e}")
                time.sleep(60)

    def bulk_download_and_process(self):
        try:
            processed = 0
            for symbol in self.all_us_tickers:
                try:
                    t = yf.Ticker(symbol)
                    hist = t.history(period="1d", interval="1m")
                    if hist is None or hist.empty:
                        continue

                    current_price = float(hist['Close'].iloc[-1])
                    current_volume = int(hist['Volume'].sum())
                    day_high = float(hist['High'].max())

                    if current_price > 50.0 or current_price <= 0 or current_volume <= 0:
                        continue

                    info = {}
                    try:
                        info = t.info or {}
                    except:
                        pass

                    prev_close = float(info.get('previousClose', info.get('regularMarketPreviousClose', current_price)))
                    avg_volume = float(info.get('averageVolume', 50_000_000))
                    shares_outstanding = float(info.get('sharesOutstanding', 0))
                    float_shares = (shares_outstanding / 1_000_000) if shares_outstanding else 0.0
                    week52_high = float(info.get('fiftyTwoWeekHigh', 0))

                    change_pct = ((current_price - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
                    rvol = self.calculate_rvol(current_volume, avg_volume)

                    ph = self.price_history.get(symbol, {'prev_high': day_high})
                    is_new_hod = current_price >= ph['prev_high'] and current_price > prev_close
                    self.price_history[symbol] = {'prev_high': max(day_high, ph['prev_high'])}

                    is_52wk_high = (week52_high > 0 and abs(current_price - week52_high) / week52_high < 0.01)

                    self.stock_data[symbol] = {
                        'price': current_price,
                        'volume': current_volume,
                        'prev_close': prev_close,
                        'change_pct': change_pct,
                        'avg_volume': avg_volume,
                        'float': float_shares,
                        'rvol': rvol,
                        'is_new_hod': is_new_hod,
                        'is_52wk_high': is_52wk_high,
                        'timestamp': time.time()
                    }

                    if self.is_strong_channel_candidate(symbol, change_pct, rvol) and self.news_manager_ref:
                        threading.Thread(
                            target=self.news_manager_ref.fetch_company_news_on_demand,
                            args=(symbol,),
                            daemon=True
                        ).start()

                    if self.callback:
                        Clock.schedule_once(lambda dt, s=symbol, d=self.stock_data[symbol]: self.callback(s, d), 0)

                    processed += 1
                except:
                    continue

            print(f"Processed {processed} stocks")
        except Exception as e:
            print(f"Download error: {e}")

    def calculate_rvol(self, current_volume, avg_volume):
        try:
            if avg_volume <= 0 or current_volume <= 0:
                return 0.0
            now_est = datetime.datetime.now(NY_TZ)
            if self.market_open_time and now_est >= self.market_open_time:
                elapsed = (now_est - self.market_open_time).total_seconds() / 60
                if elapsed < 60:
                    expected_ratio = 0.40 * (elapsed / 60)
                elif elapsed < 120:
                    expected_ratio = 0.40 + 0.25 * ((elapsed - 60) / 60)
                else:
                    expected_ratio = min(elapsed / 390, 1.0)
                expected_volume = avg_volume * expected_ratio
                return (current_volume / expected_volume) if expected_volume > 0 else 0.0
            return current_volume / avg_volume
        except:
            return 0.0

    def is_strong_channel_candidate(self, symbol, change_pct, rvol):
        return abs(change_pct) > 5 or change_pct > 2 or rvol > 3.0

    def get_index_data(self, symbol):
        try:
            m = {".IXIC": "^IXIC", ".SPX": "^GSPC"}
            ys = m.get(symbol, symbol)
            t = yf.Ticker(ys)
            hist = t.history(period="1d", interval="1m")
            if hist is not None and not hist.empty:
                current = float(hist['Close'].iloc[-1])
                info = {}
                try:
                    info = t.info or {}
                except:
                    pass
                prev_close = float(info.get('previousClose', current))
                change_pct = ((current - prev_close) / prev_close) * 100 if prev_close > 0 else 0.0
                return current, change_pct
        except:
            pass
        return 0.0, 0.0

    def stop(self):
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

        self.live_data = {k: [] for k in ["PreGap", "HOD", "RunUp", "RunDown", "Rvsl", "Jumps", "Halts"]}
        self.stock_news = {}
        self.current_channel = "RunUp"
        self.nasdaq_last = self.nasdaq_pct = 0.0
        self.sp_last = self.sp_pct = 0.0

        self.sound_manager = SoundManager()
        self.news_manager = None
        self.market_data = MarketDataManager(callback=self.on_data_update, news_manager_ref=None)
        self.halt_manager = HaltManager(callback=self.on_halt_update)

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
        Clock.schedule_once(self.start_halt_monitor, 4)
        Clock.schedule_once(self.update_indices, 5)

    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size

    def start_market_data(self, dt=None):
        self.market_data.start_bulk_scanner()

    def start_news_feed(self, dt=None):
        wl = self.market_data.all_us_tickers or []
        self.news_manager = NewsManager(callback=self.on_news_update, sound_manager_ref=self.sound_manager, watchlist=wl)
        self.market_data.news_manager_ref = self.news_manager
        self.news_manager.start_news_stream()

    def start_halt_monitor(self, dt=None):
        self.halt_manager.start_halt_monitor()

    def on_data_update(self, symbol, data):
        self.process_stock_update(symbol, data)

    def on_news_update(self, news_data):
        symbol = news_data.get('symbol', '')
        title = news_data.get('title', '')
        is_breaking = news_data.get('is_breaking', False)
        if symbol not in self.stock_news:
            self.stock_news[symbol] = {'title': title, 'is_breaking': is_breaking}
        else:
            self.stock_news[symbol].update({'title': title, 'is_breaking': is_breaking})
        self.stock_news[symbol]['tier'] = 2 if is_breaking else 3
        Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)

    def on_halt_update(self, halt_data):
        """Update Halts channel with real halt data + price/% from scanner"""
        self.live_data["Halts"] = []
        for symbol, halt_info in halt_data.items():
            stock_data = self.market_data.stock_data.get(symbol, {})
            price = stock_data.get('price', 0)
            change_pct = stock_data.get('change_pct', 0)
            
            price_str = f"${price:.2f}" if price > 0 else "N/A"
            pct_str = f"{change_pct:+.1f}%" if price > 0 else "N/A"
            
            self.live_data["Halts"].append([
                symbol,
                halt_info['halt_time'],
                halt_info['reason'][:20],
                halt_info['resume_time'][:10],
                halt_info['exchange'],
                price_str,
                pct_str,
                ""
            ])
        Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)

    def process_stock_update(self, symbol, data):
        try:
            price = data.get('price', 0)
            volume = data.get('volume', 0)
            change_pct = data.get('change_pct', 0)
            float_shares = data.get('float', 0)
            rvol = data.get('rvol', 0)
            is_new_hod = data.get('is_new_hod', False)
            is_52wk_high = data.get('is_52wk_high', False)

            if price == 0:
                return

            if float_shares > 0:
                if float_shares <= 20:
                    float_str = f"{float_shares:.1f}M [LOW]"
                elif float_shares <= 100:
                    float_str = f"{float_shares:.1f}M [MED]"
                elif float_shares >= 1000:
                    float_str = f"{float_shares/1000:.1f}B"
                else:
                    float_str = f"{float_shares:.1f}M"
            else:
                float_str = "N/A"

            rvol_str = f"{rvol:.2f}x" if rvol > 0 else "0.00x"

            formatted = [
                symbol,
                f"${price:.2f}",
                f"{change_pct:+.1f}%",
                self.format_volume(volume),
                float_str,
                rvol_str,
                ""
            ]

            self.categorize_stock(formatted, change_pct, volume, rvol, float_shares, price, is_new_hod, is_52wk_high)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    def categorize_stock(self, stock_data, change_pct, volume, rvol, float_shares, price, is_new_hod, is_52wk_high):
        ticker = stock_data[0]

        was_in_hod = any(s[0] == ticker for s in self.live_data["HOD"])

        for ch in ["PreGap", "HOD", "RunUp", "RunDown", "Rvsl", "Jumps"]:
            self.live_data[ch] = [s for s in self.live_data[ch] if s[0] != ticker]

        now_est = datetime.datetime.now(NY_TZ)
        is_premarket = now_est.hour < 9 or (now_est.hour == 9 and now_est.minute < 30)

        hod_candidate_added = False

        if (is_premarket and 1.0 <= price <= 20.0 and abs(change_pct) >= 10.0
                and volume >= 100000 and float_shares <= 100):
            self.live_data["PreGap"].append(stock_data)

        if ((is_new_hod and rvol >= 3.0 and float_shares <= 100 and change_pct >= 5.0)
                or (is_52wk_high and rvol >= 3.0 and change_pct >= 5.0)):
            self.live_data["HOD"].append(stock_data)
            hod_candidate_added = True

        if (3.0 <= change_pct <= 15.0 and rvol >= 1.5 and volume >= 250000):
            self.live_data["RunUp"].append(stock_data)

        if (change_pct <= -3.0 and rvol >= 1.5 and volume >= 250000):
            self.live_data["RunDown"].append(stock_data)

        if (rvol >= 8.0 and abs(change_pct) >= 8.0):
            self.live_data["Rvsl"].append(stock_data)

        if abs(change_pct) >= 20.0:
            self.live_data["Jumps"].append(stock_data)

        for ch in ["PreGap", "HOD", "RunUp", "RunDown", "Rvsl", "Jumps"]:
            try:
                self.live_data[ch].sort(key=lambda x: float(str(x[5]).replace('x', '')), reverse=True)
            except:
                pass

        if hod_candidate_added and not was_in_hod:
            self.sound_manager.play_candidate_alert()

        Clock.schedule_once(lambda dt: self.refresh_data_table(), 0)

    def update_indices(self, dt=None):
        self.nasdaq_last, self.nasdaq_pct = self.market_data.get_index_data(".IXIC")
        self.sp_last, self.sp_pct = self.market_data.get_index_data(".SPX")
        try:
            self.nasdaq_label.text = f"NASDAQ  {self.nasdaq_pct:+.1f}%"
            self.nasdaq_label.color = (0, 1, 0, 1) if self.nasdaq_pct > 0 else (1, 0, 0, 1)
            self.sp_label.text = f"S&P     {self.sp_pct:+.1f}%"
            self.sp_label.color = (0, 1, 0, 1) if self.sp_pct > 0 else (1, 0, 0, 1)
        except:
            pass

        now_est = datetime.datetime.now(NY_TZ)
        state, _ = self.get_market_state_and_color(now_est)

        if state == "Market Open":
            nxt = 300
        elif state in ["PreMarket", "After Hours"]:
            nxt = 900
        else:
            nxt = 1800

        Clock.unschedule(self.update_indices)
        Clock.schedule_once(self.update_indices, nxt)

    def format_volume(self, volume):
        try:
            v = int(volume)
        except:
            return str(volume)
        if v >= 1_000_000_000:
            return f"{v/1_000_000_000:.1f}B"
        if v >= 1_000_000:
            return f"{v/1_000_000:.1f}M"
        if v >= 1_000:
            return f"{v/1_000:.1f}K"
        return str(int(v))

    def build_header(self):
        header = BoxLayout(orientation="horizontal", size_hint=(1, None), height=80, padding=[15, 10, 15, 10])
        with header.canvas.before:
            Color(0.12, 0.12, 0.12, 1)
            header.bg_rect = Rectangle(size=header.size, pos=header.pos)
        header.bind(size=lambda inst, val: setattr(header.bg_rect, "size", inst.size))
        header.bind(pos=lambda inst, val: setattr(header.bg_rect, "pos", inst.pos))

        title = Label(text="SignalScan PRO", font_size=26, color=(0.25, 0.41, 0.88, 1), bold=True, size_hint=(None, 1), width=240)
        header.add_widget(title)

        times_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=160)
        self.local_time_label = Label(text="Local Time    --:--", font_size=14, color=(0.8, 0.8, 0.8, 1))
        self.nyc_time_label = Label(text="NYC Time     --:--", font_size=14, color=(0.8, 0.8, 0.8, 1))
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

        channels = ["PreGap", "HOD", "RunUp", "RunDown", "Rvsl", "Jumps", "Halts"]
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
        
        self.header_layout = BoxLayout(orientation="horizontal", size_hint=(1, None), height=35)
        with self.header_layout.canvas.before:
            Color(0.15, 0.15, 0.15, 1)
            self.header_layout.bg_rect = Rectangle(size=self.header_layout.size, pos=self.header_layout.pos)
        self.header_layout.bind(size=lambda inst, val: setattr(self.header_layout.bg_rect, "size", inst.size))
        self.header_layout.bind(pos=lambda inst, val: setattr(self.header_layout.bg_rect, "pos", inst.pos))

        self.update_header_labels()
        data_section.add_widget(self.header_layout)

        self.scroll_view = ScrollView(size_hint=(1, 1))
        self.rows_container = BoxLayout(orientation="vertical", size_hint_y=None, spacing=2)
        self.rows_container.bind(minimum_height=self.rows_container.setter('height'))
        self.scroll_view.add_widget(self.rows_container)
        data_section.add_widget(self.scroll_view)
        return data_section

    def update_header_labels(self):
        """Update column headers based on current channel"""
        self.header_layout.clear_widgets()
        
        if self.current_channel == "Halts":
            headers = ["SYMBOL", "HALT TIME", "REASON", "RESUME", "EXCH", "PRICE", "%", "NEWS"]
        else:
            headers = ["TICKER", "PRICE", "GAP%", "VOL", "FLOAT", "RVOL", "NEWS"]
        
        for h in headers:
            self.header_layout.add_widget(Label(text=h, font_size=15, color=(0.7, 0.7, 0.7, 1), bold=True))

    def refresh_data_table(self):
        self.rows_container.clear_widgets()
        stocks = self.live_data.get(self.current_channel, [])
        
        for stock_data in stocks[:50]:
            if self.current_channel == "Halts":
                row = self.create_halt_row(stock_data)
            else:
                row = self.create_stock_row(stock_data)
            if row:
                self.rows_container.add_widget(row)

    def create_halt_row(self, halt_data):
        """Create row for Halts channel with price, %, and news"""
        row = BoxLayout(orientation="horizontal", size_hint=(1, None), height=40)
        
        ticker = halt_data[0]
        
        for i, value in enumerate(halt_data[:7]):
            if i == 0:
                text_color = (1, 0.3, 0.3, 1)
            elif i == 5:
                text_color = (0.9, 0.9, 0.9, 1)
            elif i == 6:
                try:
                    pct_str = str(value).replace('%', '').replace('+', '')
                    change_pct = float(pct_str)
                    text_color = (0, 1, 0, 1) if change_pct >= 0 else (1, 0, 0, 1)
                except:
                    text_color = (0.9, 0.9, 0.9, 1)
            else:
                text_color = (0.9, 0.9, 0.9, 1)
            
            row.add_widget(Label(text=str(value), font_size=12, color=text_color))
        
        if ticker in self.stock_news:
            tier = self.stock_news[ticker].get('tier', 3)
            if tier == 2:
                btn_color = (1, 1, 0, 1); btn_text = "BREAK"; btn_text_color = (0, 0, 0, 1)
            else:
                btn_color = (0, 0.8, 1, 1); btn_text = "NEWS"; btn_text_color = (0, 0, 0, 1)
        else:
            btn_color = (1, 0.3, 0.3, 1); btn_text = "NONE"; btn_text_color = (1, 1, 1, 1)

        btn = Button(text=btn_text, font_size=11, size_hint=(1, 1),
                     background_color=btn_color, bold=True, color=btn_text_color)
        news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
        btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
        row.add_widget(btn)
        
        return row

    def create_stock_row(self, stock_data):
        row = BoxLayout(orientation="horizontal", size_hint=(1, None), height=40)
        try:
            change_str = stock_data[2].replace('%', '').replace('+', '')
            change_pct = float(change_str)
            text_color = (0, 1, 0, 1) if change_pct >= 0 else (1, 0, 0, 1)
        except:
            text_color = (0.9, 0.9, 0.9, 1)

        for i, value in enumerate(stock_data):
            if i == 6:
                ticker = stock_data[0]
                if ticker in self.stock_news:
                    tier = self.stock_news[ticker].get('tier', 3)
                    if tier == 2:
                        btn_color = (1, 1, 0, 1); btn_text = "BREAKING"; btn_text_color = (0, 0, 0, 1)
                    else:
                        btn_color = (0, 0.8, 1, 1); btn_text = "NEWS"; btn_text_color = (0, 0, 0, 1)
                else:
                    btn_color = (1, 0.3, 0.3, 1); btn_text = "NO NEWS"; btn_text_color = (1, 1, 1, 1)

                btn = Button(text=btn_text, font_size=12, size_hint=(1, 1),
                             background_color=btn_color, bold=True, color=btn_text_color)
                news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
                btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
                row.add_widget(btn)
            else:
                row.add_widget(Label(text=str(value), font_size=14, color=text_color))
        return row

    def select_channel(self, channel_name):
        self.current_channel = channel_name
        for ch, btn in self.channel_buttons.items():
            if ch == channel_name:
                btn.background_color = (0, 0.8, 0, 1)
                btn.color = (1, 1, 1, 1)
            else:
                btn.background_color = (0.25, 0.25, 0.25, 1)
                btn.color = (0.7, 0.7, 0.7, 1)
        self.update_header_labels()
        self.refresh_data_table()

    def show_news_popup(self, ticker, news_text):
        content = BoxLayout(orientation="vertical", padding=10, spacing=10)
        content.add_widget(Label(text=f"{ticker} News:", font_size=18, size_hint=(1, 0.2), bold=True))
        content.add_widget(Label(text=news_text, font_size=14, size_hint=(1, 0.6), text_size=(600, None)))
        close_btn = Button(text="Close", size_hint=(1, 0.2))
        content.add_widget(close_btn)
        popup = Popup(title=f"{ticker}", content=content, size_hint=(0.8, 0.6))
        close_btn.bind(on_release=popup.dismiss)
        popup.open()

    def update_times(self, dt):
        local_time = datetime.datetime.now()
        nyc_time = datetime.datetime.now(NY_TZ)
        
        self.local_time_label.text = f"Local Time    {local_time.strftime('%I:%M:%S %p')}"
        self.nyc_time_label.text = f"NYC Time     {nyc_time.strftime('%I:%M:%S %p')}"
        
        state, color = self.get_market_state_and_color(nyc_time)
        self.market_state_label.text = state
        self.market_state_label.color = color
        
        countdown = self.get_countdown(nyc_time)
        self.countdown_label.text = countdown
        self.countdown_label.color = color
        
        self.sound_manager.check_market_bells(nyc_time)

    def get_market_state_and_color(self, now_est):
        day_of_week = now_est.weekday()
        if day_of_week >= 5:
            return "Weekend", (1, 0.6, 0, 1)
        
        if now_est.hour < 4:
            return "Closed", (1, 0.6, 0, 1)
        elif now_est.hour == 4 or (now_est.hour < 9) or (now_est.hour == 9 and now_est.minute < 30):
            return "PreMarket", (0, 1, 1, 1)
        elif (now_est.hour == 9 and now_est.minute >= 30) or (9 < now_est.hour < 16):
            return "Market Open", (0, 1, 0, 1)
        elif now_est.hour >= 16 and now_est.hour < 20:
            return "After Hours", (1, 1, 0, 1)
        else:
            return "Closed", (1, 0.6, 0, 1)

    def get_countdown(self, now_est):
        day_of_week = now_est.weekday()
        
        if day_of_week >= 5:
            days_until_monday = (7 - day_of_week) % 7
            if days_until_monday == 0:
                days_until_monday = 1
            next_open = now_est.replace(hour=4, minute=0, second=0, microsecond=0) + datetime.timedelta(days=days_until_monday)
            delta = next_open - now_est
            hours, remainder = divmod(int(delta.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        if now_est.hour < 4:
            target = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
        elif now_est.hour < 9 or (now_est.hour == 9 and now_est.minute < 30):
            target = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        elif now_est.hour < 16:
            target = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        elif now_est.hour < 20:
            target = now_est.replace(hour=20, minute=0, second=0, microsecond=0)
        else:
            target = (now_est + datetime.timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0)
        
        delta = target - now_est
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def exit_app(self, instance):
        try:
            self.market_data.stop()
            if self.news_manager:
                self.news_manager.stop()
            self.halt_manager.stop()
        except:
            pass
        App.get_running_app().stop()


class SignalScanEnhancedApp(App):
    def build(self):
        return SignalScanApp()


if __name__ == '__main__':
    SignalScanEnhancedApp().run()
