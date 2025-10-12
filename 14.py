"""
SignalScan Enhanced - Full US Market Scanner
Version 12.0 - No Volume Filter + Auto-Enrich + News-Trigger
"""

from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.uix.scrollview import ScrollView
from kivy.uix.popup import Popup
from kivy.uix.image import Image
from kivy.graphics import Color, Rectangle
from kivy.clock import Clock
from kivy.config import Config
from kivy.core.window import Window
import pygame.mixer
import datetime
import pytz
import os
import json
import webbrowser
from urllib.parse import urlparse
from dotenv import load_dotenv
import threading
import time
import requests
import pandas as pd
import yfinance as yf
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import logging

load_dotenv()

# Setup news debug logger
news_logger = logging.getLogger('news_debug')
news_logger.setLevel(logging.DEBUG)
log_filename = f"news_debug_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
news_logger.addHandler(file_handler)
print(f"[NEWS DEBUG] Logging to: {log_filename}")

Config.set('graphics', 'fullscreen', '0')
Config.set('graphics', 'borderless', '0')
Config.set('graphics', 'resizable', '1')
Config.set('graphics', 'width', '1400')
Config.set('graphics', 'height', '900')
Config.write()

NY_TZ = pytz.timezone('America/New_York')

CACHE_DIR = "cache"
BACKUP_DIR = os.path.join(CACHE_DIR, "backups")
MASTER_TICKERS_FILE = os.path.join(CACHE_DIR, "master_tickers.json")
ENRICHED_TICKERS_FILE = os.path.join(CACHE_DIR, "enriched_tickers.json")
PRICE_CACHE_FILE = os.path.join(CACHE_DIR, "yesterday_prices.json")
PRICE_HISTORY_FILE = os.path.join(CACHE_DIR, "price_history.json")
TICKER_METADATA_FILE = os.path.join(CACHE_DIR, "ticker_metadata.json")
MAINTENANCE_LOG_FILE = os.path.join(CACHE_DIR, "maintenance_log.json")
NEWS_VAULT_FILE = os.path.join(CACHE_DIR, 'news_vault.json')

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
    print(f"[CACHE] Created cache directory: {CACHE_DIR}")

if not os.path.exists(BACKUP_DIR):
    os.makedirs(BACKUP_DIR)
    print(f"[CACHE] Created backup directory: {BACKUP_DIR}")

ticker_timestamp_registry = {}

def register_ticker_timestamp(symbol):
    if symbol not in ticker_timestamp_registry:
        ts = datetime.datetime.now()
        hour = ts.hour % 12
        if hour == 0:
            hour = 12
        ampm = "AM" if ts.hour < 12 else "PM"
        display_str = f"{hour}:{ts.strftime('%M:%S')} {ampm}"
        ticker_timestamp_registry[symbol] = {'datetime': ts, 'display': display_str}
    return ticker_timestamp_registry[symbol]

def get_timestamp_display(symbol):
    if symbol in ticker_timestamp_registry:
        return ticker_timestamp_registry[symbol]['display']
    register_ticker_timestamp(symbol)
    return ticker_timestamp_registry[symbol]['display']

def get_timestamp_color(symbol):
    if symbol not in ticker_timestamp_registry:
        return (0, 0.8, 0, 0.3)
    timestamp = ticker_timestamp_registry[symbol]['datetime']
    now = datetime.datetime.now()
    elapsed_minutes = (now - timestamp).total_seconds() / 60
    if elapsed_minutes < 30:
        return (0, 0, 1, 0.3)
    elif elapsed_minutes < 60:
        return (1, 0, 1, 0.3)
    elif elapsed_minutes < 120:
        return (0, 1, 1, 0.3)
    else:
        return (0, 0, 0, 0.3)

breaking_news_flash_registry = {}
breaking_news_sound_played = set()

def register_breaking_news(symbol):
    if symbol not in breaking_news_flash_registry:
        breaking_news_flash_registry[symbol] = True

def clear_breaking_news_flash(symbol):
    if symbol in breaking_news_flash_registry:
        del breaking_news_flash_registry[symbol]

def has_breaking_news_flash(symbol):
    return symbol in breaking_news_flash_registry

class SoundManager:
    def __init__(self):
        pygame.mixer.init(frequency=44100, size=-16, channels=2, buffer=2048)
        pygame.mixer.set_num_channels(8)
        self.sounds = {}
        self.sound_dir = "sounds"
        self.bell_played_open = False
        self.bell_played_close = False
        self.premarket_played = False
        self._load_sounds()

    def _load_sounds(self):
        sound_files = {'bell': 'nyse_bell.wav', 'news': 'iphone_news_flash.wav', 'premarket': 'woke_up_this_morning.wav', 'candidate': 'morse_code_alert.wav'}
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
        now_est = datetime.datetime.now(NY_TZ)
        return 7 <= now_est.hour < 20

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

    def play_news_alert(self):
        self.play_sound('news')

    def play_premarket_alert(self):
        self.play_sound('premarket')

    def play_candidate_alert(self):
        self.play_sound('candidate')

    def check_market_bells(self, now_est):
        market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        premarket_alert = now_est.replace(hour=7, minute=0, second=0, microsecond=0)
        
        if now_est.hour == 0 and now_est.minute == 0:
            self.bell_played_open = False
            self.bell_played_close = False
            self.premarket_played = False
        
        if not self.premarket_played and premarket_alert <= now_est < premarket_alert + datetime.timedelta(seconds=10):
            self.play_premarket_alert()
            self.premarket_played = True
        
        if not self.bell_played_open and market_open <= now_est < market_open + datetime.timedelta(seconds=10):
            self.play_bell()
            self.bell_played_open = True
        
        if not self.bell_played_close and market_close <= now_est < market_close + datetime.timedelta(seconds=10):
            self.play_bell()
            self.bell_played_close = True

class HaltManager:
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
                    
                    self.halt_data[symbol] = {'symbol': symbol, 'halt_time': halt_time, 'reason': reason, 'resume_time': resume_time, 'exchange': exchange}
                except Exception:
                    continue
            
            if self.callback:
                Clock.schedule_once(lambda dt: self.callback(self.halt_data), 0)
            
            if len(self.halt_data) > 0:
                print(f"[HALTS] {len(self.halt_data)} active halts today")
        except Exception as e:
            print(f"Halt RSS fetch error: {e}")

    def stop(self):
        self.running = False

class NewsManager:
    def __init__(self, callback, sound_manager_ref, watchlist=None, news_trigger_callback=None):
        self.seen_article_ids = set()
        self.callback = callback
        self.sound_manager_ref = sound_manager_ref
        self.news_trigger_callback = news_trigger_callback
        self.watchlist = set([x.upper() for x in (watchlist or [])])
        self.finnhub_key = os.getenv('FINNHUB_API_KEY')
        self.polygon_key = os.getenv('POLYGON_API_KEY')
        self.marketaux_key = os.getenv('MARKETAUX_API_KEY')
        self.newsapi_key = os.getenv('NEWSAPI_API_KEY')
        self.alphavantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        self.API_PRIORITY = ['polygon', 'marketaux', 'newsapi', 'alphavantage']
        self.capped_apis = set()
        self.running = False
        self.news_cache = {}
        self.news_vault = {}  # Persistent storage
        self.company_news_fetched = set()
        self.VAULT_EXPIRATION_HOURS = 72  # 72-hour rule
        self.BREAKING_NEWS_WINDOW_HOURS = 2
        self.KEYWORD_NEWS_WINDOW_HOURS = 72  # Match vault
        self.RECENT_NEWS_WINDOW_HOURS = 12
        self.load_news_vault()
        self.last_vault_cleanup = datetime.datetime.now(NY_TZ)
        self.breaking_keywords = [
            'files chapter 11', 'files chapter 7', 'files for bankruptcy', 'bankruptcy protection', 'receivership filed',
            'material cybersecurity incident', 'major data breach', 'ransomware attack',
            'notice of delisting', 'delisting determination', 'trading suspended', 'listing standards deficiency',
            'restates financials', 'accounting restatement', 'material weakness disclosed', 'non-reliance on financials',
            'ceo resigns', 'cfo resigns', 'ceo terminated', 'cfo terminated', 'ceo steps down', 'interim ceo appointed', 'ceo ousted',
            'terminates merger agreement', 'terminates acquisition agreement', 'merger terminated', 'deal terminated', 'breaks merger',
            'withdraws guidance', 'guidance withdrawn', 'suspends guidance', 'slashes outlook', 'cuts outlook',
            'covenant breach', 'loan default', 'debt default', 'missed payment',
            'auditor resigns', 'dismisses auditor', 'auditor terminated',
            'suspends dividend', 'cuts dividend', 'dividend suspended', 'eliminates dividend',
            'trading halted', 'halt pending news', 'volatility halt',
            'sec charges', 'sec investigation', 'fda rejection', 'doj investigation', 'subpoena received',
            'fda approves', 'fda approval for', 'receives fda approval', 'breakthrough therapy designation', 'fast track designation',
            'beats earnings estimates', 'crushes earnings', 'blows past earnings', 'raises full year guidance',
            'wins contract worth', 'awarded contract valued', 'secures major contract', 'receives purchase order',
            'upgrades to buy', 'raises price target', 'strong buy rating',
            'receives buyout offer', 'takeover bid at', 'acquisition offer of', 'agrees to be acquired', 'to be acquired for', 'buyout valued at', 'acquisition at premium',
            'merger agreement signed', 'definitive merger agreement', 'announces acquisition of',
            'special dividend of', 'initiates dividend', 'announces buyback program', 'authorizes buyback of',
            'strategic partnership with', 'joint venture with',
            'successful trial results', 'positive phase',
            'record revenue', 'record quarterly revenue',
            'warren buffett buys',
            'credit rating upgraded', 'rating upgrade by',
            'wins patent lawsuit', 'patent granted for',
            'debt free',
            'bitcoin surges', 'bitcoin rallies', 'bitcoin hits new high', 'bitcoin crashes',
            'expands mining operations', 'increases hash rate', 'purchases mining equipment',
            'purchases bitcoin', 'adds bitcoin to balance sheet', 'acquires bitcoin', 'buys bitcoin worth',
            'bitcoin etf approval', 'spot bitcoin etf', 'sec approves bitcoin', 'bitcoin legal tender',
            'private placement', 'private placement financing', 'announces private placement',
            'executes loi', 'signs loi', 'letter of intent', 'strategic partnership', 'crispr', 'molecule ai', 'ai breakthrough', 'clinical trial', 'orphan drug designation', 'phase 1 trial', 'research collaboration', 'technology licensing'
        ]

    def get_active_pair(self):
        available = [api for api in self.API_PRIORITY if api not in self.capped_apis]
        if len(available) < 2:
            print(f"[NEWS] All APIs exhausted, resetting...")
            self.capped_apis.clear()
            available = self.API_PRIORITY.copy()
        return available[:2]

    def mark_api_capped(self, api_name):
        self.capped_apis.add(api_name)
        print(f"[NEWS] {api_name.upper()} marked as capped. Active: {[a for a in self.API_PRIORITY if a not in self.capped_apis]}")

    def fetch_news_pair(self, symbol):
        pair = self.get_active_pair()
        print(f"[NEWS-PAIR] {symbol}: Using {pair[0].upper()} + {pair[1].upper()}")
        if pair[0] == 'polygon':
            self.fetch_polygon_news(symbol)
        elif pair[0] == 'marketaux':
            self.fetch_marketaux_news(symbol)
        elif pair[0] == 'newsapi':
            self.fetch_newsapi_news(symbol)
        elif pair[0] == 'alphavantage':
            self.fetch_alphavantage_news(symbol)
        time.sleep(0.1)
        if pair[1] == 'polygon':
            self.fetch_polygon_news(symbol)
        elif pair[1] == 'marketaux':
            self.fetch_marketaux_news(symbol)
        elif pair[1] == 'newsapi':
            self.fetch_newsapi_news(symbol)
        elif pair[1] == 'alphavantage':
            self.fetch_alphavantage_news(symbol)

    def fetch_polygon_news(self, symbol):
        news_logger.info(f"[POLYGON] Fetching for {symbol}")
        try:
            url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=10&apiKey={self.polygon_key}"
            response = requests.get(url, timeout=10)
            news_logger.info(f"[POLYGON] Response {response.status_code} for {symbol}")
            # After response = requests.get(url, timeout=10)
            if response.status_code != 200:
                news_logger.error(f"[POLYGON] Failed with code {response.status_code} for {symbol}")
                if response.status_code in [401, 402, 403, 429, 500, 502, 503, 504]:
                    news_logger.error(f"[POLYGON] Marking as capped due to error {response.status_code}")
                    self.mark_api_capped('polygon')
                return
            data = response.json()
            if data.get('status') == 'OK' and data.get('results'):
                news_logger.info(f"[POLYGON] Found {len(data['results'])} articles for {symbol}")
                for article in data['results']:
                    title = article.get('title', '')
                    published_utc = article.get('published_utc', '')
                    article_url = article.get('article_url', '')
                    try:
                        pub_datetime = datetime.datetime.strptime(published_utc, '%Y-%m-%dT%H:%M:%SZ')
                        pub_datetime = pub_datetime.replace(tzinfo=pytz.UTC).astimezone(NY_TZ)
                        age_hours = (datetime.datetime.now(NY_TZ) - pub_datetime).total_seconds() / 3600
                        if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                            news_logger.debug(f"[POLYGON] Skipping old article {age_hours:.1f}h")
                            continue
                        is_breaking = age_hours <= self.BREAKING_NEWS_WINDOW_HOURS and any(kw in title.lower() for kw in self.breaking_keywords)
                        # Check and add to persistent vault
                        if not self.add_to_vault(symbol, title, article_url, pub_datetime, 'polygon'):
                            continue                        
                        self.news_cache[symbol] = {
                            'symbol': symbol,
                            'title': title,
                            'timestamp': pub_datetime,
                            'age_hours': age_hours,
                            'age_display': self.format_age(age_hours),
                            'url': article_url,
                            'is_breaking': is_breaking,
                            'tier': 2 if is_breaking else 3
                        }
                        if is_breaking:
                            register_breaking_news(symbol)
                        if self.callback:
                            news_logger.info(f"[POLYGON] Calling callback for {symbol}")
                            self.callback(self.news_cache[symbol])
                        else:
                            news_logger.error(f"[POLYGON] NO CALLBACK for {symbol}")
                        print(f"[POLYGON] Fetched news for {symbol}")
                        break
                    except Exception as e:
                        news_logger.error(f"[POLYGON] Article processing error: {e}")
                        continue
            else:
                news_logger.warning(f"[POLYGON] No articles found for {symbol}")
        except Exception as e:
            news_logger.error(f"[POLYGON] Error for {symbol}: {e}")

    def fetch_alphavantage_news(self, symbol):
        news_logger.info(f"[ALPHAVANTAGE] Fetching for {symbol}")
        try:
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={symbol}&apikey={self.alphavantage_key}"
            response = requests.get(url, timeout=10)
            news_logger.info(f"[ALPHAVANTAGE] Response {response.status_code} for {symbol}")
            # After response = requests.get(url, timeout=10)
            if response.status_code != 200:
                news_logger.error(f"[ALPHAVANTAGE] Failed with code {response.status_code} for {symbol}")
                if response.status_code in [401, 402, 403, 429, 500, 502, 503, 504]:
                    news_logger.error(f"[ALPHAVANTAGE] Marking as capped due to error {response.status_code}")
                    self.mark_api_capped('alphavantage')
                return
            data = response.json()
            if 'feed' in data and data['feed']:
                news_logger.info(f"[ALPHAVANTAGE] Found {len(data['feed'])} articles for {symbol}")
                for article in data['feed']:
                    title = article.get('title', '')
                    time_published = article.get('time_published', '')
                    article_url = article.get('url', '')
                    try:
                        pub_datetime = datetime.datetime.strptime(time_published, '%Y%m%dT%H%M%S')
                        pub_datetime = pub_datetime.replace(tzinfo=pytz.UTC).astimezone(NY_TZ)
                        age_hours = (datetime.datetime.now(NY_TZ) - pub_datetime).total_seconds() / 3600
                        if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                            news_logger.debug(f"[ALPHAVANTAGE] Skipping old article {age_hours:.1f}h")
                            continue
                        is_breaking = age_hours <= self.BREAKING_NEWS_WINDOW_HOURS and any(kw in title.lower() for kw in self.breaking_keywords)
                        # Check and add to persistent vault
                        if not self.add_to_vault(symbol, title, article_url, pub_datetime, 'alphavantage'):
                            continue                        
                        self.news_cache[symbol] = {
                            'symbol': symbol,
                            'title': title,
                            'timestamp': pub_datetime,
                            'age_hours': age_hours,
                            'age_display': self.format_age(age_hours),
                            'url': article_url,
                            'is_breaking': is_breaking,
                            'tier': 2 if is_breaking else 3
                        }
                        if is_breaking:
                            register_breaking_news(symbol)
                        if self.callback:
                            news_logger.info(f"[ALPHAVANTAGE] Calling callback for {symbol}")
                            self.callback(self.news_cache[symbol])
                        else:
                            news_logger.error(f"[ALPHAVANTAGE] NO CALLBACK for {symbol}")
                        print(f"[ALPHAVANTAGE] Fetched news for {symbol}")
                        break
                    except Exception as e:
                        news_logger.error(f"[ALPHAVANTAGE] Article processing error: {e}")
                        continue
            else:
                news_logger.warning(f"[ALPHAVANTAGE] No articles found for {symbol}")
        except Exception as e:
            news_logger.error(f"[ALPHAVANTAGE] Error for {symbol}: {e}")

    def fetch_yfinance_news(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            yf_news = ticker.news
            if not yf_news:
                return
            for article in yf_news[:10]:
                title = article.get('title', '')
                link = article.get('link', '')
                provider = article.get('publisher', '')
                pub_time = article.get('providerPublishTime', 0)
                if not pub_time:
                    continue
                formatted_article = {
                    'headline': title,
                    'summary': title,
                    'url': link,
                    'datetime': int(pub_time),
                    'related': symbol,
                    'id': f"yf_{symbol}_{pub_time}",
                    'source': provider
                }
                self.process_news_article(formatted_article, source='yfinance')
        except Exception as e:
            print(f"[YFINANCE] Error fetching news for {symbol}: {e}")

    def fetch_marketaux_news(self, symbol):
        """Fetch news from Marketaux API"""
        news_logger.info(f"[MARKETAUX] Fetching for {symbol}")
        try:
            url = f"https://api.marketaux.com/v1/news/all?symbols={symbol}&filter_entities=true&limit=10&api_token={self.marketaux_key}"
            response = requests.get(url, timeout=10)
            news_logger.info(f"[MARKETAUX] Response {response.status_code} for {symbol}")
            # After response = requests.get(url, timeout=10)
            if response.status_code != 200:
                news_logger.error(f"[MARKETAUX] Failed with code {response.status_code} for {symbol}")
                if response.status_code in [401, 402, 403, 429, 500, 502, 503, 504]:
                    news_logger.error(f"[MARKETAUX] Marking as capped due to error {response.status_code}")
                    self.mark_api_capped('marketaux')
                return
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                news_logger.info(f"[MARKETAUX] Found {len(data['data'])} articles for {symbol}")
                for article in data['data']:
                    title = article.get('title', '')
                    published_at = article.get('published_at', '')
                    article_url = article.get('url', '')
                    try:
                        pub_datetime = datetime.datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%S.%fZ')
                        pub_datetime = pub_datetime.replace(tzinfo=pytz.UTC).astimezone(NY_TZ)
                        age_hours = (datetime.datetime.now(NY_TZ) - pub_datetime).total_seconds() / 3600
                        if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                            news_logger.debug(f"[MARKETAUX] Skipping old article {age_hours:.1f}h")
                            continue
                        is_breaking = age_hours <= self.BREAKING_NEWS_WINDOW_HOURS and any(kw in title.lower() for kw in self.breaking_keywords)
                        # Check and add to persistent vault
                        if not self.add_to_vault(symbol, title, article_url, pub_datetime, 'marketaux'):
                            continue                     
                        self.news_cache[symbol] = {
                            'symbol': symbol,
                            'title': title,
                            'timestamp': pub_datetime,
                            'age_hours': age_hours,
                            'age_display': self.format_age(age_hours),
                            'url': article_url,
                            'is_breaking': is_breaking,
                            'tier': 2 if is_breaking else 3
                        }
                        if is_breaking:
                            register_breaking_news(symbol)
                        if self.callback:
                            news_logger.info(f"[MARKETAUX] Calling callback for {symbol}")
                            self.callback(self.news_cache[symbol])
                        print(f"[MARKETAUX] Fetched news for {symbol}")
                        break
                    except Exception as e:
                        news_logger.error(f"[MARKETAUX] Article processing error: {e}")
                        continue
            else:
                news_logger.warning(f"[MARKETAUX] No articles found for {symbol}")
        except Exception as e:
            news_logger.error(f"[MARKETAUX] Error for {symbol}: {e}")

    def fetch_newsapi_news(self, symbol):
        """Fetch news from NewsAPI"""
        news_logger.info(f"[NEWSAPI] Fetching for {symbol}")
        try:
            url = f"https://newsapi.org/v2/everything?q={symbol}&sortBy=publishedAt&language=en&pageSize=10&apiKey={self.newsapi_key}"
            response = requests.get(url, timeout=10)
            news_logger.info(f"[NEWSAPI] Response {response.status_code} for {symbol}")
            # After response = requests.get(url, timeout=10)
            if response.status_code != 200:
                news_logger.error(f"[NEWSAPI] Failed with code {response.status_code} for {symbol}")
                if response.status_code in [401, 402, 403, 429, 500, 502, 503, 504]:
                    news_logger.error(f"[NEWSAPI] Marking as capped due to error {response.status_code}")
                    self.mark_api_capped('newsapi')
                return
            data = response.json()
            if data.get('status') == 'ok' and data.get('articles'):
                news_logger.info(f"[NEWSAPI] Found {len(data['articles'])} articles for {symbol}")
                for article in data['articles']:
                    title = article.get('title', '')
                    published_at = article.get('publishedAt', '')
                    article_url = article.get('url', '')
                    try:
                        pub_datetime = datetime.datetime.strptime(published_at, '%Y-%m-%dT%H:%M:%SZ')
                        pub_datetime = pub_datetime.replace(tzinfo=pytz.UTC).astimezone(NY_TZ)
                        age_hours = (datetime.datetime.now(NY_TZ) - pub_datetime).total_seconds() / 3600
                        if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                            news_logger.debug(f"[NEWSAPI] Skipping old article {age_hours:.1f}h")
                            continue
                        is_breaking = age_hours <= self.BREAKING_NEWS_WINDOW_HOURS and any(kw in title.lower() for kw in self.breaking_keywords)
                        # Check and add to persistent vault
                        if not self.add_to_vault(symbol, title, article_url, pub_datetime, 'newsapi'):
                            continue                      
                        self.news_cache[symbol] = {
                            'symbol': symbol,
                            'title': title,
                            'timestamp': pub_datetime,
                            'age_hours': age_hours,
                            'age_display': self.format_age(age_hours),
                            'url': article_url,
                            'is_breaking': is_breaking,
                            'tier': 2 if is_breaking else 3
                        }
                        if is_breaking:
                            register_breaking_news(symbol)
                        if self.callback:
                            news_logger.info(f"[NEWSAPI] Calling callback for {symbol}")
                            self.callback(self.news_cache[symbol])
                        print(f"[NEWSAPI] Fetched news for {symbol}")
                        break
                    except Exception as e:
                        news_logger.error(f"[NEWSAPI] Article processing error: {e}")
                        continue
            else:
                news_logger.warning(f"[NEWSAPI] No articles found for {symbol}")
        except Exception as e:
            news_logger.error(f"[NEWSAPI] Error for {symbol}: {e}")

    def format_age(self, age_hours):
        """Format age in hours to human-readable string"""
        if age_hours < 1:
            minutes = int(age_hours * 60)
            return f"{minutes}m ago"
        elif age_hours < 24:
            return f"{age_hours:.1f}h ago"
        elif age_hours < 168:
            days = int(age_hours / 24)
            return f"{days}d ago"
        else:
            weeks = int(age_hours / 168)
            return f"{weeks}w ago"
        
    def load_news_vault(self):
        """Load persistent news vault from disk"""
        try:
            with open(NEWS_VAULT_FILE, 'r') as f:
                self.news_vault = json.load(f)
            self.cleanup_expired_news()
            print(f"[VAULT] Loaded {len(self.news_vault)} cached articles")
        except FileNotFoundError:
            self.news_vault = {}
            print("[VAULT] No existing vault found, starting fresh")
        except Exception as e:
            print(f"[VAULT] Load error: {e}")
            self.news_vault = {}

    def save_news_vault(self):
        """Save news vault to disk"""
        try:
            with open(NEWS_VAULT_FILE, 'w') as f:
                json.dump(self.news_vault, f, indent=2, default=str)
            print(f"[VAULT] Saved {len(self.news_vault)} articles")
        except Exception as e:
            print(f"[VAULT] Save error: {e}")

    def cleanup_expired_news(self):
        """Remove news older than VAULT_EXPIRATION_HOURS"""
        now = datetime.datetime.now(NY_TZ)
        expired_keys = []
        
        for article_id, article_data in self.news_vault.items():
            try:
                # Parse timestamp
                ts_str = article_data.get('timestamp')
                if isinstance(ts_str, str):
                    article_time = datetime.datetime.fromisoformat(ts_str)
                else:
                    continue
                    
                age_hours = (now - article_time).total_seconds() / 3600
                
                if age_hours > self.VAULT_EXPIRATION_HOURS:
                    expired_keys.append(article_id)
            except Exception:
                continue
        
        for key in expired_keys:
            del self.news_vault[key]
        
        if expired_keys:
            print(f"[VAULT] Cleaned {len(expired_keys)} expired articles (>{self.VAULT_EXPIRATION_HOURS}h)")
            self.save_news_vault()

    def add_to_vault(self, symbol, title, url, timestamp, source):
        """Add news to persistent vault with deduplication"""
        # Create unique ID from URL or title+symbol
        article_id = url if url else f"{symbol}:{title[:100]}"
        
        # Check if already in vault
        if article_id in self.news_vault:
            # Update if this is a better source (priority order)
            existing_source = self.news_vault[article_id].get('source', 'unknown')
            source_priority = {'polygon': 1, 'marketaux': 2, 'newsapi': 3, 'alphavantage': 4}
            
            if source_priority.get(source, 99) < source_priority.get(existing_source, 99):
                self.news_vault[article_id]['source'] = source
                print(f"[VAULT] Updated {symbol} source to {source}")
            return False  # Already existed
        
        # Add new article
        self.news_vault[article_id] = {
            'symbol': symbol,
            'title': title,
            'url': url,
            'timestamp': timestamp.isoformat() if timestamp else None,
            'source': source,
            'added_at': datetime.datetime.now(NY_TZ).isoformat()
        }
        
        print(f"[VAULT] Added {symbol} from {source}: {title[:60]}...")
        
        # Save every 10 new articles
        if len(self.news_vault) % 10 == 0:
            self.save_news_vault()
        
        return True  # New article added

    def extract_source(self, url):
        """Extract domain name from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return "Unknown"

    def process_news_article(self, article, source='general'):
        try:
            title = article.get('headline') or article.get('title') or ''
            content = article.get('summary') or article.get('description') or ''
            related_raw = article.get('related') or article.get('symbols') or ''
            article_id = article.get('id') or article.get('url') or ''
            ts = article.get('datetime') or article.get('time') or 0
            article_url = article.get('url', '')
            if not article_id:
                article_id = f"{title[:80]}::{ts}"
            if article_id in self.seen_article_ids:
                return
            self.seen_article_ids.add(article_id)
            now = datetime.datetime.now(NY_TZ)
            if ts:
                article_time = datetime.datetime.fromtimestamp(ts, tz=NY_TZ)
            else:
                return
            age_hours = (now - article_time).total_seconds() / 3600
            if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                return
            text = (title + " " + content).lower()
            has_breaking_keywords = any(keyword in text for keyword in self.breaking_keywords)
            if has_breaking_keywords and age_hours <= self.BREAKING_NEWS_WINDOW_HOURS:
                is_breaking = True
                news_type = "BREAKING"
            elif has_breaking_keywords and age_hours <= self.KEYWORD_NEWS_WINDOW_HOURS:
                is_breaking = False
                news_type = "NEWS"
            elif age_hours <= self.RECENT_NEWS_WINDOW_HOURS:
                is_breaking = False
                news_type = "NEWS"
            else:
                return
            symbols = []
            if related_raw:
                if isinstance(related_raw, list):
                    symbols = [s.strip().upper() for s in related_raw if s]
                else:
                    symbols = [s.strip().upper() for s in str(related_raw).split(',') if s.strip()]
            matched = [s for s in symbols if s in self.watchlist]
            if not matched:
                if is_breaking and self.news_trigger_callback:
                    for sym in symbols:
                        if sym and sym.isalpha() and len(sym) <= 5:
                            self.news_trigger_callback(sym, title)
                return
            for sym in matched:
                if sym.startswith('CRYPTO') or sym.startswith('FOREX'):
                    continue
                age_display = self.format_age(age_hours)
                nd = {
                    'symbol': sym,
                    'title': title,
                    'content': content,
                    'is_breaking': is_breaking,
                    'timestamp': article_time,
                    'age_hours': age_hours,
                    'age_display': age_display,
                    'url': article_url
                }
                Clock.schedule_once(lambda dt, d=nd, s=sym: self.callback(d), 0)
                if is_breaking:
                    print(f"[BREAKING] {sym}: {title[:60]}... (Age: {age_hours:.1f}h)")
                    if sym not in breaking_news_sound_played:
                        breaking_news_sound_played.add(sym)
                        Clock.schedule_once(lambda dt, s=sym, t=title: self._check_and_play_sound(s, t), 0.2)
                else:
                    print(f"[NEWS] {sym}: {title[:60]}... (Age: {age_hours:.1f}h)")
        except Exception as e:
            print(f"Error processing article: {e}")

    def _check_and_play_sound(self, symbol, title):
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
                print(f"🔊 SOUND ALERT: {symbol} breaking news")
            else:
                print(f"🔇 {symbol} breaking news (not on active channels, sound skipped)")
        except Exception as e:
            print(f"Sound check error: {e}")

    def stop(self):
        """Stop news fetching threads"""
        self.running = False
        self.save_news_vault()  # Save before shutdown      
        print("[NEWS] NewsManager stopped, vault saved.")

    def start_news_stream(self):
        """Start continuous news monitoring"""
        try:
            news_logger.info("="*60)
            news_logger.info("start_news_stream() METHOD CALLED")
            news_logger.info(f"self.running BEFORE: {self.running}")
            self.running = True
            news_logger.info(f"self.running AFTER: {self.running}")
            news_logger.info("Creating background thread...")
            threading.Thread(target=self._news_monitor_loop, daemon=True).start()
            news_logger.info("Background thread started successfully")
            news_logger.info("="*60)
        except Exception as e:
            news_logger.error("="*60)
            news_logger.error(f"CRASH IN start_news_stream: {e}")
            import traceback
            news_logger.error(traceback.format_exc())
            news_logger.error("="*60)
            raise

    def _news_monitor_loop(self):
        """Background loop to fetch news for tickers on channels"""
        news_logger.info("NEWS MONITOR LOOP Started")
        cycle = 0
        while self.running:
            # Periodic vault cleanup (every 6 hours)
            now_time = datetime.datetime.now(NY_TZ)
            if (now_time - self.last_vault_cleanup).total_seconds() > 21600:  # 6 hours
                self.cleanup_expired_news()
                self.last_vault_cleanup = now_time
            try:
                cycle += 1
                news_logger.info(f"=== NEWS CYCLE {cycle} START ===")
                app = App.get_running_app()
                if not hasattr(app.root, 'live_data'):
                    news_logger.warning("live_data not ready yet")
                    time.sleep(10)
                    continue
                active_tickers = set()
                for channel_name, channel_stocks in app.root.live_data.items():
                    if channel_name == "Halts":
                        continue
                    for stock in channel_stocks[:10]:
                        active_tickers.add(stock[0])
                news_logger.info(f"Monitoring {len(active_tickers)} tickers: {list(active_tickers)[:20]}")
                fetched = 0
                for symbol in active_tickers:
                    if not self.running:
                        break
                    if symbol not in self.news_cache:
                        news_logger.info(f"Fetching news for {symbol}")
                        self.fetch_news_pair(symbol)
                        fetched += 1
                        time.sleep(2)
                news_logger.info(f"Cycle {cycle} complete - fetched {fetched} new | cache: {len(self.news_cache)}")
                time.sleep(300)
            except Exception as e:
                news_logger.error(f"NEWS MONITOR ERROR: {e}")
                import traceback
                news_logger.error(traceback.format_exc())
                time.sleep(60)
        news_logger.info("NEWS MONITOR LOOP Stopped")

    def process_news_article(self, article, source='general'):
            try:
                title = article.get('headline') or article.get('title') or ''
                content = article.get('summary') or article.get('description') or ''
                related_raw = article.get('related') or article.get('symbols') or ''
                article_id = article.get('id') or article.get('url') or ''
                ts = article.get('datetime') or article.get('time') or 0
                article_url = article.get('url', '')
                if not article_id:
                    article_id = f"{title[:80]}::{ts}"
                if article_id in self.seen_article_ids:
                    return
                self.seen_article_ids.add(article_id)
                now = datetime.datetime.now(NY_TZ)
                if ts:
                    article_time = datetime.datetime.fromtimestamp(ts, tz=NY_TZ)
                else:
                    return
                age_hours = (now - article_time).total_seconds() / 3600
                if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                    return
                text = (title + " " + content).lower()
                has_breaking_keywords = any(keyword in text for keyword in self.breaking_keywords)
                if has_breaking_keywords and age_hours <= self.BREAKING_NEWS_WINDOW_HOURS:
                    is_breaking = True
                    news_type = "BREAKING"
                elif has_breaking_keywords and age_hours <= self.KEYWORD_NEWS_WINDOW_HOURS:
                    is_breaking = False
                    news_type = "NEWS"
                elif age_hours <= self.RECENT_NEWS_WINDOW_HOURS:
                    is_breaking = False
                    news_type = "NEWS"
                else:
                    return
                symbols = []
                if related_raw:
                    if isinstance(related_raw, list):
                        symbols = [s.strip().upper() for s in related_raw if s]
                    else:
                        symbols = [s.strip().upper() for s in str(related_raw).split(',') if s.strip()]
                matched = [s for s in symbols if s in self.watchlist]
                if not matched:
                    if is_breaking and self.news_trigger_callback:
                        for sym in symbols:
                            if sym and sym.isalpha() and len(sym) <= 5:
                                self.news_trigger_callback(sym, title)
                    return
                for sym in matched:
                    if sym.startswith('CRYPTO') or sym.startswith('FOREX'):
                        continue
                    age_display = self.format_age(age_hours)
                    nd = {
                        'symbol': sym,
                        'title': title,
                        'content': content,
                        'is_breaking': is_breaking,
                        'timestamp': article_time,
                        'age_hours': age_hours,
                        'age_display': age_display,
                        'url': article_url
                    }
                    Clock.schedule_once(lambda dt, d=nd, s=sym: self.callback(d), 0)
                    if is_breaking:
                        print(f"[BREAKING] {sym}: {title[:60]}... (Age: {age_hours:.1f}h)")
                        if sym not in breaking_news_sound_played:
                            breaking_news_sound_played.add(sym)
                            Clock.schedule_once(lambda dt, s=sym, t=title: self._check_and_play_sound(s, t), 0.2)
                    else:
                        print(f"[NEWS] {sym}: {title[:60]}... (Age: {age_hours:.1f}h)")
            except Exception as e:
                print(f"Error processing article: {e}")

    def _check_and_play_sound(self, symbol, title):
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
                print(f"🔊 SOUND ALERT: {symbol} breaking news")
            else:
                print(f"🔇 {symbol} breaking news (not on active channels, sound skipped)")
        except Exception as e:
            print(f"Sound check error: {e}")

class EnrichmentManager:
    def __init__(self):
        self.enriched = {}
        self.max_enriched = 200
        self.load_enriched()

    def load_enriched(self):
        try:
            with open(ENRICHED_TICKERS_FILE, 'r') as f:
                self.enriched = json.load(f)
            print(f"[ENRICH] Loaded {len(self.enriched)} enriched tickers")
        except Exception as e:
            self.enriched = {}
            print(f"[ENRICH] No enriched cache found, starting fresh: {e}")

    def save_enriched(self):
        try:
            with open(ENRICHED_TICKERS_FILE, 'w') as f:
                json.dump(self.enriched, f, indent=2)
        except Exception as e:
            print(f"[ENRICH] Save error: {e}")

    def check_gates(self, symbol, price, change_pct, rvol, volume, float_shares, is_new_hod):
        now = datetime.datetime.now(NY_TZ)
        is_premarket = now.hour < 9 or (now.hour == 9 and now.minute < 30)
        gate_triggered = None
        score_bonus = 1
        
        if is_premarket and rvol >= 8.0 and abs(change_pct) >= 10.0 and volume >= 75000:
            gate_triggered = "premarket_spike"
            score_bonus = 3
        elif rvol >= 5.0 and abs(change_pct) >= 8.0:
            gate_triggered = "intraday_momentum"
            score_bonus = 2
        elif is_new_hod and rvol >= 6.0 and float_shares <= 100 and change_pct >= 5.0:
            gate_triggered = "hod_sprint"
            score_bonus = 3
        
        if gate_triggered:
            self.promote_ticker(symbol, gate_triggered, score_bonus)

    def promote_ticker(self, symbol, reason, score_bonus):
        now_str = datetime.datetime.now(NY_TZ).isoformat()
        if symbol not in self.enriched:
            self.enriched[symbol] = {
                'first_seen': now_str,
                'last_seen': now_str,
                'hits_today': 1,
                'total_hits': 1,
                'score': score_bonus,
                'last_reason': reason,
                'channels_hit': []
            }
            print(f"[ENRICH] New: {symbol} ({reason}) +{score_bonus}")
        else:
            self.enriched[symbol]['last_seen'] = now_str
            self.enriched[symbol]['hits_today'] += 1
            self.enriched[symbol]['total_hits'] += 1
            self.enriched[symbol]['score'] += score_bonus
            self.enriched[symbol]['last_reason'] = reason
            print(f"[ENRICH] Update: {symbol} score={self.enriched[symbol]['score']} hits={self.enriched[symbol]['total_hits']}")
        
        if len(self.enriched) > self.max_enriched:
            self.cull_weakest()

    def record_channel_hit(self, symbol, channel_name):
        if symbol in self.enriched:
            if channel_name not in self.enriched[symbol]['channels_hit']:
                self.enriched[symbol]['channels_hit'].append(channel_name)
            bonus = 1
            if channel_name == "HOD":
                bonus = 2
            self.enriched[symbol]['score'] += bonus
            self.enriched[symbol]['hits_today'] += 1
            self.enriched[symbol]['total_hits'] += 1
            self.enriched[symbol]['last_seen'] = datetime.datetime.now(NY_TZ).isoformat()

    def decay_scores(self):
        for symbol in list(self.enriched.keys()):
            self.enriched[symbol]['score'] *= 0.9
            if self.enriched[symbol]['score'] <= 5:
                del self.enriched[symbol]
                print(f"[ENRICH] Removed {symbol} (low score)")

    def cull_weakest(self):
        if len(self.enriched) <= self.max_enriched:
            return
        sorted_symbols = sorted(self.enriched.items(), key=lambda x: x[1]['score'])
        to_remove = len(self.enriched) - self.max_enriched
        for i in range(to_remove):
            symbol = sorted_symbols[i][0]
            del self.enriched[symbol]
            print(f"[ENRICH] Culled {symbol} (cap reached)")

    def get_enriched_list(self):
        return list(self.enriched.keys())

class MaintenanceEngine:
    def __init__(self):
        self.master_tickers = []
        self.yesterday_prices = {}
        self.price_history = {}
        self.ticker_metadata = {}
        self.maintenance_log = {}

    def load_all_caches(self):
        self.master_tickers = self._load_json(MASTER_TICKERS_FILE, [])
        self.yesterday_prices = self._load_json(PRICE_CACHE_FILE, {})
        self.price_history = self._load_json(PRICE_HISTORY_FILE, {})
        self.ticker_metadata = self._load_json(TICKER_METADATA_FILE, {})
        self.maintenance_log = self._load_json(MAINTENANCE_LOG_FILE, {})

    def save_all_caches(self):
        self._save_json(MASTER_TICKERS_FILE, self.master_tickers)
        self._save_json(PRICE_CACHE_FILE, self.yesterday_prices)
        self._save_json(PRICE_HISTORY_FILE, self.price_history)
        self._save_json(TICKER_METADATA_FILE, self.ticker_metadata)
        self._save_json(MAINTENANCE_LOG_FILE, self.maintenance_log)

    def backup_caches(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        backup_path = os.path.join(BACKUP_DIR, timestamp)
        os.makedirs(backup_path, exist_ok=True)
        for filepath in [MASTER_TICKERS_FILE, ENRICHED_TICKERS_FILE, PRICE_CACHE_FILE, PRICE_HISTORY_FILE, TICKER_METADATA_FILE]:
            if os.path.exists(filepath):
                basename = os.path.basename(filepath)
                with open(filepath, 'rb') as src:
                    with open(os.path.join(backup_path, basename), 'wb') as dst:
                        dst.write(src.read())
        print(f"[BACKUP] {message} - {timestamp}")

    def _load_json(self, path, default):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception:
            return default

    def _save_json(self, path, data):
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[ERROR] Failed to save {path}: {e}")

def download_ticker_universe(self):
    print(f"[MAINT] Downloading ticker universe...")
    try:
        nasdaq_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt'
        other_url = 'ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt'
        nasdaq_df = pd.read_csv(nasdaq_url, sep='|')
        other_df = pd.read_csv(other_url, sep='|')
        nasdaq_list = nasdaq_df[nasdaq_df['Test Issue'] == 'N']['Symbol'].tolist()
        other_list = other_df[other_df['Test Issue'] == 'N']['ACT Symbol'].tolist()
        all_tickers = set(nasdaq_list + other_list)
        all_tickers = {str(t).strip() for t in all_tickers if t and str(t).strip() and len(str(t).strip()) <= 5}
        self.master_tickers = list(all_tickers)
        self._save_json(MASTER_TICKERS_FILE, self.master_tickers)
        print(f"[MAINT] Downloaded {len(self.master_tickers)} tickers (NO VOLUME FILTER)")
    except Exception as e:
        print(f"[ERROR] Ticker download failed: {e}")

def refresh_prices(self):
    print(f"[MAINT] Refreshing yesterday prices...")
    if not self.master_tickers:
        print(f"[MAINT] No tickers to refresh")
        return
    updated = 0
    for symbol in self.master_tickers[:1000]:
        try:
            t = yf.Ticker(symbol)
            hist = t.history(period="1d", interval="1d")
            if hist is not None and not hist.empty:
                close_price = float(hist['Close'].iloc[-1])
                self.yesterday_prices[symbol] = close_price
                updated += 1
            time.sleep(0.1)
        except Exception as e:
            continue
    self._save_json(PRICE_CACHE_FILE, self.yesterday_prices)
    print(f"[MAINT] Refreshed {updated} prices")

def weekend_mega_build(self):
    print(f"[MAINT] ===== WEEKEND MEGA BUILD START =====")
    self.backup_caches("Pre-weekend-build")
    self.download_ticker_universe()
    self.refresh_prices()
    self.backup_caches("Post-weekend-build")
    print(f"[MAINT] ===== WEEKEND MEGA BUILD COMPLETE =====")

def weekday_maintenance(self):
    print(f"[MAINT] ===== WEEKDAY MAINTENANCE START =====")
    self.backup_caches("Pre-daily-maintenance")
    self.refresh_prices()
    self.backup_caches("Post-daily-maintenance")
    print(f"[MAINT] ===== WEEKDAY MAINTENANCE COMPLETE =====")

class MarketDataManager:
    def __init__(self, callback, news_manager_ref=None, enrichment_manager_ref=None):
        self.callback = callback
        self.news_manager_ref = news_manager_ref
        self.enrichment_manager_ref = enrichment_manager_ref
        self.running = False
        self.stock_data = {}
        self.price_history = {}
        self.yesterday_prices = {}
        self.all_tickers = []
        self.market_open_time = None
        self.scan_count = 0
        self.rotation_offset = 0
        self.news_trigger_queue = set()
        self.maintenance_engine = MaintenanceEngine()
        self.load_all_caches()
        self.start_maintenance_scheduler()

    def load_all_caches(self):
        self.maintenance_engine.load_all_caches()
        self.all_tickers = self.maintenance_engine.master_tickers
        self.yesterday_prices = self.maintenance_engine.yesterday_prices
        self.price_history = self.maintenance_engine.price_history
        print(f"[CACHE] Loaded {len(self.all_tickers)} master tickers")

    def save_all_caches(self):
        self.maintenance_engine.yesterday_prices = self.yesterday_prices
        self.maintenance_engine.price_history = self.price_history
        self.maintenance_engine.save_all_caches()
        if self.enrichment_manager_ref:
            self.enrichment_manager_ref.save_enriched()
        print(f"[CACHE] Saved all caches")

    def add_news_trigger(self, symbol, title):
        if symbol and symbol.upper() not in self.news_trigger_queue:
            self.news_trigger_queue.add(symbol.upper())
            print(f"[NEWS-TRIGGER] {symbol}: {title[:50]}")

    def start_maintenance_scheduler(self):
        def maintenance_loop():
            last_maintenance_day = None
            last_weekend_build_day = None
            while True:
                now_est = datetime.datetime.now(NY_TZ)
                today_str = now_est.strftime('%Y-%m-%d')
                
                if now_est.weekday() in [5, 6] and now_est.hour == 0 and now_est.minute < 5:
                    if last_weekend_build_day != today_str:
                        print(f"[SCHEDULER] Triggering weekend mega build...")
                        threading.Thread(target=self.maintenance_engine.weekend_mega_build, daemon=True).start()
                        last_weekend_build_day = today_str
                elif now_est.weekday() < 5 and now_est.hour == 0 and now_est.minute < 5:
                    if last_maintenance_day != today_str:
                        print(f"[SCHEDULER] Triggering daily maintenance...")
                        threading.Thread(target=self.maintenance_engine.weekday_maintenance, daemon=True).start()
                        last_maintenance_day = today_str
                elif now_est.hour == 16 and now_est.minute == 5:
                    self.save_all_caches()
                
                time.sleep(60)
        
        threading.Thread(target=maintenance_loop, daemon=True).start()
        print(f"[SCHEDULER] Maintenance scheduler started")

    def start_bulk_scanner(self):
        if len(self.all_tickers) == 0:
            print(f"No tickers in memory, forcing reload...")
            self.load_all_caches()
        if len(self.all_tickers) == 0:
            print(f"CRITICAL: Cache file empty or load failed!")
            return
        print(f"Starting scanner with {len(self.all_tickers)} tickers")
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
                
                scan_list = []
                
                if self.enrichment_manager_ref:
                    enriched_list = self.enrichment_manager_ref.get_enriched_list()
                    scan_list.extend(enriched_list)
                    print(f"Priority 1: {len(enriched_list)} enriched")
                
                rotation_size = 500
                if len(self.all_tickers) > 0:
                    rotation_slice = []
                    for i in range(rotation_size):
                        idx = (self.rotation_offset + i) % len(self.all_tickers)
                        rotation_slice.append(self.all_tickers[idx])
                    scan_list.extend(rotation_slice)
                    self.rotation_offset = (self.rotation_offset + rotation_size) % len(self.all_tickers)
                    print(f"Priority 2: {len(rotation_slice)} rotation")
                
                if len(self.news_trigger_queue) > 0:
                    news_list = list(self.news_trigger_queue)
                    scan_list.extend(news_list)
                    print(f"Priority 3: {len(news_list)} news-triggered")
                    self.news_trigger_queue.clear()
                
                scan_list = list(set(scan_list))
                self.batch_scan_tickers(scan_list)
                
                print(f"Scan completed in {time.time() - start_time:.1f}s")
                time.sleep(300)
            except Exception as e:
                print(f"Scan error: {e}")
                time.sleep(60)

    def batch_scan_tickers(self, ticker_list):
        try:
            processed = 0
            for symbol in ticker_list:
                try:
                    t = yf.Ticker(symbol)
                    hist = t.history(period="1d", interval="1m")
                    if hist is None or hist.empty:
                        continue
                    
                    current_price = float(hist['Close'].iloc[-1])
                    current_volume = int(hist['Volume'].sum())
                    day_high = float(hist['High'].max())
                    
                    if current_price > 25.0 or current_price <= 0 or current_volume <= 0:
                        continue
                    
                    info = {}
                    try:
                        info = t.info or {}
                    except:
                        pass
                    
                    prev_close = float(info.get('previousClose', info.get('regularMarketPreviousClose', current_price)))
                    if symbol in self.yesterday_prices and self.yesterday_prices[symbol] > 0:
                        prev_close = self.yesterday_prices[symbol]
                    
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
                    
                    if self.enrichment_manager_ref:
                        self.enrichment_manager_ref.check_gates(
                            symbol, current_price, change_pct, rvol,
                            current_volume, float_shares, is_new_hod
                        )
                    
                    if self.callback:
                        Clock.schedule_once(lambda dt, s=symbol, d=self.stock_data[symbol]: self.callback(s, d), 0)
                    
                    processed += 1
                except Exception as e:
                    continue
            
            print(f"Processed {processed} stocks")
        except Exception as e:
            print(f"Batch scan error: {e}")

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
        except Exception:
            return 0.0

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
        except Exception:
            pass
        return 0.0, 0.0

    def stop(self):
            self.running = False
            self.save_all_caches()
            print(f"[SCANNER] Stopped, caches saved")

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
        self.current_sort_column = None
        self.current_sort_ascending = True
        self.is_kiosk_mode = False
        
        self.sound_manager = SoundManager()
        self.enrichment_manager = EnrichmentManager()
        self.news_manager = None
        self.market_data = MarketDataManager(
            callback=self.on_data_update,
            news_manager_ref=None,
            enrichment_manager_ref=self.enrichment_manager
        )
        self.halt_manager = HaltManager(callback=self.on_halt_update)
        
        self.build_header()
        
        main_content = BoxLayout(orientation="vertical", spacing=0, padding=0)
        self.tabs_container = self.build_channel_tabs()
        main_content.add_widget(self.tabs_container)
        self.data_container = self.build_data_section()
        main_content.add_widget(self.data_container)
        self.add_widget(main_content)
        
        Clock.schedule_interval(self.update_times, 1)
        Clock.schedule_interval(self.refresh_data_table, 2)
        Clock.schedule_once(self.start_market_data, 2)
        Clock.schedule_once(self.start_news_feed, 10)
        Clock.schedule_once(self.start_halt_monitor, 4)
        Clock.schedule_once(self.update_indices, 5)
        Clock.schedule_interval(self.refresh_timestamp_colors, 60)
        Clock.schedule_interval(self.flash_breaking_news_tabs, 0.5)
        
        Window.bind(on_request_close=self.on_window_close)

    def _update_bg(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size

    def on_window_close(self, *args):
        try:
            self.market_data.stop()
            if self.news_manager:
                self.news_manager.stop()
            self.halt_manager.stop()
            print(f"[SCANNER] Window closed - shutting down scanner")
        except Exception as e:
            print(f"[ERROR] Error during shutdown: {e}")
        return False

    def start_market_data(self, dt=None):
        self.market_data.start_bulk_scanner()

    def start_news_feed(self, dt=None):
        if not self.market_data.all_tickers:
            print("NEWS FEED: all_tickers not loaded, retrying in 2 seconds...")
            Clock.schedule_once(self.start_news_feed, 2)
            return
        
        self.stock_news.clear()
        print(f"[NEWS] Cleared {len(self.stock_news)} old news entries")       
        
        wl = self.market_data.all_tickers or []
        if not wl:
            print("NEWS FEED: watchlist is empty after load, forcing rebuild...")
            self.market_data.maintenance_engine.weekend_mega_build()
            Clock.schedule_once(self.start_news_feed, 5)
            return

        self.news_manager = NewsManager(
            callback=self.on_news_update,
            sound_manager_ref=self.sound_manager,
            watchlist=wl,
            news_trigger_callback=self.market_data.add_news_trigger
        )
        self.market_data.news_manager_ref = self.news_manager
        self.news_manager.start_news_stream()
        print(f"NEWS FEED STARTED with {len(wl)} tickers")

    def start_halt_monitor(self, dt=None):
        self.halt_manager.start_halt_monitor()

    def on_data_update(self, symbol, data):
        self.process_stock_update(symbol, data)

    def on_news_update(self, news_data):
        print(f"[DEBUG] on_news_update: {news_data.get('symbol', 'NO SYMBOL')} - {news_data.get('title', 'NO TITLE')[:50]}")
        symbol = news_data.get('symbol', '')
        title = news_data.get('title', '')
        is_breaking = news_data.get('is_breaking', False)
        timestamp = news_data.get('timestamp')
        age_hours = news_data.get('age_hours', 0)
        age_display = news_data.get('age_display', 'Unknown')
        url = news_data.get('url', '')
        
        if symbol not in self.stock_news:
            self.stock_news[symbol] = {
                'title': title,
                'is_breaking': is_breaking,
                'timestamp': timestamp,
                'age_hours': age_hours,
                'age_display': age_display,
                'url': url,
                'tier': 2 if is_breaking else 3
            }

        else:
            self.stock_news[symbol].update({
                'title': title,
                'is_breaking': is_breaking,
                'timestamp': timestamp,
                'age_hours': age_hours,
                'age_display': age_display,
                'url': url
            })
        
        self.stock_news[symbol]['tier'] = 2 if is_breaking else 3
        if is_breaking:
            register_breaking_news(symbol)
        

    def flash_breaking_news_tabs(self, dt):
        flash_on = int(time.time() * 2) % 2 == 0
        for channel_name, btn in self.channel_buttons.items():
            has_breaking = any(has_breaking_news_flash(stock[0]) for stock in self.live_data.get(channel_name, []))
            if has_breaking and channel_name != self.current_channel:
                if flash_on:
                    btn.background_color = (0, 0.5, 1, 1)
                else:
                    btn.background_color = (0.15, 0.15, 0.4, 1)
            elif channel_name == self.current_channel:
                btn.background_color = (0, 0.8, 0, 1)
            else:
                btn.background_color = (0.25, 0.25, 0.25, 1)

    def on_halt_update(self, halt_data):
        self.live_data["Halts"] = []
        for symbol, halt_info in halt_data.items():
            register_ticker_timestamp(symbol)
            stock_data = self.market_data.stock_data.get(symbol, {})
            price = stock_data.get('price', 0)
            change_pct = stock_data.get('change_pct', 0)
            price_str = f"${price:.2f}" if price > 0 else "N/A"
            pct_str = f"{change_pct:+.1f}%" if price > 0 else "N/A"
            
            self.live_data["Halts"].append([
                symbol,
                get_timestamp_display(symbol),
                halt_info['halt_time'],
                halt_info['reason'][:20],
                halt_info['resume_time'][:10],
                halt_info['exchange'],
                price_str,
                pct_str,
                ""
            ])
        

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
            
            register_ticker_timestamp(symbol)
            
            formatted = [
                symbol,
                get_timestamp_display(symbol),
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
        
        if (is_premarket and 1.0 <= price <= 20.0 and abs(change_pct) >= 10.0 and volume >= 100000 and float_shares <= 100):
            self.live_data["PreGap"].append(stock_data)
            self.enrichment_manager.record_channel_hit(ticker, "PreGap")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        if ((is_new_hod and rvol >= 3.0 and float_shares <= 100 and change_pct >= 5.0) or
            (is_52wk_high and rvol >= 3.0 and change_pct >= 5.0)):
            self.live_data["HOD"].append(stock_data)
            hod_candidate_added = True
            self.enrichment_manager.record_channel_hit(ticker, "HOD")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        if (3.0 <= change_pct <= 15.0 and rvol >= 1.5 and volume >= 250000):
            self.live_data["RunUp"].append(stock_data)
            self.enrichment_manager.record_channel_hit(ticker, "RunUp")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        if (change_pct <= -3.0 and rvol >= 1.5 and volume >= 250000):
            self.live_data["RunDown"].append(stock_data)
            self.enrichment_manager.record_channel_hit(ticker, "RunDown")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        if (rvol >= 8.0 and abs(change_pct) >= 8.0):
            self.live_data["Rvsl"].append(stock_data)
            self.enrichment_manager.record_channel_hit(ticker, "Rvsl")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        if abs(change_pct) >= 20.0:
            self.live_data["Jumps"].append(stock_data)
            self.enrichment_manager.record_channel_hit(ticker, "Jumps")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        if self.current_sort_column is None:
            for ch in ["PreGap", "HOD", "RunUp", "RunDown", "Rvsl", "Jumps"]:
                try:
                    self.live_data[ch].sort(key=lambda x: float(str(x[6]).replace('x', '')), reverse=True)
                except:
                    pass
        else:
            self.apply_current_sort()
        
        if hod_candidate_added and not was_in_hod:
            self.sound_manager.play_candidate_alert()
        

    def update_indices(self, dt=None):
        self.nasdaq_last, self.nasdaq_pct = self.market_data.get_index_data(".IXIC")
        self.sp_last, self.sp_pct = self.market_data.get_index_data(".SPX")
        
        try:
            self.nasdaq_label.text = f"NASDAQ {self.nasdaq_pct:+.1f}%"
            self.nasdaq_label.color = (0, 1, 0, 1) if self.nasdaq_pct > 0 else (1, 0, 0, 1)
            self.sp_label.text = f"S&P {self.sp_pct:+.1f}%"
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
        
        logo = Image(source='SignalScan.jpeg', size_hint=(None, 1), width=70, allow_stretch=True, keep_ratio=True)
        header.add_widget(logo)
        
        title = Label(text="SignalScan PRO", font_size=26, color=(0.25, 0.41, 0.88, 1), bold=True, size_hint=(None, 1), width=240)
        header.add_widget(title)
        
        times_section = BoxLayout(orientation="vertical", spacing=5, size_hint=(None, 1), width=160)
        self.local_time_label = Label(text="Local Time --:--", font_size=14, color=(0.8, 0.8, 0.8, 1))
        self.nyc_time_label = Label(text="NYC Time --:--", font_size=14, color=(0.8, 0.8, 0.8, 1))
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
        self.nasdaq_label = Label(text=f"NASDAQ +0.0%", font_size=13, color=(0, 1, 0, 1), bold=True)
        self.sp_label = Label(text=f"S&P +0.0%", font_size=13, color=(0, 1, 0, 1), bold=True)
        indicators_section.add_widget(self.nasdaq_label)
        indicators_section.add_widget(self.sp_label)
        header.add_widget(indicators_section)
        
        header.add_widget(Label(text="", size_hint=(1, 1)))
        
        self.update_btn = Button(text="UPDATE", font_size=11, size_hint=(None, 1), width=65, background_color=(0.2, 0.4, 0.8, 1), color=(1, 1, 1, 1), bold=True)
        self.update_btn.bind(on_release=self.trigger_cache_update)
        header.add_widget(self.update_btn)
        
        self.kiosk_btn = Button(text="KIOSK", font_size=11, size_hint=(None, 1), width=65, background_color=(0.2, 0.6, 0.2, 1), color=(1, 1, 1, 1), bold=True)
        self.kiosk_btn.bind(on_release=self.toggle_kiosk_mode)
        header.add_widget(self.kiosk_btn)
        self.add_widget(header)

    def trigger_cache_update(self, instance):
        self.update_btn.text = "UPDATING..."
        self.update_btn.disabled = True
        threading.Thread(target=self.run_weekend_build_safe, daemon=True).start()

    def run_weekend_build_safe(self):
        try:
            print("🔧 Starting weekend_mega_build...")
            self.market_data.maintenance_engine.weekend_mega_build()
            print("✅ WEEKEND MEGA BUILD COMPLETE")
        except Exception as e:
            print(f"❌ [ERROR] weekend_mega_build failed: {e}")
            import traceback
            traceback.print_exc()
        finally:
            Clock.schedule_once(self.reset_update_button, 1)

    def reset_update_button(self, dt):
        self.update_btn.text = "UPDATE"
        self.update_btn.disabled = False
        print(f"[MANUAL] Cache update triggered")

    def toggle_kiosk_mode(self, instance):
        if not self.is_kiosk_mode:
            Window.fullscreen = 'auto'
            Window.borderless = True
            self.kiosk_btn.background_color = (0.8, 0.4, 0, 1)
            self.is_kiosk_mode = True
            print(f"[KIOSK] Entering fullscreen kiosk mode")
        else:
            Window.fullscreen = False
            Window.borderless = False
            Window.size = (1400, 900)
            self.kiosk_btn.background_color = (0.2, 0.6, 0.2, 1)
            self.is_kiosk_mode = False
            print(f"[KIOSK] Exiting to windowed mode")

    def exit_kiosk_mode(self, instance):
        if self.is_kiosk_mode:
            Window.fullscreen = False
            Window.borderless = False
            Window.size = (1400, 900)
            self.kiosk_btn.background_color = (0.2, 0.6, 0.2, 1)
            self.is_kiosk_mode = False
            print(f"[KIOSK] Exited to windowed mode (scanner still running)")
        else:
            print(f"[KIOSK] Already in windowed mode (use window X to close scanner)")

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
        self.header_layout.clear_widgets()
        if self.current_channel == "Halts":
            headers = [("SYMBOL", 0), ("TIME", 1), ("HALT TIME", 2), ("REASON", 3), ("RESUME", 4), ("EXCH", 5), ("PRICE", 6), ("%", 7), ("NEWS", 8)]
        else:
            headers = [("TICKER", 0), ("TIME", 1), ("PRICE", 2), ("GAP%", 3), ("VOL", 4), ("FLOAT", 5), ("RVOL", 6), ("NEWS", 7)]
        
        for h_text, h_index in headers:
            btn = Button(text=h_text, font_size=14, size_hint=(1, 1), background_color=(0.15, 0.15, 0.15, 1), color=(0.7, 0.7, 0.7, 1), bold=True)
            btn.bind(on_press=lambda x, col=h_index: self.sort_by_column(col))
            self.header_layout.add_widget(btn)

    def sort_by_column(self, column_index):
        if self.current_sort_column == column_index:
            self.current_sort_ascending = not self.current_sort_ascending
        else:
            self.current_sort_column = column_index
            self.current_sort_ascending = True
        
        stocks = self.live_data.get(self.current_channel, [])
        try:
            if column_index == 1:
                stocks.sort(key=lambda x: ticker_timestamp_registry.get(x[0], {}).get('datetime', datetime.datetime.min), reverse=not self.current_sort_ascending)
            else:
                stocks.sort(key=lambda x: self.parse_sort_value(x[column_index] if column_index < len(x) else "", column_index), reverse=not self.current_sort_ascending)
            self.live_data[self.current_channel] = stocks
            self.refresh_data_table()
        except Exception as e:
            print(f"Sort error: {e}")

    def parse_sort_value(self, value, column_index):
        if column_index == 4:
            val_str = str(value).replace(',', '').strip()
            try:
                if 'B' in val_str:
                    return float(val_str.replace('B', '')) * 1_000_000_000
                elif 'M' in val_str:
                    return float(val_str.replace('M', '')) * 1_000_000
                elif 'K' in val_str:
                    return float(val_str.replace('K', '')) * 1_000
                else:
                    return float(val_str)
            except:
                return 0
        
        if isinstance(value, (int, float)):
            return value
        
        val_str = str(value).replace('$', '').replace('%', '').replace(',', '').replace('M', '').replace('K', '').replace('B', '').replace('x', '').replace('+', '').replace('[LOW]', '').replace('[MED]', '').strip()
        try:
            return float(val_str)
        except:
            return 0

    def apply_current_sort(self):
        if self.current_sort_column is not None:
            stocks = self.live_data.get(self.current_channel, [])
            try:
                if self.current_sort_column == 1:
                    stocks.sort(key=lambda x: ticker_timestamp_registry.get(x[0], {}).get('datetime', datetime.datetime.min), reverse=not self.current_sort_ascending)
                else:
                    stocks.sort(key=lambda x: self.parse_sort_value(x[self.current_sort_column] if self.current_sort_column < len(x) else "", self.current_sort_column), reverse=not self.current_sort_ascending)
                self.live_data[self.current_channel] = stocks
            except Exception as e:
                print(f"Apply sort error: {e}")

    def refresh_data_table(self, dt=None):
        self.rows_container.clear_widgets()
        stocks = self.live_data.get(self.current_channel, [])
        
        for stock_data in stocks[:50]:
            if self.current_channel == "Halts":
                row = self.create_halt_row(stock_data)
            else:
                row = self.create_stock_row(stock_data)
            if row:
                self.rows_container.add_widget(row)

    def refresh_timestamp_colors(self, dt):
        self.refresh_data_table()

    def create_halt_row(self, halt_data):
        row = BoxLayout(orientation="horizontal", size_hint=(1, None), height=40)
        ticker = halt_data[0]
        bg_color = get_timestamp_color(ticker)
        
        with row.canvas.before:
            Color(bg_color[0], bg_color[1], bg_color[2], bg_color[3])
            rect = Rectangle(pos=row.pos, size=row.size)
        row.bind(pos=lambda instance, value, r=rect: setattr(r, 'pos', value))
        row.bind(size=lambda instance, value, r=rect: setattr(r, 'size', value))
        
        for i, value in enumerate(halt_data[:8]):
            if i == 0:
                text_color = (1, 0.3, 0.3, 1)
            elif i == 6:
                text_color = (0.9, 0.9, 0.9, 1)
            elif i == 7:
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
                btn_color = (0, 0, 0.5, 1); btn_text = "BREAK"; btn_text_color = (1, 1, 1, 1)
            else:
                btn_color = (1, 1, 0, 1); btn_text = "NEWS"; btn_text_color = (0, 0, 0, 1)
        else:
            btn_color = (0, 0, 0, 1); btn_text = "NONE"; btn_text_color = (1, 1, 1, 1)
        
        btn = Button(text=btn_text, font_size=11, size_hint=(1, 1), background_color=btn_color, background_normal='', bold=True, color=btn_text_color)
        news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
        btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
        row.add_widget(btn)
        
        return row

    def create_stock_row(self, stock_data):
        row = BoxLayout(orientation="horizontal", size_hint=(1, None), height=40)
        ticker = stock_data[0]
        
        if has_breaking_news_flash(ticker):
            flash_on = int(time.time() * 2) % 2 == 0
            if flash_on:
                bg_color = (0, 1, 0, 0.3)
            else:
                bg_color = (0, 0.1, 0.3, 0.3)
        else:
            bg_color = get_timestamp_color(ticker)
        
        with row.canvas.before:
            Color(bg_color[0], bg_color[1], bg_color[2], bg_color[3])
            rect = Rectangle(pos=row.pos, size=row.size)
        row.bind(pos=lambda instance, value, r=rect: setattr(r, 'pos', value))
        row.bind(size=lambda instance, value, r=rect: setattr(r, 'size', value))
        
        try:
            change_str = stock_data[3].replace('%', '').replace('+', '')
            change_pct = float(change_str)
            text_color = (0, 1, 0, 1) if change_pct >= 0 else (1, 0, 0, 1)
        except:
            text_color = (0.9, 0.9, 0.9, 1)
        
        for i, value in enumerate(stock_data):
            if i == 7:
                if ticker in self.stock_news:
                    tier = self.stock_news[ticker].get('tier', 3)
                    if tier == 2:
                        btn_color = (0, 0, 0.5, 1); btn_text = "BREAKING"; btn_text_color = (1, 1, 1, 1)
                    else:
                        btn_color = (1, 1, 0, 1); btn_text = "NEWS"; btn_text_color = (0, 0, 0, 1)
                else:
                    btn_color = (0, 0, 0, 1); btn_text = "NO NEWS"; btn_text_color = (1, 1, 1, 1)
                
                btn = Button(text=btn_text, font_size=12, size_hint=(1, 1), background_color=btn_color, background_normal='', bold=True, color=btn_text_color)
                news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
                btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
                row.add_widget(btn)
            else:
                row.add_widget(Label(text=str(value), font_size=14, color=text_color))
        
        return row

    def select_channel(self, channel_name):
        self.current_channel = channel_name
        self.current_sort_column = None
        self.current_sort_ascending = True
        
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
        clear_breaking_news_flash(ticker)
        news_data = self.stock_news.get(ticker, {})
        timestamp = news_data.get('timestamp')
        age_display = news_data.get('age_display', 'Unknown')
        age_hours = news_data.get('age_hours', 0)
        news_url = news_data.get('url', '')
        
        if timestamp:
            time_str = timestamp.strftime('%I:%M %p ET')
            timestamp_text = f"Published: {time_str} ({age_display})"
            if age_hours < 1:
                time_color = (0, 1, 0, 1)
            elif age_hours < 6:
                time_color = (1, 1, 0, 1)
            else:
                time_color = (1, 0.6, 0, 1)
        else:
            timestamp_text = "Published: Unknown"
            time_color = (0.7, 0.7, 0.7, 1)
        
        source = self.news_manager.extract_source(news_url) if self.news_manager else "Unknown"
        
        content = BoxLayout(orientation="vertical", padding=10, spacing=8)
        content.add_widget(Label(text=f"{ticker} News:", font_size=18, size_hint=(1, 0.12), bold=True))
        content.add_widget(Label(text=timestamp_text, font_size=13, size_hint=(1, 0.08), color=time_color, italic=True))
        
        if source != "Unknown":
            content.add_widget(Label(text=f"Source: {source}", font_size=12, size_hint=(1, 0.08), color=(0.7, 0.7, 0.7, 1)))
        
        content.add_widget(Label(text=news_text, font_size=14, size_hint=(1, 0.52), text_size=(600, None)))
        
        if news_url:
            article_btn = Button(text="Read Full Article →", size_hint=(1, 0.1), background_color=(0.2, 0.5, 0.9, 1), color=(1, 1, 1, 1), bold=True)
            article_btn.bind(on_release=lambda x: (webbrowser.open(news_url), True)[1])

            content.add_widget(article_btn)
        
        close_btn = Button(text="Close", size_hint=(1, 0.1))
        content.add_widget(close_btn)
        
        popup = Popup(title=f"{ticker}", content=content, size_hint=(0.8, 0.6))
        close_btn.bind(on_release=popup.dismiss)
        popup.open()

    def update_times(self, dt):
        local_time = datetime.datetime.now()
        nyc_time = datetime.datetime.now(NY_TZ)
        
        local_hour = local_time.hour % 12
        if local_hour == 0:
            local_hour = 12
        local_ampm = "AM" if local_time.hour < 12 else "PM"
        
        nyc_hour = nyc_time.hour % 12
        if nyc_hour == 0:
            nyc_hour = 12
        nyc_ampm = "AM" if nyc_time.hour < 12 else "PM"
        
        self.local_time_label.text = f"Local Time {local_hour}:{local_time.strftime('%M:%S')} {local_ampm}"
        self.nyc_time_label.text = f"NYC Time {nyc_hour}:{nyc_time.strftime('%M:%S')} {nyc_ampm}"
        
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

class SignalScanEnhancedApp(App):
    def build(self):
        return SignalScanApp()

if __name__ == '__main__':
    # Create crash logger
    crash_logger = logging.getLogger('crash_log')
    crash_logger.setLevel(logging.ERROR)
    crash_log_file = f"crash_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    crash_handler = logging.FileHandler(crash_log_file)
    crash_handler.setLevel(logging.ERROR)
    crash_formatter = logging.Formatter('%(asctime)s - CRASH - %(message)s')
    crash_handler.setFormatter(crash_formatter)
    crash_logger.addHandler(crash_handler)
    
    print(f"[CRASH LOG] Will log crashes to: {crash_log_file}")
    
    try:
        crash_logger.info("="*60)
        crash_logger.info("APP STARTING")
        crash_logger.info("="*60)
        SignalScanEnhancedApp().run()
    except Exception as e:
        crash_logger.error("="*60)
        crash_logger.error("FATAL CRASH DETECTED")
        crash_logger.error("="*60)
        crash_logger.error(f"Error Type: {type(e).__name__}")
        crash_logger.error(f"Error Message: {str(e)}")
        crash_logger.error("\nFull Traceback:")
        import traceback
        crash_logger.error(traceback.format_exc())
        crash_logger.error("="*60)
        print(f"\n❌ APP CRASHED - Check {crash_log_file} for details")
        raise
