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
import websocket
import ssl
import queue
import random
import logging
import tkinter as tk
from tkinter import Toplevel
# Silence yfinance debug spam
logging.getLogger('yfinance').setLevel(logging.WARNING)
logging.getLogger('peewee').setLevel(logging.WARNING)  # Also silence yfinance's database


load_dotenv()
ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
TRADIER_ACCESS_TOKEN = os.getenv("TRADIER_ACCESS_TOKEN")

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
# HALT DEBUG Logger
halt_logger = logging.getLogger("halt_debug")
halt_logger.setLevel(logging.DEBUG)
halt_log_filename = f"halt_debug_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
halt_filehandler = logging.FileHandler(halt_log_filename)
halt_filehandler.setLevel(logging.DEBUG)
halt_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
halt_filehandler.setFormatter(halt_formatter)
halt_logger.addHandler(halt_filehandler)
print(f"[HALT DEBUG] Logging to {halt_log_filename}")

# SCANNER DEBUG Logger
scanner_logger = logging.getLogger("scanner_debug")
scanner_logger.setLevel(logging.DEBUG)
scanner_log_filename = f"scanner_debug_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
scanner_filehandler = logging.FileHandler(scanner_log_filename)
scanner_filehandler.setLevel(logging.DEBUG)
scanner_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
scanner_filehandler.setFormatter(scanner_formatter)
scanner_logger.addHandler(scanner_filehandler)
print(f"[SCANNER DEBUG] Logging to {scanner_log_filename}")

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
# Track halt resumption alerts
halt_resumption_alerts = {}  # {symbol_halttime: {'symbol': '...', 'halt_time': '...', 'alerted': False}}

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
    if elapsed_minutes < 30:  # Blue - Less than 30 minutes
        return (0, 0, 1, 0.3)
    elif elapsed_minutes < 60:  # Magenta - 30 to 60 minutes
        return (1, 0, 1, 0.3)
    elif elapsed_minutes < 120:  # Cyan - 1 to 2 hours
        return (0, 1, 1, 0.3)
    elif elapsed_minutes < 180:  # Black - 2 to 3 hours
        return (0, 0, 0, 0.3)
    else:
        return (0.5, 0.5, 0.5, 0.3)  # Mid-grey - 3+ hours

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
        # Schedule ticker cleanup every 5 minutes

    def _load_sounds(self):
        sound_files = {'bell': 'nyse_bell.wav', 'news': 'iphone_news_flash.wav', 'premarket': 'woke_up_this_morning.wav', 'candidate': 'morse_code_alert.wav', 'halt_resume': 'halt_resume.wav'}
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

    def play_halt_resume_alert(self):
        self.play_sound('halt_resume')

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
        self.nyse_url = "https://www.nyse.com/api/trade-halts/historical/download?symbol=&reason=&haltDateFrom={date}&haltDateTo="

    def start_halt_monitor(self):
        self.running = True
        threading.Thread(target=self._halt_loop, daemon=True).start()
        print("Starting halt monitor (Nasdaq + NYSE)...")

    def _halt_loop(self):
        while self.running:
            try:
                self.fetch_halts()
                time.sleep(60)
            except Exception as e:
                print(f"Halt fetch error: {e}")
                time.sleep(60)

    def fetch_halts(self):
        """Fetch from both Nasdaq RSS and NYSE API"""
        try:
            halt_logger.info("===== HALT FETCH CYCLE STARTING =====")
            self.halt_data = {}
            
            self.fetch_nasdaq_halts()
            self._fetch_nyse_halts()

            if self.callback:
                Clock.schedule_once(lambda dt: self.callback(self.halt_data), 0)
            
            total_halts = sum(len(halt_list) for halt_list in self.halt_data.values())
            if total_halts > 0:
                halt_logger.info(f"FOUND {total_halts} active halts across {len(self.halt_data)} symbols: {list(self.halt_data.keys())}")
                print(f"[HALTS] {total_halts} active halts today ({len(self.halt_data)} symbols)")
            else:
                halt_logger.info("No active halts found")
        except Exception as e:
            halt_logger.error(f"Halt fetch error: {e}")
            print(f"Halt fetch error: {e}")

    def fetch_nasdaq_halts(self):
        """Fetch from Nasdaq RSS with enhanced HTML parsing"""
        from html.parser import HTMLParser
        
        try:
            r = requests.get(self.rss_url, timeout=10)
            r.raise_for_status()
            halt_logger.info(f"Nasdaq RSS fetch: {r.status_code}, {len(r.content)} bytes")
            
            root = ET.fromstring(r.content)
            items = root.findall('.//item')
            halt_logger.info(f"Found {len(items)} items in RSS feed")
            
            today_et = datetime.datetime.now(NY_TZ).date()
            
            for item in items:
                try:
                    title = item.find('title').text or ""
                    description = item.find('description').text or ""
                    pub_date = item.find('pubDate').text or ""
                    
                    halt_logger.debug(f"Processing item: title='{title[:50]}', desc='{description[:50]}'")
                    
                    # Skip invalid titles
                    if len(title) < 2 or title.startswith('-'):
                        halt_logger.debug(f"SKIP: invalid title")
                        continue
                    
                    # Parse publication date
                    try:
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_datetime_et = pub_datetime.astimezone(NY_TZ)
                    except Exception as e:
                        halt_logger.debug(f"SKIP: date parse error - {e}")
                        continue
                    
                    # ENHANCED HTML TABLE PARSER
                    class EnhancedTableParser(HTMLParser):
                        def __init__(self):
                            super().__init__()
                            self.in_td = False
                            self.in_b = False
                            self.cells = []
                            self.current_cell = []
                        
                        def handle_starttag(self, tag, attrs):
                            if tag == 'td':
                                self.in_td = True
                                self.current_cell = []
                            elif tag == 'b':
                                self.in_b = True
                        
                        def handle_endtag(self, tag):
                            if tag == 'td':
                                self.in_td = False
                                # Join and strip cell content
                                cell_text = ''.join(self.current_cell).strip()
                                if cell_text:  # Only add non-empty cells
                                    self.cells.append(cell_text)
                            elif tag == 'b':
                                self.in_b = False
                        
                        def handle_data(self, data):
                            if self.in_td:
                                text = data.strip()
                                if text and not text.startswith('<'):  # Filter out HTML tags
                                    self.current_cell.append(text)
                    
                    # Parse HTML table
                    parser = EnhancedTableParser()
                    parser.feed(description)
                    cells = parser.cells
                    
                    halt_logger.debug(f"Extracted {len(cells)} cells: {cells}")
                    
                    # Validate we have enough cells (expected: 7+)
                    if len(cells) >= 7:
                        symbol = title.strip()
                        halt_date = cells[1].strip()
                        halt_time = cells[2].strip()
                        resume_date = cells[3].strip()
                        resume_time = cells[4].strip()
                        reason_code = cells[5].strip()
                        
                        # Filter out any symbols that look like HTML
                        if '<' in symbol or '>' in symbol or len(symbol) > 6:
                            halt_logger.debug(f"SKIP: invalid symbol '{symbol}'")
                            continue
                        
                        # Format times
                        halt_time_display = f"{halt_date} {halt_time}"
                        
                        # Determine resumption status
                        if resume_time and resume_time.lower() not in ['', 'n/a', 'pending']:
                            resume_time_display = f"{resume_date} {resume_time}"
                        else:
                            resume_time_display = "Pending"
                        
                        # Map reason code to description
                        reason_map = {
                            'T1': 'News Pending',
                            'T2': 'News Released',
                            'T3': 'News/Resume',
                            'T5': 'Single Stock Pause',
                            'T6': 'Extraordinary Market Activity',
                            'T8': 'ETF Components',
                            'T12': 'Additional Info Requested',
                            'H4': 'Non-Compliance',
                            'H9': 'Not Current',
                            'H10': 'SEC Trading Suspension',
                            'H11': 'Regulatory Concern',
                            'LUDP': 'Volatility Pause',
                            'LUDS': 'Volatility Pause',
                            'MWC1': 'Market-Wide Circuit Breaker Level 1',
                            'MWC2': 'Market-Wide Circuit Breaker Level 2',
                            'MWC3': 'Market-Wide Circuit Breaker Level 3',
                            'IPO1': 'IPO/New Issue',
                            'M': 'Volatility Pause',
                        }
                        reason = reason_map.get(reason_code, reason_code)
                        
                        # Determine exchange
                        exchange = "NASDAQ"
                        if "NYSE" in description.upper() or "NYSE" in title.upper():
                            exchange = "NYSE"
                        elif "AMEX" in description.upper():
                            exchange = "AMEX"
                        
                        # Add to halt data
                        if symbol not in self.halt_data:
                            self.halt_data[symbol] = []
                        
                        self.halt_data[symbol].append({
                            'symbol': symbol,
                            'halt_time': halt_time_display,
                            'reason': reason,
                            'resume_time': resume_time_display,
                            'exchange': exchange
                        })
                        
                        halt_logger.info(f"✓ ADDED HALT: {symbol} at {halt_time_display} - {reason}")
                    
                    else:
                        halt_logger.debug(f"SKIP: insufficient cells ({len(cells)})")
                        continue
                    
                except Exception as e:
                    halt_logger.error(f"Item processing error: {e}")
                    continue
            
            halt_logger.info(f"Nasdaq fetch complete: {len(self.halt_data)} symbols added")
            
        except Exception as e:
            halt_logger.error(f"Nasdaq RSS error: {e}")
            print(f"Nasdaq RSS error: {e}")

    def _fetch_nyse_halts(self):
        """Fetch from NYSE API (backup source)"""
        try:
            today = datetime.datetime.now(NY_TZ).strftime('%Y-%m-%d')
            url = self.nyse_url.format(date=today)
            
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            
            lines = r.text.strip().split('\n')
            if len(lines) < 2:
                return
                
            for line in lines[1:]:
                parts = line.split(',')
                if len(parts) < 6:
                    continue
                    
                symbol = parts[1].strip().strip('"')
                halt_time = parts[2].strip().strip('"')
                resume_time = parts[3].strip().strip('"') or "Pending"
                reason = parts[4].strip().strip('"')
                
                if not symbol or len(symbol) > 5:
                    continue
                
                if symbol not in self.halt_:
                    self.halt_data[symbol] = []
                    
                record = {
                    'symbol': symbol,
                    'halt_time': halt_time,
                    'reason': reason,
                    'resume_time': resume_time,
                    'exchange': 'NYSE'
                }
                self.halt_data[symbol].append(record)
                print(f"[NYSE-HALT] {symbol} added: {halt_time}, {reason}, resume: {resume_time}")
                    
        except Exception as e:
            print(f"[NYSE API error] {e}")

    def stop(self):
        self.running = False
    
    def _fetch_nasdaq_halts(self):
        """Fetch from Nasdaq RSS"""
        try:
            r = requests.get(self.rss_url, timeout=10)
            r.raise_for_status()
            halt_logger.info(f"Nasdaq RSS fetch: {r.status_code}, {len(r.content)} bytes")
            
            root = ET.fromstring(r.content)
            items = root.findall('.//item')
            halt_logger.info(f"Found {len(items)} items in RSS feed")
            
            today_et = datetime.datetime.now(NY_TZ).date()
            
            for item in items:
                try:
                    title = item.find('title').text or ''
                    description = item.find('description').text or ''
                    pub_date = item.find('pubDate').text or ''
                    
                    halt_logger.debug(f"Processing item: title='{title[:50]}', desc='{description[:50]}'")
                    
                    if len(title) < 2 or title.startswith('-'):
                        halt_logger.debug(f"SKIP: invalid title")
                        continue
                    
                    try:
                        pub_datetime = parsedate_to_datetime(pub_date)
                        pub_datetime_et = pub_datetime.astimezone(NY_TZ)
                    except Exception as e:
                        halt_logger.debug(f"SKIP: date parse error - {e}")
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
                    
                    halt_logger.debug(f"Parsed: symbol={symbol}, halt_time={halt_time}")
                    
                    exchange = 'NASDAQ'
                    if 'NYSE' in description.upper() or 'NYSE' in title.upper():
                        exchange = 'NYSE'
                    elif 'AMEX' in description.upper():
                        exchange = 'AMEX'

                    if symbol not in self.halt_data:
                        self.halt_data[symbol] = []
                    self.halt_data[symbol].append({'symbol': symbol, 'halt_time': halt_time, 'reason': reason, 'resume_time': resume_time, 'exchange': exchange})
                    halt_logger.info(f"✓ ADDED HALT: {symbol} at {halt_time} - {reason}")
                    
                except Exception as e:
                    halt_logger.error(f"Item processing error: {e}")
                    continue
                    
            halt_logger.info(f"Nasdaq fetch complete: {len(self.halt_data)} symbols added")
        except Exception as e:
            halt_logger.error(f"Nasdaq RSS error: {e}")
            print(f"[Nasdaq RSS error] {e}")
    
    def _fetch_nyse_halts(self):
        """Fetch from NYSE API (backup source)"""
        try:
            today = datetime.datetime.now(NY_TZ).strftime('%Y-%m-%d')
            url = self.nyse_url.format(date=today)
            
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            
            lines = r.text.strip().split('\n')
            if len(lines) < 2:
                return
                
            for line in lines[1:]:
                parts = line.split(',')
                if len(parts) < 6:
                    continue
                    
                symbol = parts[1].strip().strip('"')
                halt_time = parts[2].strip().strip('"')
                resume_time = parts[3].strip().strip('"') or "Pending"
                reason = parts[4].strip().strip('"')
                
                if not symbol or len(symbol) > 5:
                    continue
                
                if symbol not in self.halt_:
                    self.halt_data[symbol] = []
                    
                record = {
                    'symbol': symbol,
                    'halt_time': halt_time,
                    'reason': reason,
                    'resume_time': resume_time,
                    'exchange': 'NYSE'
                }
                self.halt_data[symbol].append(record)
                print(f"[NYSE-HALT] {symbol} added: {halt_time}, {reason}, resume: {resume_time}")
                    
        except Exception as e:
            print(f"[NYSE API error] {e}")


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
        # PRIMARY PROVIDERS (Always called every 1 minute)
        self.primary_providers = ['alpaca', 'yfinance', 'finnhub']
        
        # SECONDARY PROVIDERS (Rotating priority every 4 minutes)
        self.secondary_providers_config = {
            'polygon': {'cycles_limit': 120, 'calls_per_day': 7200},
            'fmp': {'cycles_limit': 3, 'calls_per_day': 180},
            'marketaux': {'cycles_limit': 1, 'calls_per_day': 60},
            'newsapi': {'cycles_limit': 1, 'calls_per_day': 60},
            'alphavantage': {'cycles_limit': 0, 'calls_per_day': 25}
        }
        
        # Cycle counters for secondary providers
        self.secondary_cycle_counters = {
            'polygon': 0,
            'fmp': 0,
            'marketaux': 0,
            'newsapi': 0,
            'alphavantage': 0
        }
        
        # Capped providers set
        self.capped_providers = set()
        
        # GDELT special provider
        self.gdelt_daily_used = False
        self.gdelt_manual_uses = 0
        self.gdelt_manual_limit = 10
        
        # Last reset time
        self.last_reset_time = datetime.datetime.now(NY_TZ).replace(hour=4, minute=0, second=0, microsecond=0)
        # ================= ALPACA NEWS WEBSOCKET VARIABLES =================
        self.news_ws = None
        self.current_news_symbols = []
        # ====================================================================
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
        
        # ================= CONTINUOUS NEWS SYSTEM VARIABLES =================
        self.primary_loop_active = False
        self.secondary_loop_active = False

        # Secondary provider cycle tracking
        self.polygon_cycles = 0
        self.polygon_max_cycles = 120
        self.fmp_cycles = 0
        self.fmp_max_cycles = 3
        self.marketaux_cycles = 0
        self.marketaux_max_cycles = 1
        self.newsapi_cycles = 0
        self.newsapi_max_cycles = 1
        self.alphavantage_cycles = 0
        self.alphavantage_max_cycles = 25  # AlphaVantage limit: 25 calls/day
        self.finnhub_cycles = 0
        self.finnhub_max_cycles = 60  # Finnhub limit: 60 calls/min free tier

        # Track exhausted providers
        self.capped_providers = set()

        # GDELT manual use tracking
        self.gdelt_manual_uses = 0
        self.gdelt_manual_limit = 10

        # Last reset timestamp
        self.last_reset_time = datetime.datetime.now(NY_TZ) - datetime.timedelta(days=1)
        # =====================================================================

    def reset_4am_operations(self):
        """4:00 AM EST Reset Operations"""
        print("4AM-RESET: STARTING 4:00 AM RESET")
        
        # 1. Clear all secondary provider caps
        self.capped_providers = set()
        print("4AM-RESET: All provider caps cleared")
        
        # 2. Reset all cycle counters
        self.polygon_cycles = 0
        self.fmp_cycles = 0
        self.marketaux_cycles = 0
        self.newsapi_cycles = 0
        self.alphavantage_cycles = 0
        self.finnhub_cycles = 0
        print("4AM-RESET: All cycle counters reset to 0")
        
        # 3. Run GDELT special fetch (background thread)
        self.gdelt_daily_used = False
        self.gdelt_manual_uses = 0
        threading.Thread(target=self.gdelt_special_fetch, daemon=True).start()
        print("[4AM-RESET] GDELT special fetch started (background)")
        
        # 4. Update last reset time
        self.last_reset_time = datetime.datetime.now(NY_TZ)
        print("[4AM-RESET] ===== 4:00 AM RESET COMPLETE =====")

    def gdelt_special_fetch(self):
        """GDELT special daily run - fetches news for all active tickers including Halts"""
        print("[GDELT-SPECIAL] Starting daily GDELT fetch...")
        try:
            app = App.get_running_app()
            if not hasattr(app.root, 'live_data'):
                print("[GDELT-SPECIAL] live_data not ready")
                return
            
            # Collect ALL active tickers including Halts channel
            active_tickers = set()
            for channel_name, channel_stocks in app.root.live_data.items():
                for stock in channel_stocks:
                    active_tickers.add(stock[0])
            
            print(f"[GDELT-SPECIAL] Fetching for {len(active_tickers)} tickers (including Halts)...")
            
            for i, symbol in enumerate(active_tickers, 1):
                if not self.running:
                    break
                
                self.fetch_gdelt_news(symbol)
                
                # Log progress every 10 tickers
                if i % 10 == 0 or i == len(active_tickers):
                    print(f"[GDELT-SPECIAL] {i}/{len(active_tickers)}: {symbol}")
                
                time.sleep(5)  # 1 per 5 seconds rate limit
            
            self.gdelt_daily_used = True
            print(f"[GDELT-SPECIAL] Complete - fetched {len(active_tickers)} tickers")
        except Exception as e:
            print(f"[GDELT-SPECIAL] Error: {e}")

    def fetch_gdelt_news(self, symbol):
        """Fetch news from GDELT"""
        print(f"[GDELT] Fetching for {symbol}")
        # Placeholder - implement GDELT API call here
        pass

    def get_active_secondary(self):
        """Get the highest priority secondary provider that hasn't exhausted cycles"""
        priority_order = ['polygon', 'fmp', 'marketaux', 'newsapi', 'alphavantage']
        
        for provider in priority_order:
            if provider in self.capped_providers:
                continue
            
            config = self.secondary_providers_config[provider]
            current_cycles = self.secondary_cycle_counters[provider]
            
            if current_cycles < config['cycles_limit']:
                return provider
        
        # All exhausted - degraded mode
        print("[SECONDARY] All secondary providers exhausted - degraded mode")
        return None

    def increment_secondary_cycle(self, provider):
        """Increment cycle counter and cap if limit reached"""
        if provider not in self.secondary_cycle_counters:
            return
        
        self.secondary_cycle_counters[provider] += 1
        config = self.secondary_providers_config[provider]
        
        if self.secondary_cycle_counters[provider] >= config['cycles_limit']:
            self.capped_providers.add(provider)
            print(f"[SECONDARY] {provider.upper()} exhausted ({config['cycles_limit']} cycles)")

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

    def fetch_fmp_news(self, symbol):
        """Fetch news from Financial Modeling Prep (FMP)"""
        try:
            fmp_key = os.getenv('FMP_API_KEY')
            if not fmp_key:
                print("[FMP] API key not found")
                return
            
            url = f"https://financialmodelingprep.com/api/v3/stock_news?tickers={symbol}&limit=10&apikey={fmp_key}"
            response = requests.get(url, timeout=10)
            
            if response.status_code != 200:
                news_logger.error(f"[FMP] Failed with code {response.status_code}")
                return
            
            data = response.json()
            if data:
                for article in data[:10]:
                    title = article.get('title', '')
                    published_at = article.get('publishedDate', '')
                    article_url = article.get('url', '')
                    
                    try:
                        pub_datetime = datetime.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                        pub_datetime = pub_datetime.astimezone(NY_TZ)
                        
                        formatted_article = {
                            'headline': title,
                            'summary': article.get('text', title),
                            'url': article_url,
                            'datetime': int(pub_datetime.timestamp()),
                            'related': symbol,
                            'id': f"fmp_{symbol}_{int(pub_datetime.timestamp())}",
                            'source': 'fmp'
                        }
                        
                        self.process_news_article(formatted_article, source='fmp')
                        
                    except Exception as e:
                        continue
                        
        except Exception as e:
            print(f"[FMP] Error fetching news for {symbol}: {e}")

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

    def fetch_alpaca_news(self, symbol):
        """Fetch news from Alpaca (Benzinga)"""
        try:
            url = f"https://data.alpaca.markets/v1beta1/news?symbols={symbol}&limit=10"
            headers = {
                "APCA-API-KEY-ID": ALPACA_API_KEY,
                "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                news_logger.error(f"[ALPACA] Failed with code {response.status_code}")
                return
            
            data = response.json()
            if data.get('news'):
                for article in data['news'][:10]:
                    title = article.get('headline', '')
                    published_at = article.get('created_at', '')
                    article_url = article.get('url', '')
                    
                    try:
                        pub_datetime = datetime.datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                        pub_datetime = pub_datetime.astimezone(NY_TZ)
                        
                        formatted_article = {
                            'headline': title,
                            'summary': title,
                            'url': article_url,
                            'datetime': int(pub_datetime.timestamp()),
                            'related': symbol,
                            'id': f"alpaca_{symbol}_{int(pub_datetime.timestamp())}",
                            'source': 'alpaca'
                        }
                        
                        self.process_news_article(formatted_article, source='alpaca')
                        
                    except Exception as e:
                        continue
                        
        except Exception as e:
            print(f"[ALPACA] Error fetching news for {symbol}: {e}")
    
    def start_alpaca_news_websocket(self):
        """
        Alpaca News WebSocket - Real-time news stream
        - Subscribes to all active tickers
        - Instant breaking news detection
        - No polling required
        """
        print("[NEWS-WS] Alpaca News WebSocket starting...")
        news_logger.info("NEWS-WS: Alpaca News WebSocket starting...")

        def on_open(ws):
            print("[NEWS-WS] Alpaca News WebSocket connected, authenticating...")
            news_logger.info("NEWS-WS: Alpaca News WebSocket connected, authenticating...")
            auth_msg = {
                "action": "auth",
                "key": ALPACA_API_KEY,
                "secret": ALPACA_SECRET_KEY
            }
            ws.send(json.dumps(auth_msg))
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                if isinstance(data, list):
                    for msg in data:
                        msg_type = msg.get("T")
                        
                        # Authentication successful
                        if msg_type == "success" and msg.get("msg") == "authenticated":
                            print("[NEWS-WS] ✓ Authenticated with Alpaca News")
                            news_logger.info("NEWS-WS: Authenticated with Alpaca News")

                            # Subscribe to news
                            if self.current_news_symbols:
                                self._subscribe_news(ws, self.current_news_symbols)
                        
                        # News article received
                        elif msg_type == "n":
                            self._process_news_message(msg)
                
            except Exception as e:
                print(f"[NEWS-WS] Message processing error: {e}")
                news_logger.error(f"NEWS-WS: Message processing error: {e}")
        
        def on_error(ws, error):
            print(f"[NEWS-WS] WebSocket error: {error}")
            news_logger.error(f"NEWS-WS: WebSocket error: {error}")
        
        def on_close(ws, *args):
            print("[NEWS-WS] Alpaca News WebSocket closed")
        
        while self.running:
            try:
                # Get active tickers from app
                app = App.get_running_app()
                if not hasattr(app.root, 'livedata'):
                    time.sleep(10)
                    continue
                
                active_tickers = set()
                for channel_name, channel_stocks in app.root.livedata.items():
                    for stock in channel_stocks:
                        active_tickers.add(stock[0])
                
                symbols = list(active_tickers)[:500]  # Max 500
                
                # Only reconnect if symbol list changed significantly
                if set(symbols) != set(self.current_news_symbols):
                    print(f"[NEWS-WS] Symbol list changed, reconnecting with {len(symbols)} tickers")
                    news_logger.info(f"NEWS-WS: Symbol list changed, reconnecting with {len(symbols)} tickers") 
                    self.current_news_symbols = symbols
                    
                    # Close existing WebSocket
                    if self.news_ws:
                        self.news_ws.close()
                        time.sleep(1)
                    
                    # Create new WebSocket connection
                    ws_url = "wss://stream.data.alpaca.markets/v1beta1/news"
                    self.news_ws = websocket.WebSocketApp(
                        ws_url,
                        on_open=on_open,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close
                    )
                    
                    # Run WebSocket in separate thread
                    ws_thread = threading.Thread(
                        target=self.news_ws.run_forever,
                        kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
                        daemon=True
                    )
                    ws_thread.start()
                    print(f"[NEWS-WS] ✓ Streaming news for {len(symbols)} symbols")
                    news_logger.info(f"NEWS-WS: Streaming news for {len(symbols)} symbols")
                
                # Check for symbol updates every 30 seconds
                time.sleep(30)
                
            except Exception as e:
                print(f"[NEWS-WS] ✗ Error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(30)
    
    def _subscribe_news(self, ws, symbols):
        """Helper method to subscribe to Alpaca news"""
        try:
            subscribe_msg = {
                "action": "subscribe",
                "news": symbols
            }
            ws.send(json.dumps(subscribe_msg))
            print(f"[NEWS-WS] Subscribed to news for {len(symbols)} symbols")
            news_logger.info(f"NEWS-WS: Subscribed to {len(self.current_news_symbols)} symbols")
        except Exception as e:
            print(f"[NEWS-WS] Subscribe error: {e}")
            news_logger.error(f"NEWS-WS: Subscribe error: {e}")
    
    def _process_news_message(self, msg):
        """Process incoming news message from Alpaca WebSocket"""
        try:
            # Extract news data
            symbols = msg.get("symbols", [])
            headline = msg.get("headline", "")
            summary = msg.get("summary", "")
            created_at = msg.get("created_at", "")
            url = msg.get("url", "")
            
            if not headline or not symbols:
                return
            
            # Parse timestamp
            pub_datetime = datetime.datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            pub_datetime = pub_datetime.astimezone(NY_TZ)
            
            age_hours = (datetime.datetime.now(NY_TZ) - pub_datetime).total_seconds() / 3600
            
            # Filter by age
            if age_hours > self.KEYWORD_NEWS_WINDOW_HOURS:
                return
            
            # Check if breaking news
            text = (headline + " " + summary).lower()
            is_breaking = (age_hours <= self.BREAKING_NEWS_WINDOW_HOURS and 
                          any(kw in text for kw in self.breaking_keywords))
            
            # Process for each symbol
            for symbol in symbols:
                if symbol.startswith("CRYPTO:") or symbol.startswith("FOREX:"):
                    continue
                
                # Add to vault
                if not self.add_to_vault(symbol, headline, url, pub_datetime, "alpaca_ws"):
                    continue
                
                # Store in cache
                self.news_cache[symbol] = {
                    "symbol": symbol,
                    "title": headline,
                    "content": summary,
                    "timestamp": pub_datetime,
                    "agehours": age_hours,
                    "agedisplay": self.format_age(age_hours),
                    "url": url,
                    "isbreaking": is_breaking,
                    "tier": 2 if is_breaking else 3
                }
                
                # Trigger callback
                if self.callback:
                    Clock.schedule_once(lambda dt, s=symbol: self.callback(self.news_cache[s]), 0)
                
                # Breaking news alerts
                if is_breaking:
                    print(f"[NEWS-WS] 🚨 BREAKING: {symbol} - {headline[:60]}... (Age: {age_hours:.1f}h)")
                    if symbol not in breaking_news_sound_played:
                        breaking_news_sound_played.add(symbol)
                        Clock.schedule_once(lambda dt, s=symbol, t=headline: self.check_and_play_sound(s, t), 0.2)
                else:
                    print(f"[NEWS-WS] 📰 NEWS: {symbol} - {headline[:60]}... (Age: {age_hours:.1f}h)")
                    
        except Exception as e:
            print(f"[NEWS-WS] Process message error: {e}")


    def fetch_finnhub_news(self, symbol):
        """Fetch news from Finnhub"""
        try:
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={today}&to={today}&token={self.finnhub_key}"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code != 200:
                news_logger.error(f"[FINNHUB] Failed with code {response.status_code}")
                return
            
            data = response.json()
            if data:
                for article in data[:10]:
                    title = article.get('headline', '')
                    published_at = article.get('datetime', 0)
                    article_url = article.get('url', '')
                    
                    try:
                        pub_datetime = datetime.datetime.fromtimestamp(published_at, tz=NY_TZ)
                        
                        formatted_article = {
                            'headline': title,
                            'summary': title,
                            'url': article_url,
                            'datetime': published_at,
                            'related': symbol,
                            'id': f"finnhub_{symbol}_{published_at}",
                            'source': 'finnhub'
                        }
                        
                        self.process_news_article(formatted_article, source='finnhub')
                        
                    except Exception as e:
                        continue
                        
        except Exception as e:
            print(f"[FINNHUB] Error fetching news for {symbol}: {e}")

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
        """Start continuous news system: Alpaca WebSocket (primary), secondary rotation, reset monitor"""
        try:
            self.running = True
        
            threading.Thread(target=self.start_alpaca_news_websocket, daemon=True).start()
            print("PRIMARY: Alpaca News WebSocket started (24/7 real-time stream)")
            news_logger.info("PRIMARY: Alpaca News WebSocket started (24/7 real-time stream)")
        
            threading.Thread(target=self.secondary_update_loop, daemon=True).start()
            print("SECONDARY: Provider rotation started (1 hour cycle)")
            news_logger.info("SECONDARY: Provider rotation started (1 hour cycle)")

            threading.Thread(target=self.monitor_4am_reset, daemon=True).start()
            print("4 AM reset monitor started")
            news_logger.info("4 AM reset monitor started")
        except Exception as e:
            print(f"Error in start_news_stream: {e}")
            news_logger.error(f"Error in start_news_stream: {e}")
            import traceback
            traceback.print_exc()

        except Exception as e:
            print(f"Error in start_news_stream: {e}")
            import traceback
            traceback.print_exc()

    def _news_monitor_loop(self):
        """Background loop to fetch news for tickers on channels"""
        news_logger.info("NEWS MONITOR LOOP Started")
        cycle = 0
        while self.running:
            # Check for 4 AM reset
            now_est = datetime.datetime.now(NY_TZ)
            reset_time = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
            
            if now_est >= reset_time and now_est < reset_time + datetime.timedelta(minutes=1):
                if (now_est - self.last_reset_time).total_seconds() > 3600:
                    self.reset_4am_operations()
                    
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

    #def is_continuous_time(self):
        """Return True if between 5 AM and 12 PM EST"""
        now_est = datetime.datetime.now(NY_TZ)
        return 5 <= now_est.hour < 12

    def monitor_4am_reset(self):
        """Check every 30 seconds for 4:00 AM reset trigger"""
        while self.running:
            try:
                now_est = datetime.datetime.now(NY_TZ)
                reset_time = now_est.replace(hour=4, minute=0, second=0, microsecond=0)
                # If it's 4:00 AM (or within that first minute) and not yet reset today
                if now_est >= reset_time and (now_est - reset_time).total_seconds() < 60:
                    if (now_est - self.last_reset_time).total_seconds() > 82800:
                        self.reset_4am_operations()
                time.sleep(30)
            except Exception as e:
                print(f"4AM monitor error: {e}")
                time.sleep(60)

    def gdelt_special_background_thread(self):
        """4 AM special run: pulls GDELT for all active tickers (excludes Halts)"""
        try:
            app = App.get_running_app()
            if not hasattr(app.root, 'live_data'):
                print("GDELT-SPECIAL: live_data not ready, skipping")
                return

            active = set()
            for ch_name, ch_stocks in app.root.live_data.items():
                if ch_name == "Halts":
                    continue
                for s in ch_stocks:
                    active.add(s[0])

            tick_list = list(active)
            print(f"GDELT-SPECIAL: {len(tick_list)} tickers – 1 per 5s")
            for i, sym in enumerate(tick_list):
                if not self.running:
                    break
                self.fetch_gdelt_news(sym)
                if (i + 1) % 10 == 0:
                    print(f"GDELT progress {i+1}/{len(tick_list)}")
                time.sleep(5)

            print("GDELT-SPECIAL COMPLETE")

        except Exception as e:
            print(f"GDELT ERROR: {e}")

    def secondary_update_loop(self):
        """Every 3600s (1 hour) – rotates Polygon → FMP → Marketaux → NewsAPI → AlphaVantage → Finnhub"""
        print("SECONDARY LOOP STARTED")
        cycle = 0
        while self.running:
            try:
                if not self.is_continuous_time():
                    time.sleep(60)
                    continue
                cycle += 1
                print(f"SECONDARY CYCLE {cycle}")
                app = App.get_running_app()
                if not hasattr(app.root, 'live_data'):
                    time.sleep(3600)
                    continue
                
                # Collect active tickers (exclude Halts)
                active = set()
                for ch_name, ch_stocks in app.root.live_data.items():
                    if ch_name == "Halts":
                        continue
                    for s in ch_stocks:
                        active.add(s[0])
                tickers = list(active)
                
                if not tickers:
                    print("SECONDARY: No active tickers to fetch")
                    time.sleep(3600)
                    continue
                
                used = None
                
                # Priority 1: Polygon
                if self.polygon_cycles < self.polygon_max_cycles:
                    used = "Polygon"
                    for sym in tickers:
                        self.fetch_polygon_news(sym)
                        time.sleep(0.3)
                    self.polygon_cycles += 1
                    if self.polygon_cycles >= self.polygon_max_cycles:
                        self.capped_providers.add("Polygon")
                
                # Priority 2: FMP
                elif self.fmp_cycles < self.fmp_max_cycles:
                    used = "FMP"
                    for sym in tickers:
                        self.fetch_fmp_news(sym)
                        time.sleep(0.3)
                    self.fmp_cycles += 1
                    if self.fmp_cycles >= self.fmp_max_cycles:
                        self.capped_providers.add("FMP")
                
                # Priority 3: Marketaux
                elif self.marketaux_cycles < self.marketaux_max_cycles:
                    used = "Marketaux"
                    for sym in tickers:
                        self.fetch_marketaux_news(sym)
                        time.sleep(0.3)
                    self.marketaux_cycles += 1
                    if self.marketaux_cycles >= self.marketaux_max_cycles:
                        self.capped_providers.add("Marketaux")
                
                # Priority 4: NewsAPI
                elif self.newsapi_cycles < self.newsapi_max_cycles:
                    used = "NewsAPI"
                    for sym in tickers:
                        self.fetch_newsapi_news(sym)
                        time.sleep(0.3)
                    self.newsapi_cycles += 1
                    if self.newsapi_cycles >= self.newsapi_max_cycles:
                        self.capped_providers.add("NewsAPI")
                
                # Priority 5: AlphaVantage
                elif self.alphavantage_cycles < self.alphavantage_max_cycles:
                    used = "AlphaVantage"
                    for sym in tickers:
                        self.fetch_alphavantage_news(sym)
                        time.sleep(0.3)
                    self.alphavantage_cycles += 1
                    if self.alphavantage_cycles >= self.alphavantage_max_cycles:
                        self.capped_providers.add("AlphaVantage")
                
                # Priority 6: Finnhub
                elif self.finnhub_cycles < self.finnhub_max_cycles:
                    used = "Finnhub"
                    for sym in tickers:
                        self.fetch_finnhub_news(sym)
                        time.sleep(0.3)
                    self.finnhub_cycles += 1
                    if self.finnhub_cycles >= self.finnhub_max_cycles:
                        self.capped_providers.add("Finnhub")
                
                # All providers exhausted
                else:
                    print("DEGRADED MODE – all secondary providers exhausted")
                    time.sleep(3600)
                    continue
                
                print(f"SECONDARY: completed {used if used else 'none'} cycle for {len(tickers)} tickers")
                time.sleep(3600)  # Wait 1 hour before next cycle
            except Exception as e:
                print(f"SECONDARY ERROR: {e}")
                time.sleep(3600)

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
        self.price_snapshots = {}
        self.candidate_alerted = set()
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

def yfinance_bulk_download(universe):
    """
    Bulk downloads ticker data using threading.
    Returns a list of filtered candidates.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import yfinance as yf

    def _download_ticker(ticker):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            hist = stock.history(period='2d', interval='1m', prepost=True)
            if hist.empty:
                return None

            prev_close = hist['Close'].iloc[-2] if len(hist) >= 2 else None
            current_price = hist['Close'].iloc[-1]
            if not prev_close or prev_close == 0:
                return None

            gap_pct = ((current_price - prev_close) / prev_close) * 100
            return {
                'symbol': ticker,
                'prev_close': prev_close,
                'current_price': current_price,
                'gap_percent': gap_pct,
                'float': info.get('floatShares', 0),
                'volume': hist['Volume'].iloc[-1],
                'avg_volume': info.get('averageVolume', 0),
                '52w_high': info.get('fiftyTwoWeekHigh', 0),
                '52w_low': info.get('fiftyTwoWeekLow', 0),
                'premarket_data': hist
            }
        except Exception as e:
            return None

    candidates = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_download_ticker, ticker): ticker for ticker in universe}
        for future in as_completed(futures):
            result = future.result()
            if result:
                candidates.append(result)

    # Filter criteria: gap >= 5%, float <= 100M, price <= $15
    filtered = [c for c in candidates if abs(c['gap_percent']) >= 5.0 and c['float'] <= 100_000_000 and c['current_price'] <= 15.0]
    return filtered

import threading

def start_alpaca_websocket_validate(candidates, on_validate):
    """
    Launch Alpaca WebSocket, authenticate, subscribe to candidate tickers, and validate price (2% variance).
    candidates: list of dicts returned by yfinance_bulk_download (must include 'symbol', 'current_price')
    on_validate: callback(symbol, alpaca_price, variance_pct)
    """
    import json
    import ssl
    import websocket

    ws_url = "wss://stream.data.alpaca.markets/v2/iex"

    symbols = [c['symbol'] for c in candidates]

    def on_open(ws):
        print("[ALPACA] WebSocket connected, sending auth.")
        auth_msg = {
            "action": "auth",
            "key": ALPACA_API_KEY,
            "secret": ALPACA_SECRET_KEY
        }
        ws.send(json.dumps(auth_msg))

def on_message(ws, message):
    data = json.loads(message)

    if isinstance(data, list):
        for msg in data:
            if msg.get('T') == 'q':
                symbol = msg.get('S')
                ask_price = msg.get('ap')
                # Find original candidate price
                orig = next((c for c in candidates if c['symbol'] == symbol), None)
                if orig and ask_price and orig['current_price']:
                    yf_price = orig['current_price']
                    variance = abs((ask_price - yf_price) / yf_price) * 100
                    on_validate(symbol, ask_price, variance)

    def on_error(ws, error):
        print(f"[ALPACA] Error: {error}")

    def on_close(ws, *args):
        print("[ALPACA] WebSocket closed.")

    def run_ws():
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    # Inner subscribe function (call after open)
    def subscribe_quotes(ws):
        subscribe_msg = {
            "action": "subscribe",
            "quotes": symbols
        }
        ws.send(json.dumps(subscribe_msg))

    # Start it
    print(f"[ALPACA] Starting WebSocket for {len(symbols)} symbols.")
    t = threading.Thread(target=run_ws, daemon=True)
    t.start()

def start_tradier_websocket(symbols, session_id, on_update):
    """
    Starts Tradier WebSocket for real-time tickers streaming.
    symbols: list of tickers (max 375)
    session_id: session token from Tradier REST
    on_update: callback(symbol, data_dict)
    """
    import json
    import ssl
    import websocket

    ws_url = "wss://ws.tradier.com/v1/markets/events"

    def on_open(ws):
        print(f"[TRADIER] WebSocket connected, subscribing to {len(symbols)} symbols")
        scanner_logger.info(f"[TRADIER] WebSocket connected, subscribing to {len(symbols)} symbols")
        # Subscribe to symbols
        subscribe_msg = {
            "symbols": symbols[:375],
            "sessionid": session_id,
            "linebreak": True
        }
        ws.send(json.dumps(subscribe_msg))
        scanner_logger.info(f"[TRADIER] Subscription sent for {len(symbols[:375])} symbols")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            symbol = data.get('symbol')
            if symbol:
                on_update(symbol, data)
        except Exception as e:
            print(f"[TRADIER] On message error: {e}")
            scanner_logger.error(f"[TRADIER] On message error: {e}")

    def on_error(ws, error):
        print(f"[TRADIER] WebSocket error: {error}")
        scanner_logger.error(f"[TRADIER] WebSocket error: {error}")

    def on_close(ws, *args):
        print("[TRADIER] WebSocket closed")
        scanner_logger.warning("[TRADIER] WebSocket closed")

    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    t = threading.Thread(target=ws.run_forever, kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}}, daemon=True)
    t.start()

def get_tradier_session_id():
    """
    Fetch Tradier WebSocket session ID via REST.
    """
    import requests
    print("[TRADIER] Requesting WebSocket session ID...")
    scanner_logger.info("[TRADIER] Requesting WebSocket session ID...")
    url = "https://api.tradier.com/v1/markets/events/session"
    headers = {
        "Authorization": f"Bearer {TRADIER_ACCESS_TOKEN}",
        "Accept": "application/json"
    }
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        session_id = data["stream"]["sessionid"]
        print(f"[TRADIER] Session ID obtained: {session_id[:10]}...")
        scanner_logger.info(f"[TRADIER] Session ID obtained: {session_id[:10]}...")
        return session_id
    else:
        print(f"[TRADIER] Session request failed: {response.status_code}")
        scanner_logger.error(f"[TRADIER] Session request failed: {response.status_code}")
        return None

def tradier_manual_update(symbols):
    """
    Manual bulk quote update for current tickers (user-triggered).
    Returns dict of {symbol: data_dict}.
    """
    import requests
    import datetime
    url = "https://api.tradier.com/v1/markets/quotes"
    headers = {
        "Authorization": f"Bearer {TRADIER_ACCESS_TOKEN}",
        "Accept": "application/json"
    }
    params = {
        "symbols": ",".join(symbols),
        "greeks": "false"
    }
    response = requests.get(url, headers=headers, params=params)
    updated = {}
    if response.status_code == 200:
        data = response.json()
        quotes = data.get("quotes", {}).get("quote", [])
        if isinstance(quotes, dict):
            quotes = [quotes]
        for quote in quotes:
            symbol = quote.get("symbol")
            updated[symbol] = {
                "last": quote.get("last"),
                "open": quote.get("open"),
                "high": quote.get("high"),
                "low": quote.get("low"),
                "volume": quote.get("volume"),
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "timestamp": datetime.datetime.now()
            }
    else:
        print(f"[TRADIER] REST update failed: {response.status_code}")
    return updated

class MarketDataManager:
    def __init__(self, callback, news_manager_ref=None, enrichment_manager_ref=None):
        self.callback = callback
        self.news_manager_ref = news_manager_ref
        self.enrichment_manager_ref = enrichment_manager_ref
        self.running = False
        # Three-Tier Architecture Queues
        self.tier1_shortlist_queue = queue.Queue()
        self.tier2_validated_queue = queue.Queue()
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
        # Launch Three-Tier Architecture
        threading.Thread(target=self._tier1_yfinance_bulk_prefilter, daemon=True).start()
        threading.Thread(target=self._tier2_alpaca_websocket_manager, daemon=True).start()
        threading.Thread(target=self._tier3_tradier_websocket_manager, daemon=True).start()
        print("[SCANNER] Three-Tier Architecture launched: Tier1 (yfinance) -> Tier2 (Alpaca) -> Tier3 (Tradier)")

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
                # DISABLED - NOW USING 3-TIER SYSTEM
                # self.batch_scan_tickers(scan_list)
                pass
                
                print(f"Scan completed in {time.time() - start_time:.1f}s")
                time.sleep(300)
            except Exception as e:
                print(f"Scan error: {e}")
                time.sleep(60)
    # =====================================================
    # THREE-TIER ARCHITECTURE
    # =====================================================
    
    def _tier1_yfinance_bulk_prefilter(self):
        """Tier 1: yfinance bulk prefilter - scans market in 500-ticker chunks"""
        while self.running:
            try:
                scanner_logger.info(f"[TIER1] Starting scan at {datetime.datetime.now(NY_TZ)}")
                start_time = time.time()
                candidates = []
                
                universe = self.all_tickers[:8000] if len(self.all_tickers) > 8000 else self.all_tickers
                chunk_size = 500
                
                for chunk_idx in range(0, len(universe), chunk_size):
                    chunk = universe[chunk_idx:chunk_idx + chunk_size]
                    
                    try:
                        data = yf.download(tickers=chunk, period="1d", interval="1m", group_by='ticker', threads=True, progress=False)
                        
                        if data.empty:
                            time.sleep(5)
                            continue
                        
                        for ticker in chunk:
                            try:
                                if len(chunk) == 1:
                                    ticker_df = data
                                else:
                                    if ticker in data.columns.get_level_values(0):
                                        ticker_df = data[ticker]
                                    else:
                                        continue
                                
                                if ticker_df is None or ticker_df.empty:
                                    continue
                                
                                if 'Close' in ticker_df.columns:
                                    close_prices = ticker_df['Close'].dropna()
                                    if close_prices.empty:
                                        continue
                                    current_price = float(close_prices.iloc[-1])
                                else:
                                    continue
                                
                                if current_price < 1.0 or current_price > 10.0:
                                    continue
                                
                                if 'Volume' in ticker_df.columns:
                                    volumes = ticker_df['Volume'].dropna()
                                    if volumes.empty:
                                        continue
                                    current_volume = int(volumes.iloc[-1])
                                else:
                                    continue
                                
                                try:
                                    t = yf.Ticker(ticker)
                                    info = t.info or {}
                                    avg_volume = int(info.get('averageVolume', 0))
                                    
                                    if avg_volume < 2000000:
                                        continue
                                    
                                    candidates.append({'symbol': ticker, 'current_price': current_price, 'volume': current_volume, 'avg_volume': avg_volume})
                                except:
                                    continue
                            except:
                                continue
                        
                        time.sleep(2)
                    except Exception as e:
                        scanner_logger.error(f"[TIER1] Chunk error: {e}")
                        time.sleep(10)
                        continue
                
                scanner_logger.info(f"[TIER1] Found {len(candidates)} candidates in {time.time()-start_time:.1f}s")
                
                with open("prefiltered_candidates.json", "w") as f:
                    json.dump(candidates, f, indent=2)
                
                if candidates:
                    self.tier1_shortlist_queue.put(candidates)
                
                time.sleep(3600)
            except Exception as e:
                scanner_logger.error(f"[TIER1] Error: {e}")
                time.sleep(3600)

    def _tier2_alpaca_websocket_manager(self):
        """
        Tier 2: Alpaca WebSocket
        - Subscribes to Tier 1 shortlist (375-500 symbols)
        - Real-time price/volume streaming
        - Fills missing data (RVol, float, etc.)
        - Validates yfinance data accuracy
        - Passes validated tickers to Tier 3
        """
        print("[TIER2] Alpaca WebSocket manager started")
        
        # Store current shortlist and WebSocket connection
        self.current_alpaca_symbols = []
        self.alpaca_ws = None
        self.alpaca_validated_data = {}
        
        def on_open(ws):
            import json
            print("[TIER2] Alpaca WebSocket connected, authenticating...")
            auth_msg = {
                "action": "auth",
                "key": ALPACA_API_KEY,
                "secret": ALPACA_SECRET_KEY
            }
            ws.send(json.dumps(auth_msg))
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle auth response
                if isinstance(data, list):
                    for msg in data:
                        msg_type = msg.get("T")
                        
                        # Authentication successful
                        if msg_type == "success" and msg.get("msg") == "authenticated":
                            print("[TIER2] ✓ Authenticated with Alpaca")
                            # Subscribe to symbols
                            if self.current_alpaca_symbols:
                                self._alpaca_subscribe(ws, self.current_alpaca_symbols)
                        
                        # Quote data (real-time price updates)
                        elif msg_type == "q":
                            symbol = msg.get("S")
                            ask_price = msg.get("ap")
                            bid_price = msg.get("bp")
                            ask_size = msg.get("as")
                            bid_size = msg.get("bs")
                            timestamp = msg.get("t")
                            
                            if symbol and ask_price:
                                # Store validated price data
                                self.alpaca_validated_data[symbol] = {
                                    "symbol": symbol,
                                    "alpaca_price": ask_price,
                                    "bid_price": bid_price,
                                    "ask_size": ask_size,
                                    "bid_size": bid_size,
                                    "timestamp": timestamp,
                                    "validated": True
                                }
                                
                                # Print validation progress
                                validated_count = len(self.alpaca_validated_data)
                                if validated_count % 50 == 0:
                                    print(f"[TIER2] Validated {validated_count}/{len(self.current_alpaca_symbols)} symbols...")
                        
                        # Trade data
                        elif msg_type == "t":
                            symbol = msg.get("S")
                            price = msg.get("p")
                            size = msg.get("s")
                            
                            if symbol and price:
                                if symbol not in self.alpaca_validated_data:
                                    self.alpaca_validated_data[symbol] = {}
                                
                                self.alpaca_validated_data[symbol].update({
                                    "last_trade_price": price,
                                    "last_trade_size": size
                                })
                
            except Exception as e:
                print(f"[TIER2] Message processing error: {e}")
        
        def on_error(ws, error):
            print(f"[TIER2] WebSocket error: {error}")
        
        def on_close(ws, *args):
            print("[TIER2] Alpaca WebSocket closed")
        
        # Load existing prefiltered list on startup (BEFORE while loop)
        try:
            import json
            with open("prefiltered_candidates.json", "r") as f:
                startup_list = json.load(f)
                if startup_list:
                    self.tier1_shortlist_queue.put(startup_list)
                    scanner_logger.info(f"[TIER2] Loaded {len(startup_list)} tickers from persistent file")
        except:
            pass
        
        while self.running:
            try:
                shortlist = self.tier1_shortlist_queue.get()
                scanner_logger.info(f"[TIER2] Received {len(shortlist)} tickers from Tier1")
                print(f"\n{'='*60}")
                print(f"[TIER2] Received {len(shortlist)} tickers from Tier 1")
                print(f"{'='*60}")
                
                # Extract symbols
                symbols = [item['symbol'] for item in shortlist][:500]  # Max 500
                self.current_alpaca_symbols = symbols
                
                # Close existing WebSocket if any
                if self.alpaca_ws:
                    self.alpaca_ws.close()
                    time.sleep(1)
                
                # Reset validation data
                self.alpaca_validated_data = {}
                
                # Create WebSocket connection
                ws_url = "wss://stream.data.alpaca.markets/v2/iex"
                self.alpaca_ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close
                )
                
                # Run WebSocket in separate thread
                ws_thread = threading.Thread(
                    target=self.alpaca_ws.run_forever,
                    kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
                    daemon=True
                )
                ws_thread.start()
                
                # Wait for validation to complete (30 seconds max)
                print("[TIER2] Waiting for validation data...")
                time.sleep(30)
                
                # Merge Alpaca validation with original Tier 1 data
                validated_list = []
                for item in shortlist:
                    symbol = item['symbol']
                    
                    # Add Alpaca real-time data if available
                    if symbol in self.alpaca_validated_data:
                        item.update(self.alpaca_validated_data[symbol])
                    
                    # Calculate price variance if we have both prices
                    if 'alpaca_price' in item and 'current_price' in item:
                        yf_price = item['current_price']
                        alpaca_price = item['alpaca_price']
                        variance = abs(alpaca_price - yf_price) / yf_price * 100
                        item['price_variance'] = variance
                        
                        if variance > 2.0:
                            print(f"[TIER2] ⚠ {symbol} variance: {variance:.2f}% (yf: ${yf_price:.2f}, alpaca: ${alpaca_price:.2f})")
                    
                    validated_list.append(item)
                
                print(f"[TIER2] ✓ Validated {len(self.alpaca_validated_data)}/{len(symbols)} symbols with live data")
                print(f"[TIER2] → Passing {len(validated_list)} tickers to Tier 3 queue")

                import json
                with open('alpaca_validated.json', 'w') as f:
                    json.dump(validated_list, f, indent=2)
                print(f"[FILE] Saved {len(validated_list)} Alpaca-validated symbols for Tradier.")

                # Pass to Tier 3
                self.tier2_validated_queue.put(validated_list)
                
            except Exception as e:
                print(f"[TIER2] ✗ Error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)

    def _alpaca_subscribe(self, ws, symbols):
        """Helper method to subscribe to Alpaca symbols"""
        try:
            subscribe_msg = {
                "action": "subscribe",
                "quotes": symbols,
                "trades": symbols
            }
            ws.send(json.dumps(subscribe_msg))
            print(f"[TIER2] Subscribed to {len(symbols)} symbols for quotes and trades")
        except Exception as e:
            print(f"[TIER2] Subscribe error: {e}")
            scanner_logger.info(f"[TIER2] Subscribe error: {e}")
    
    def _tier3_tradier_websocket_manager(self):
        """
        Tier 3: Tradier WebSocket
        - Receives validated tickers from Alpaca
        - Tick-by-tick price updates (sub-second)
        - Primary categorization engine - runs categorize_stock() on every update
        - Detects quick moves (5% in 5min, 10% in 10min)
        - Triggers sound alerts
        - Keeps ALL data up-to-date for GUI
        """
        print("[TIER3] Tradier WebSocket manager started")
        scanner_logger.info("[TIER3] Tradier WebSocket manager started")
        
        # Store current subscription and WebSocket connection
        self.current_tradier_symbols = []
        self.tradier_ws = None
        self.tradier_session_id = None
        self.tradier_price_history = {}  # Track price movements for quick move detection
        
        def on_open(ws):
            print("[TIER3] Tradier WebSocket connected, subscribing...")
            if self.current_tradier_symbols and self.tradier_session_id:
                self._tradier_subscribe(ws, self.current_tradier_symbols, self.tradier_session_id)
        
        def on_message(ws, message):
            try:
                import json
                data = json.loads(message)
                
                # Extract symbol and price data
                symbol = data.get("symbol")
                if not symbol:
                    return
                
                # Real-time tick data
                last_price = data.get("last")
                bid = data.get("bid")
                ask = data.get("ask")
                volume = data.get("volume")
                timestamp = data.get("time")
                
                if not last_price:
                    return
                
                # Update stock data
                if symbol not in self.stock_data:
                    self.stock_data[symbol] = {}
                
                self.stock_data[symbol].update({
                    "symbol": symbol,
                    "current_price": last_price,
                    "bid": bid,
                    "ask": ask,
                    "volume": volume,
                    "last_update": datetime.datetime.now().isoformat(),
                    "tier3_active": True
                })
                
                # Track price history for quick move detection
                now = time.time()
                if symbol not in self.tradier_price_history:
                    self.tradier_price_history[symbol] = []
                
                self.tradier_price_history[symbol].append({
                    "price": float(last_price), "timestamp": now})
                
                # Keep only last 10 minutes of data
                cutoff = now - 600
                self.tradier_price_history[symbol] = [
                    p for p in self.tradier_price_history[symbol] 
                    if p["timestamp"] > cutoff
                ]
                
                # Detect quick moves
                self._detect_quick_moves(symbol, last_price)
                
                # Run categorization engine on this ticker
                self._run_categorization(symbol)
                
            except Exception as e:
                print(f"[TIER3] Message processing error: {e}")
                scanner_logger.error(f"[TIER3] Message processing error: {e}")
        
        def on_error(ws, error):
            print(f"[TIER3] WebSocket error: {error}")
            scanner_logger.error(f"[TIER3] WebSocket error: {error}")

        def on_close(ws, *args):
            print("[TIER3] Tradier WebSocket closed")
        
        # Load existing validated list on startup (BEFORE the while loop)
        try:
            import json
            with open("alpaca_validated.json", "r") as f:
                startup_list = json.load(f)
                if startup_list:
                    self.tier2_validated_queue.put(startup_list)
                    scanner_logger.info(f"[TIER3] Loaded {len(startup_list)} tickers from persistent file")
        except:
            pass
        
        while self.running:
            try:
                validated_list = self.tier2_validated_queue.get()
                print(f"\n{'='*60}")
                print(f"[TIER3] Received {len(validated_list)} validated tickers from Tier 2")
                scanner_logger.info(f"[TIER3] Received {len(validated_list)} validated tickers from Tier 2")
                print(f"{'='*60}")
                
                # Extract symbols (max 375 for Tradier)
                symbols = [item['symbol'] for item in validated_list][:375]
                self.current_tradier_symbols = symbols
                
                # Store validated data in stock_data
                for item in validated_list:
                    symbol = item['symbol']
                    self.stock_data[symbol] = item

                import json
                with open('tradier_final.json', 'w') as f:
                    json.dump(self.stock_data, f, indent=2)
                print(f"[FILE] Saved latest Tradier/GUI symbols for maintenance.")

                # Get Tradier session ID
                print("[TIER3] Requesting Tradier session ID...")
                self.tradier_session_id = self._get_tradier_session_id()
                
                if not self.tradier_session_id:
                    print("[TIER3] ✗ Failed to get session ID")
                    scanner_logger.error("[TIER3] Failed to get session ID")
                    time.sleep(30)
                    continue
                
                print(f"[TIER3] ✓ Session ID obtained")
                scanner_logger.info(f"[TIER3] Session ID obtained: {self.tradier_session_id[:10]}...")
                
                # Close existing WebSocket if any
                if self.tradier_ws:
                    self.tradier_ws.close()
                    time.sleep(1)
                
                # Create WebSocket connection
                ws_url = "wss://ws.tradier.com/v1/markets/events"
                self.tradier_ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close
                )
                
                # Run WebSocket in separate thread
                ws_thread = threading.Thread(
                    target=self.tradier_ws.run_forever,
                    kwargs={"sslopt": {"cert_reqs": ssl.CERT_NONE}},
                    daemon=True
                )
                ws_thread.start()
                
                print(f"[TIER3] ✓ WebSocket streaming {len(symbols)} symbols")
                scanner_logger.info(f"[TIER3] WebSocket streaming {len(symbols)} symbols")
                print("[TIER3] Categorization engine active - monitoring for quick moves...")
                scanner_logger.info("[TIER3] Categorization engine active - monitoring for quick moves...")
                
                # Keep WebSocket running until new Tier 2 data arrives
                while self.tier2_validated_queue.empty():
                    time.sleep(5)
                
            except Exception as e:
                print(f"[TIER3] ✗ Error: {e}")
                scanner_logger.error(f"[TIER3] Error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)

    
    def _tradier_subscribe(self, ws, symbols, session_id):
        """Helper method to subscribe to Tradier symbols"""
        try:
            subscribe_msg = {
                "symbols": symbols,
                "sessionid": session_id,
                "linebreak": True
            }
            ws.send(json.dumps(subscribe_msg))
            print(f"[TIER3] Subscribed to {len(symbols)} symbols")
            scanner_logger.info(f"[TIER3] Subscribed to {len(symbols)} symbols")
        except Exception as e:
            print(f"[TIER3] Subscribe error: {e}")
            scanner_logger.error(f"[TIER3] Subscribe error: {e}")

    def _get_tradier_session_id(self):
        """Fetch Tradier WebSocket session ID via REST"""
        try:
            url = "https://api.tradier.com/v1/markets/events/session"
            headers = {
                "Authorization": f"Bearer {TRADIER_ACCESS_TOKEN}",
                "Accept": "application/json"
            }
            response = requests.post(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                session_id = data.get("stream", {}).get("sessionid")
                return session_id
            else:
                print(f"[TIER3] Session request failed: {response.status_code}")
                scanner_logger.error(f"[TIER3] Session request failed: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"[TIER3] Session ID error: {e}")
            scanner_logger.error(f"[TIER3] Session ID error: {e}")
            return None
    
    def _detect_quick_moves(self, symbol, current_price):
        """Detect 5% in 5min or 10% in 10min moves"""
        try:
            if symbol not in self.tradier_price_history:
                return
            
            history = self.tradier_price_history[symbol]
            if len(history) < 2:
                return
            
            now = time.time()
            current_price = float(current_price)

            # Check 5% in 5 minutes
            five_min_ago = now - 300
            five_min_prices = [p["price"] for p in history if p["timestamp"] >= five_min_ago]
            if five_min_prices:
                min_5min = min(five_min_prices)
                max_5min = max(five_min_prices)
                move_5min = abs(current_price - min_5min) / min_5min * 100 if min_5min > 0 else 0
                
                if move_5min >= 5.0:
                    print(f"[TIER3] 🚀 {symbol} QUICK MOVE: {move_5min:.1f}% in 5min")
                    self._trigger_quick_move_alert(symbol, move_5min, "5min")
            
            # Check 10% in 10 minutes
            ten_min_ago = now - 600
            ten_min_prices = [p["price"] for p in history if p["timestamp"] >= ten_min_ago]
            if ten_min_prices:
                min_10min = min(ten_min_prices)
                max_10min = max(ten_min_prices)
                move_10min = abs(current_price - min_10min) / min_10min * 100 if min_10min > 0 else 0
                
                if move_10min >= 10.0:
                    print(f"[TIER3] 🚀🚀 {symbol} BIG MOVE: {move_10min:.1f}% in 10min")
                    scanner_logger.info(f"[TIER3] 🚀🚀 {symbol} BIG MOVE: {move_10min:.1f}% in 10min")
                    self._trigger_quick_move_alert(symbol, move_10min, "10min")
                    
        except Exception as e:
            print(f"[TIER3] Quick move detection error: {e}")
            scanner_logger.error(f"[TIER3] Quick move detection error: {e}")

    def _trigger_quick_move_alert(self, symbol, move_pct, timeframe):
        """Trigger sound alert for quick moves"""
        try:
            # Update stock data with alert flag
            if symbol in self.stock_data:
                self.stock_data[symbol]["quick_move"] = {
                    "percent": move_pct,
                    "timeframe": timeframe,
                    "timestamp": datetime.datetime.now(NY_TZ)
                }
            
            # Trigger callback to update GUI
            if self.callback:
                Clock.schedule_once(lambda dt, s=symbol: self.callback(s, self.stock_data[symbol]), 0)
            
        except Exception as e:
            print(f"[TIER3] Alert trigger error: {e}")
            scanner_logger.error(f"[TIER3] Alert trigger error: {e}")

    def _run_categorization(self, symbol):
        """Run categorization engine on updated ticker data"""
        try:
            if symbol not in self.stock_data:
                scanner_logger.error(f"[TIER3] {symbol} not in stock_data")
                return
        
            # Get stock data
            data = self.stock_data[symbol]
            current_price = data.get('current_price', 0) or 0
            volume = data.get('volume', 0) or 0
        
            if not current_price or current_price == 0:
                return
        
            # Get enrichment data from stock_data
            changepct = data.get('changepct', 0)
            rvol = data.get('rvol', 0)
            floatshares = data.get('floatshares', 0)
            isnewhod = data.get('isnewhod', False)
            
            # Call the REAL categorization function from EnrichmentManager
            channel = self.enrichment_manager_ref.check_gates(
                symbol=symbol,
                price=current_price,
                change_pct=changepct,
                rvol=rvol,
                volume=volume,
                float_shares=floatshares,
                is_new_hod=isnewhod
            )

            # If no channel assigned by categorization, skip GUI update
            if not channel:
                return
        
            # Pass to GUI with channel assignment
            if channel and self.callback:
                # Format data as GUI list
                gui_data = [
                    symbol,
                    datetime.datetime.now(NY_TZ).strftime('%I:%M %p'),
                    "Tradier Real-Time",
                    f"${data.get('current_price', 0):.2f}",
                    "0.00%",  # Calculate % change if you have previous price
                    data.get('volume', 0),
                    "N/A",
                    "N/A"
                ]
                Clock.schedule_once(lambda dt, s=symbol, d=gui_data: self.callback(s, d), 0)

                scanner_logger.info(f"[TIER3] {symbol} assigned to {channel} channel")
                
        except Exception as e:
            print(f"[TIER3] Categorization error: {e}")
            scanner_logger.error(f"[TIER3] Categorization error: {e}")

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
                    cbvol = int(hist['Volume'].iloc[-1])  # current bar volume
                    day_high = float(hist['High'].max())

                    # Upfront price filter: $0 <= price <= $10
                    if current_price > 10.0 or current_price < 1.0:
                        scanner_logger.debug(f"[SCAN] {symbol} rejected: price=${current_price:.2f} (need $1-$10)")
                        continue

                    # Upfront volume filter: current volume > 0
                    info = {}
                    try:
                        info = t.info or {}
                    except:
                        pass

                    # Get average volume (use int, not float)
                    avg_volume = int(info.get('averageVolume', 0))

                    # FILTER: Average volume must be >= 2M
                    if avg_volume < 2000000:
                        scanner_logger.debug(f"[SCAN] {symbol} rejected: avgvol={avg_volume:,} (need 2M+)")
                        continue

                    scanner_logger.info(f"[SCAN] ✓ {symbol} PASSED: ${current_price:.2f}, avgvol={avg_volume/1e6:.1f}M")

                    prev_close = float(info.get('previousClose', info.get('regularMarketPreviousClose', current_price)))
                    if symbol in self.yesterday_prices and self.yesterday_prices[symbol] > 0:
                        prev_close = self.yesterday_prices[symbol]

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
                        'cbvol': cbvol,
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
            if avg_volume <= 0:
                return 0.0
            return round(current_volume / avg_volume, 2)
        except Exception:
            return 0.0

    def get_index_data(self, symbol):
        try:
            t = yf.Ticker(symbol)
            info = t.info or {}
            
            current = float(info.get('regularMarketPrice', 0))
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

class PerplexityManager:
    """Manages manual Perplexity API deep news checks with usage tracking"""
    
    def __init__(self):
        self.api_key = os.getenv('PERPLEXITY_API_KEY')
        self.monthly_spend = 0.0
        self.monthly_limit = 5.00
        self.query_count = 0
        self.last_reset = datetime.datetime.now(NY_TZ)
        self.cache = {}  # Cache results for 30 minutes
        
        if not self.api_key:
            print("[PERPLEXITY] Warning: API key not found")
    
    def check_monthly_reset(self):
        """Reset usage tracking on first of month"""
        now = datetime.datetime.now(NY_TZ)
        if now.month != self.last_reset.month:
            self.monthly_spend = 0.0
            self.query_count = 0
            self.last_reset = now
            print(f"[PERPLEXITY] Monthly usage reset - New month: {now.strftime('%B')}")
    
    def can_query(self):
        """Check if within budget limits"""
        self.check_monthly_reset()
        remaining = self.monthly_limit - self.monthly_spend
        return remaining > 0.01  # Keep $0.01 buffer
    
    def get_deep_news(self, ticker):
        """
        Perform deep news check for a ticker using Perplexity API
        Returns: (success, news_text, cost)
        """
        if not self.api_key:
            return False, "Perplexity API key not configured", 0.0
        
        if not self.can_query():
            remaining = self.monthly_limit - self.monthly_spend
            return False, f"Monthly limit reached (${remaining:.3f} remaining)", 0.0
        
        # Check cache (30 minute expiry)
        cache_key = f"{ticker}_{datetime.datetime.now(NY_TZ).strftime('%Y%m%d%H%M')[:11]}"
        if cache_key in self.cache:
            print(f"[PERPLEXITY] Using cached result for {ticker}")
            return True, self.cache[cache_key], 0.0
        
        try:
            url = "https://api.perplexity.ai/chat/completions"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            prompt = f"""Provide a concise summary of breaking news and catalysts for stock ticker ${ticker} in the last 24 hours. 
Include: FDA approvals, earnings, mergers, partnerships, SEC filings, analyst upgrades/downgrades, or significant price movements.
Format: Brief bullet points. If no significant news, state "No breaking catalysts found." Keep response under 150 words."""
            
            payload = {
                "model": "sonar",  # Most cost-effective at $0.005/request
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a stock market news analyst providing concise catalyst summaries."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 200,  # Limit response length to control costs
                "temperature": 0.2   # More factual, less creative
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                news_text = data['choices'][0]['message']['content']
                
                # Estimate cost (Sonar: $0.005 per request + minimal token costs)
                input_tokens = len(prompt.split()) * 1.3  # Rough estimate
                output_tokens = len(news_text.split()) * 1.3
                cost = 0.005 + (input_tokens / 1000000) + (output_tokens / 1000000)
                
                # Track usage
                self.monthly_spend += cost
                self.query_count += 1
                
                # Cache result
                self.cache[cache_key] = news_text
                
                print(f"[PERPLEXITY] {ticker} query successful - Cost: ${cost:.4f} | Total: ${self.monthly_spend:.2f}/{self.monthly_limit}")
                return True, news_text, cost
            
            else:
                error_msg = f"API Error: {response.status_code}"
                print(f"[PERPLEXITY] {ticker} failed: {error_msg}")
                return False, error_msg, 0.0
        
        except Exception as e:
            print(f"[PERPLEXITY] Exception for {ticker}: {e}")
            return False, f"Error: {str(e)}", 0.0
    
    def get_usage_stats(self):
        """Return usage statistics for display"""
        self.check_monthly_reset()
        remaining = self.monthly_limit - self.monthly_spend
        queries_left = int(remaining / 0.006)  # Estimate remaining queries
        
        return {
            'spent': self.monthly_spend,
            'limit': self.monthly_limit,
            'remaining': remaining,
            'query_count': self.query_count,
            'queries_remaining_estimate': queries_left,
            'percent_used': (self.monthly_spend / self.monthly_limit) * 100
        }

class SignalScanApp(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.orientation = "vertical"
        self.spacing = 0
        
        with self.canvas.before:
            Color(0.08, 0.08, 0.08, 1)
            self.bg_rect = Rectangle(size=self.size, pos=self.pos)
        self.bind(size=self._update_bg, pos=self._update_bg)
        
        self.live_data = {k: [] for k in ["PreGap", "HOD", "RunUp", "P-HOD", "P-RunUp", "Rvsl", "Halts", "BKG-News"]}
        self.stock_news = {}
        self.price_snapshots = {}
        self.current_channel = "RunUp"
        self.nasdaq_last = self.nasdaq_pct = 0.0
        self.sp_last = self.sp_pct = 0.0
        self.current_sort_column = None
        self.current_sort_ascending = True
        self.is_kiosk_mode = False
        
        self.sound_manager = SoundManager()
        self.enrichment_manager = EnrichmentManager()
        self.news_manager = None
        self.perplexity_manager = PerplexityManager()
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
        Clock.schedule_interval(self.check_halt_resumptions, 10)  # Check every 10 seconds
        Clock.schedule_interval(self.check_midnight_reset, 60)  # Check every minute
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
            news_logger.info("NEWS FEED: all_tickers not loaded, retrying...")
            Clock.schedule_once(self.start_news_feed, 2)
            return
        
        self.stock_news.clear()
        print(f"[NEWS] Cleared {len(self.stock_news)} old news entries")       
        
        wl = self.market_data.all_tickers or []
        if not wl:
            print("NEWS FEED: watchlist is empty after load, forcing rebuild...")
            news_logger.info("NEWS FEED: watchlist is empty after load, forcing rebuild...")
            self.market_data.maintenance_engine.weekend_mega_build()
            Clock.schedule_once(self.start_news_feed, 5)
            return

        self.news_manager = NewsManager(
            callback=self.on_news_update,
            sound_manager_ref=self.sound_manager,
            # Load prefiltered watchlist from Alpaca validation
        # Load prefiltered watchlist from Alpaca validation
        validated_file = "alpaca_validated.json"
        )
        try:
            with open(validated_file, 'r') as f:
                prefiltered = json.load(f)
                news_watchlist = [item['symbol'] for item in prefiltered]
                print(f"[NEWS] Using {len(news_watchlist)} prefiltered tickers for watchlist")
        except:
            news_watchlist = []
            print(f"[NEWS] No validated file found, starting with empty watchlist")
        
        self.news_manager = NewsManager(
            callback=self.on_news_update,
            sound_manager_ref=self.sound_manager,
            watchlist=news_watchlist,
            news_trigger_callback=self.market_data.add_news_trigger
        )
        self.market_data.news_manager_ref = self.news_manager
        self.news_manager.start_news_stream()
        print(f"NEWS FEED STARTED with {len(news_watchlist)} tickers")
        news_logger.info(f"NEWS FEED STARTED with {len(news_watchlist)} tickers")

    def start_halt_monitor(self, dt=None):
        self.halt_manager.start_halt_monitor()

    def on_data_update(self, symbol, channel, data):
        self.process_stock_update(symbol, channel, data)

    def on_news_update(self, news_data):
        print(f"[DEBUG] on_news_update: {news_data.get('symbol', 'NO SYMBOL')} - {news_data.get('title', 'NO TITLE')[:50]}")
        news_logger.info(f"Received news update for {news_data.get('symbol', 'NO SYMBOL')}")
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
        print(f"[HALT-UPDATE] Received halt  {halt_data}")  # ADD THIS LINE
        halt_logger.info(f"Received halt_data with {len(halt_data)} symbols: {list(halt_data.keys())}")
        self.live_data['Halts'] = []
        for symbol, halt_list in halt_data.items():
            register_ticker_timestamp(symbol)
            stock_data = self.market_data.stock_data.get(symbol, {})
            price = stock_data.get('current_price', 0)
            change_pct = stock_data.get('changepct', 0)
            price_str = f"{price:.2f}" if price > 0 else "N/A"
            pct_str = f"{change_pct:.1f}%" if price > 0 else "N/A"
            
            for halt_info in halt_list:
                # Check if alert is set for this halt
                key = f"{symbol}_{halt_info['reason'][:20]}"
                alert_indicator = "🔔" if key in halt_resumption_alerts else ""
                
                # Get news status for this symbol
                if symbol in self.stock_news:
                    tier = self.stock_news[symbol].get('tier', 3)
                    news_text = "BREAK" if tier == 2 else "NEWS"
                else:
                    news_text = ""
                
                self.live_data['Halts'].append(
                    (symbol, get_timestamp_display(symbol), halt_info['reason'][:20],
                     price_str, pct_str, news_text, alert_indicator)
                )

    def toggle_halt_alert(self, symbol, reason):
        """Toggle alert for halt resumption"""
        key = f"{symbol}_{reason}"
        
        if key in halt_resumption_alerts:
            del halt_resumption_alerts[key]
            print(f"[HALT-ALERT] Removed alert for {symbol}")
        else:
            halt_resumption_alerts[key] = {
                'symbol': symbol,
                'reason': reason,
                'alerted': False
            }
            print(f"[HALT-ALERT] Added alert for {symbol} (Reason: {reason})")
        
        self.refresh_data_table()

    def show_halt_alert_popup(self, symbol, resume_time):
        """Show popup alert window for halt resumption"""
        def create_popup():
            popup = tk.Toplevel()
            popup.title("Halt Resumed")
            popup.geometry("400x150")
            popup.configure(bg='#1a1a1a')
            popup.attributes('-topmost', True)
            
            # Center on screen
            popup.update_idletasks()
            x = (popup.winfo_screenwidth() // 2) - 200
            y = 100
            popup.geometry(f"+{x}+{y}")
            
            # Title
            title_label = tk.Label(popup, text="HALT RESUMED", font=('Arial', 18, 'bold'), 
                                  fg='#00ff00', bg='#1a1a1a')
            title_label.pack(pady=10)
            
            # Symbol
            symbol_label = tk.Label(popup, text=symbol, font=('Arial', 24, 'bold'), 
                                   fg='#ffffff', bg='#1a1a1a')
            symbol_label.pack(pady=5)
            
            # Resume time
            time_label = tk.Label(popup, text=f"Resumes: {resume_time} ET", 
                                 font=('Arial', 14), fg='#cccccc', bg='#1a1a1a')
            time_label.pack(pady=10)
            
            # Auto-close after 10 seconds
            popup.after(10000, popup.destroy)
            
            # Click to close
            popup.bind('<Button-1>', lambda e: popup.destroy())
            
        # Run in separate thread to avoid blocking
        threading.Thread(target=create_popup, daemon=True).start()

    def check_halt_resumptions(self, dt=None):
        """Check for halt resumptions and trigger alerts"""
        for key, alert_info in list(halt_resumption_alerts.items()):
            if alert_info['alerted']:
                continue
                
            symbol = alert_info['symbol']
            
            # Check if halt is in current halt_data
            if symbol in self.halt_manager.halt_:
                halt_list = self.halt_manager.halt_data[symbol]
                
                for halt in halt_list:
                    if halt.get('resume_time') and halt['resume_time'] != 'Pending':
                        # Resumption time is set - trigger alert
                        halt_resumption_alerts[key]['alerted'] = True
                        self.sound_manager.play_halt_resume_alert()
                        self.show_halt_alert_popup(symbol, halt['resume_time'])
                        print(f"[HALT-RESUMED] {symbol} resumption at {halt['resume_time']}")
                        break

    def show_halt_alert_popup(self, symbol, resume_time):
        """Show popup alert window for halt resumption"""
        def create_popup():
            popup = tk.Toplevel()
            popup.title("Halt Resumed")
            popup.geometry("400x150")
            popup.configure(bg='#1a1a1a')
            popup.attributes('-topmost', True)
            
            # Center on screen
            popup.update_idletasks()
            x = (popup.winfo_screenwidth() // 2) - 200
            y = 100
            popup.geometry(f"+{x}+{y}")
            
            # Title
            title_label = tk.Label(popup, text="HALT RESUMED", font=('Arial', 18, 'bold'), 
                                  fg='#00ff00', bg='#1a1a1a')
            title_label.pack(pady=10)
            
            # Symbol
            symbol_label = tk.Label(popup, text=symbol, font=('Arial', 24, 'bold'), 
                                   fg='#ffffff', bg='#1a1a1a')
            symbol_label.pack(pady=5)
            
            # Resume time
            time_label = tk.Label(popup, text=f"Resumes: {resume_time} ET", 
                                 font=('Arial', 14), fg='#cccccc', bg='#1a1a1a')
            time_label.pack(pady=10)
            
            # Auto-close after 10 seconds
            popup.after(10000, popup.destroy)
            
            # Click to close
            popup.bind('<Button-1>', lambda e: popup.destroy())
            
        # Run in separate thread to avoid blocking
        threading.Thread(target=create_popup, daemon=True).start()

    def check_halt_resumptions(self, dt=None):
        """Check for halt resumptions and trigger alerts"""
        for key, alert_info in list(halt_resumption_alerts.items()):
            if alert_info['alerted']:
                continue
                
            symbol = alert_info['symbol']
            
            # Check if halt is in current halt_data
            if symbol in self.halt_manager.halt_:
                halt_list = self.halt_manager.halt_data[symbol]                
                for halt in halt_list:
                    if halt.get('resume_time') and halt['resume_time'] != 'Pending':
                        # Resumption time is set - trigger alert
                        halt_resumption_alerts[key]['alerted'] = True
                        self.sound_manager.play_alert_sound()
                        self.show_halt_alert_popup(symbol, halt['resume_time'])
                        print(f"[HALT-RESUMED] {symbol} resumption at {halt['resume_time']}")
                        break

    def toggle_halt_alert(self, symbol, reason):
        """Toggle alert for halt resumption"""
        key = f"{symbol}_{reason}"
        
        if key in halt_resumption_alerts:
            del halt_resumption_alerts[key]
            print(f"[HALT-ALERT] Removed alert for {symbol}")
        else:
            halt_resumption_alerts[key] = {
                'symbol': symbol,
                'reason': reason,
                'alerted': False
            }
            print(f"[HALT-ALERT] Added alert for {symbol} (Reason: {reason})")
        
        self.refresh_data_table()
        
        if key in halt_resumption_alerts:
            del halt_resumption_alerts[key]
            print(f"[HALT-ALERT] Removed alert for {symbol}")
        else:
            halt_resumption_alerts[key] = {
                'symbol': symbol,
                'halt_time': halt_time,
                'alerted': False
            }
            print(f"[HALT-ALERT] Added alert for {symbol} resumption")
        
        self.refresh_data_table()

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

            # Track price history for quick move detection
            now = datetime.datetime.now(NY_TZ)
            if symbol not in self.price_snapshots:
                self.price_snapshots[symbol] = []
            
            self.price_snapshots[symbol].append((now, price))
            
            # Keep only last 15 minutes of data
            cutoff = now - datetime.timedelta(minutes=15)
            self.price_snapshots[symbol] = [(ts, p) for ts, p in self.price_snapshots[symbol] if ts >= cutoff]

            # Get current bar volume from data
            cbvol = data.get('cbvol', 0)
            cbvolstr = self.format_volume(cbvol) if cbvol > 0 else "N/A"

            formatted = [symbol, get_timestamp_display(symbol), f"${price:.2f}", f"{change_pct:+.1f}%",
                         cbvolstr, self.format_volume(volume), float_str, rvol_str,"NEWS"]

            self.categorize_stock(formatted, change_pct, volume, rvol, float_shares, price, is_new_hod, is_52wk_high)
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    def check_quick_move(self, symbol, current_price):
        """
        Check if stock meets quick move criteria:
        - 5% gain in last 5 minutes, OR
        - 10% gain in last 10 minutes
        """
        if symbol not in self.price_snapshots or len(self.price_snapshots[symbol]) < 2:
            return False
        
        now_est = datetime.datetime.now(NY_TZ)
        snapshots = self.price_snapshots[symbol]
        
        # Check 5% in 5 minutes
        five_min_ago = now_est - datetime.timedelta(minutes=5)
        five_min_prices = [p for ts, p in snapshots if ts >= five_min_ago]
        if five_min_prices:
            min_price_5m = min(five_min_prices)
            if min_price_5m > 0:
                gain_5m = ((current_price - min_price_5m) / min_price_5m) * 100
                if gain_5m >= 5.0:
                    print(f"[QUICK-MOVE] {symbol}: 5min gain {gain_5m:.2f}% detected")
                    return True
        
        # Check 10% in 10 minutes
        ten_min_ago = now_est - datetime.timedelta(minutes=10)
        ten_min_prices = [p for ts, p in snapshots if ts >= ten_min_ago]
        if ten_min_prices:
            min_price_10m = min(ten_min_prices)
            if min_price_10m > 0:
                gain_10m = ((current_price - min_price_10m) / min_price_10m) * 100
                if gain_10m >= 10.0:
                    print(f"[QUICK-MOVE] {symbol}: 10min gain {gain_10m:.2f}% detected")
                    return True
        
        print(f"[QUICK-MOVE] {symbol}: No quick move detected")
        return False

    def categorize_stock(self, stock_data, change_pct, volume, rvol, float_shares, price, is_new_hod, is_52wk_high):
        ticker = stock_data[0]
        scanner_logger.debug(f"[CATEGORIZE] Processing {ticker}")
        was_in_hod = any(s[0] == ticker for s in self.live_data["HOD"])
        
        # Remove from all channels except Halts
        for ch in ["PreGap", "HOD", "RunUp", "P-HOD", "P-RunUp", "Rvsl"]:
            self.live_data[ch] = [s for s in self.live_data[ch] if s[0] != ticker]
        
        now_est = datetime.datetime.now(NY_TZ)
        is_premarket = now_est.hour < 9 or (now_est.hour == 9 and now_est.minute < 30)
        
        hod_candidate_added = False

        # PreGap: <= $15, premarket, >=10% change, >=500K volume, <=100M float
        if (is_premarket and price <= 15.0 and abs(change_pct) >= 10.0 and 
            volume >= 500000 and float_shares <= 100000000):
            self.live_data['PreGap'].append(stock_data)
            self.ticker_timestamp_registry[ticker] = datetime.now()
            self.enrichment_manager.record_channel_hit(ticker, "PreGap")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)

        # HOD: $1-$15, new HOD, >=5.0x RVOL, <=100M float, >=+10% gain
        if (1.0 <= price <= 15.0 and is_new_hod and rvol >= 5.0 and 
            float_shares <= 100000000 and change_pct >= 10.0):
            self.live_data['HOD'].append(stock_data)
            scanner_logger.info(f"[CATEGORIZE] {ticker} assigned to {channel}")
            self.ticker_timestamp_registry[ticker] = datetime.now()
            hod_candidate_added = True
            self.enrichment_manager.record_channel_hit(ticker, "HOD")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        # RunUp: $1-$15, gap>=10%, >=5x RVOL, float<10M, quick move (5% in 5min or 10% in 10min)
        runup_added = False
        if (1.0 <= price <= 15.0 and change_pct >= 10.0 and 
            rvol >= 5.0 and float_shares < 10 and self.check_quick_move(ticker, price)):
            self.live_data['RunUp'].append(stock_data)
            scanner_logger.info(f"[CATEGORIZE] {ticker} assigned to {channel}")
            self.ticker_timestamp_registry[ticker] = datetime.now()
            runup_added = True
            print(f"[RUNUP-QUALIFIED] {ticker}: ${price:.2f}, Gap {change_pct:.1f}%, RVol {rvol:.2f}x, Float {float_shares:.1f}M")           
            self.enrichment_manager.record_channel_hit(ticker, "RunUp")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        # Sound alert for new RunUp candidates (once per session)
        if runup_added and ticker not in self.candidate_alerted:
            self.candidate_alerted.add(ticker)
            print(f"[ALERT-FIRED] RunUp alert for {ticker}")
            self.sound_manager.play_candidate_alert()

        # P-HOD: <= $1, new HOD, >=5.0x RVOL, <=100M float, >=+10% gain
        if (price <= 1.0 and is_new_hod and rvol >= 5.0 and 
            float_shares <= 100000000 and change_pct >= 10.0):
            self.live_data['P-HOD'].append(stock_data)
            scanner_logger.info(f"[CATEGORIZE] {ticker} assigned to {channel}")
            self.ticker_timestamp_registry[ticker] = datetime.now()
            self.enrichment_manager.record_channel_hit(ticker, "P-HOD")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        # P-RunUp: <= $1, gap>=10%, >=7x RVOL, float<10M, quick move (5% in 5min or 10% in 10min)
        prunup_added = False       
        if (price <= 1.0 and change_pct >= 10.0 and 
            rvol >= 7.0 and float_shares < 10 and self.check_quick_move(ticker, price)):
            self.live_data['P-RunUp'].append(stock_data)
            scanner_logger.info(f"[CATEGORIZE] {ticker} assigned to {channel}")
            self.ticker_timestamp_registry[ticker] = datetime.now()
            prunup_added = True
            print(f"[P-RUNUP-QUALIFIED] {ticker}: ${price:.2f}, Gap {change_pct:.1f}%, RVol {rvol:.2f}x, Float {float_shares:.1f}M")          
            self.enrichment_manager.record_channel_hit(ticker, "P-RunUp")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        # Sound alert for new P-RunUp candidates (once per session)
        if prunup_added and ticker not in self.candidate_alerted:
            self.candidate_alerted.add(ticker)
            print(f"[ALERT-FIRED] P-RunUp alert for {ticker}")
            self.sound_manager.play_candidate_alert()

        # Rvsl: <=$15, >=8.0x RVOL, >=8% change
        if (price <= 15.0 and rvol >= 8.0 and abs(change_pct) >= 8.0):
            self.live_data['Rvsl'].append(stock_data)
            scanner_logger.info(f"[CATEGORIZE] {ticker} assigned to {channel}")
            self.ticker_timestamp_registry[ticker] = datetime.now()
            self.enrichment_manager.record_channel_hit(ticker, "Rvsl")
            if self.news_manager and ticker not in self.stock_news:
                self.news_manager.fetch_news_pair(ticker)
                time.sleep(0.1)
                self.news_manager.fetch_yfinance_news(ticker)
        
        # Breaking News Channel: Has breaking news regardless of technical criteria
        news_data = self.stock_news.get(ticker, {})
        if news_data.get('is_breaking', False):
            # Check if news age is still within breaking window (2 hours)
            news_age = news_data.get('age_hours', 999)
            if news_age <= 2.0:
                self.live_data['BKG-News'].append(stock_data)
                self.enrichment_manager.record_channel_hit(ticker, "BKG-News")
                register_breaking_news(ticker)
                # Play breaking news sound alert
                if self.sound_manager:
                    self.sound_manager.play_news_alert()

        # Sort channels by RVOL
        if self.current_sort_column is None:
            for ch in ["PreGap", "HOD", "RunUp", "P-HOD", "P-RunUp", "Rvsl"]:
                try:
                    self.live_data[ch].sort(key=lambda x: float(str(x[6]).replace('x', '')), reverse=True)
                except:
                    pass
        else:
            self.apply_current_sort()
        
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

    def cleanup_expired_tickers(self, dt=None):
        """Remove tickers older than 8 hours"""
        current_time = datetime.datetime.now()
        max_age = datetime.timedelta(hours=8)
        removed = 0
    
        for channel in list(self.live_data.keys()):
            original_count = len(self.live_data[channel])

            self.live_data[channel] = [
                stock for stock in self.live_data[channel]
                if stock[0] not in self.ticker_timestamp_registry or
                (current_time - self.ticker_timestamp_registry[stock[0]]['datetime']) <= max_age
            ]

            removed += (original_count - len(self.live_data[channel]))
    
        if removed > 0:
            print(f"[CLEANUP] Removed {removed} expired tickers")
            self.update_display()

    def clear_all_tickers_daily(self, dt=None):
        """Clear all tickers at 2 AM EST daily"""
        now_est = datetime.datetime.now(NY_TZ)
        
        # Check if it's 2 AM hour
        if now_est.hour == 2:
            # Clear ticker timestamp registry
            ticker_timestamp_registry.clear()
            
            # Clear all live_data channels
            for channel in self.live_data.keys():
                self.live_data[channel] = []
            
            # Clear stock news cache
            self.stock_news = {}
            
            # Refresh display
            self.refresh_data_table()
            
            print(f"[DAILY-RESET] All tickers cleared at {now_est.strftime('%I:%M %p ET')}")

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
        
        # NEWS button (left of UPDATE)
        self.news_btn = Button(text="NEWS", font_size=11, size_hint=(None, 1), width=65, background_color=(0.8, 0.4, 0.2, 1), color=(1, 1, 1, 1), bold=True)
        self.news_btn.bind(on_release=self.open_news_panel)
        header.add_widget(self.news_btn)

        # UPDATE button
        self.update_btn = Button(text="UPDATE", font_size=11, size_hint=(None, 1), width=65, background_color=(0.2, 0.4, 0.8, 1), color=(1, 1, 1, 1), bold=True)
        self.update_btn.bind(on_release=self.trigger_cache_update)
        header.add_widget(self.update_btn)

        # KIOSK button
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

    def opennewspanel(self, instance):
        print("NEWS: Manual news refresh button clicked")
        
        def manualfetchthread():
            # Disable button and change text
            Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'text', 'FETCHING...'), 0)
            Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'disabled', True), 0)
            
            if not self.marketdata.alltickers:
                print("MANUAL: No tickers loaded yet, skipping")
                Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'text', 'NEWS'), 0)
                Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'disabled', False), 0)
                return
            
            # Collect active tickers from all channels (exclude Halts)
            symbols = []
            for channelname, channelstocks in self.livedata.items():
                if channelname == "Halts":
                    continue
                for stock in channelstocks:
                    symbol = stock[0]
                    if symbol not in symbols:
                        symbols.append(symbol)
            
            if not symbols:
                print("MANUAL: No active tickers to fetch news for")
                Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'text', 'NEWS'), 0)
                Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'disabled', False), 0)
                return
            
            print(f"MANUAL: Fetching news for {len(symbols)} tickers...")
            start_time = time.time()
            
            # Fetch news for each ticker
            for i, symbol in enumerate(symbols, 1):
                if self.news_manager:
                    # Fetch from all 3 providers (no delays between providers)
                    self.news_manager.fetch_gdelt_news(symbol)
                    self.news_manager.fetch_alpaca_news(symbol)
                    self.news_manager.fetch_finnhub_news(symbol)

                    # Log progress every 10 tickers
                    if i % 10 == 0 or i == len(symbols):
                        print(f"MANUAL: {i}/{len(symbols)} - Fetched {symbol}")
                    
                    # Wait 2 seconds before next ticker
                    time.sleep(2)
            
            elapsed = time.time() - start_time
            print(f"MANUAL: Complete - {len(symbols)} tickers in {elapsed:.1f}s")
            
            # Re-enable button and reset text
            Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'text', 'NEWS'), 0)
            Clock.schedule_once(lambda dt: setattr(self.newsbtn, 'disabled', False), 0)
        
        threading.Thread(target=manualfetchthread, daemon=True).start()
    
    def manual_fetch_thread(self):
        """Background thread for manual news fetching"""
        while self.running:
            try:
                app = App.get_running_app()
                if not hasattr(app.root, 'live_data'):
                    time.sleep(5)
                    continue
                
                # Get active tickers from GUI
                active_tickers = set()
                for channel_name, channel_stocks in app.root.live_data.items():
                    for stock in channel_stocks:
                        active_tickers.add(stock[0])  # stock[0] is symbol
                
                # Fetch news for each active ticker
                for symbol in active_tickers:
                    if not self.running:
                        break
                    
                    # GDELT (if not exhausted)
                    if self.news_manager.gdelt_manual_uses < self.news_manager.gdelt_manual_limit:
                        self.news_manager.fetch_gdelt_news(symbol)
                        self.news_manager.gdelt_manual_uses += 1
                        time.sleep(0.5)
                    
                    # Alpaca
                    self.news_manager.fetch_alpaca_news(symbol)
                    time.sleep(0.5)
                    
                    # yFinance
                    self.news_manager.fetch_yfinance_news(symbol)
                    time.sleep(0.5)
                    
                    print(f"[MANUAL] Fetched {symbol} (GDELT: {self.news_manager.gdelt_manual_uses}/{self.news_manager.gdelt_manual_limit})")
                
                # Sleep before next cycle
                time.sleep(300)  # Run every 5 minutes
                
            except Exception as e:
                print(f"Manual fetch thread error: {e}")
                time.sleep(60)

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

        channels = ["PreGap", "HOD", "RunUp", "P-HOD", "P-RunUp", "Rvsl", "Halts", "BKG-News"]
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
        if self.current_channel == 'Halts':
            headers = [('SYMBOL', 0), ('TIME', 1), ('REASON', 2), ('PRICE', 3), ('%', 4), ('NEWS', 5), ('ALERT', 6)]
        else:
            headers = [("TICKER", 0), ("TIME", 1), ("PRICE", 2), ("GAP%", 3), ("CB-VOL", 4), ("VOL", 5), ("FLOAT", 6), ("RVOL", 7), ("NEWS", 8)]
        
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

    def update_indices(self, dt=None):
        self.nasdaq_last, self.nasdaq_pct = self.market_data.get_index_data("^IXIC")
        self.sp_last, self.sp_pct = self.market_data.get_index_data("^SPX")
        
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
        
        # NEWS button (left of UPDATE)
        self.news_btn = Button(text="NEWS", font_size=11, size_hint=(None, 1), width=65, background_color=(0.8, 0.4, 0.2, 1), color=(1, 1, 1, 1), bold=True)
        self.news_btn.bind(on_release=self.open_news_panel)
        header.add_widget(self.news_btn)

        # UPDATE button
        self.update_btn = Button(text="UPDATE", font_size=11, size_hint=(None, 1), width=65, background_color=(0.2, 0.4, 0.8, 1), color=(1, 1, 1, 1), bold=True)
        self.update_btn.bind(on_release=self.trigger_cache_update)
        header.add_widget(self.update_btn)

        # KIOSK button
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
    
    def open_news_panel(self, instance):
        print("[NEWS] News panel button clicked")
        
        def manual_fetch_thread():
            if not self.market_data.all_tickers:
                print("[MANUAL] No tickers loaded yet, skipping")
                return
            
            symbols = []
            for channel_name, channel_stocks in self.live_data.items():
                if channel_name == "Halts":
                    continue
                for stock in channel_stocks:
                    symbol = stock[0]
                    if symbol not in symbols:
                        symbols.append(symbol)
            
            if not symbols:
                print("[MANUAL] No active tickers to fetch news for")
                return
            
            print(f"[MANUAL] Fetching news for {len(symbols)} tickers...")
            
            for i, symbol in enumerate(symbols, 1):
                if self.news_manager:
                    self.news_manager.fetch_news_pair(symbol)
                    time.sleep(0.5)
                    self.news_manager.fetch_yfinance_news(symbol)
                    time.sleep(0.5)
                    print(f"[MANUAL] {i}/{len(symbols)}: Fetched {symbol}")
                    
                    if self.news_manager.gdelt_manual_uses < self.news_manager.gdelt_manual_limit:
                        self.news_manager.fetch_gdelt_news(symbol)
                        self.news_manager.gdelt_manual_uses += 1
                        time.sleep(0.5)
                        print(f"[MANUAL] Fetched {symbol} GDELT")
                    
                    self.news_manager.fetch_alpaca_news(symbol)
                    time.sleep(0.5)
        
        threading.Thread(target=manual_fetch_thread, daemon=True).start()

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

        channels = ["PreGap", "HOD", "RunUp", "P-HOD", "P-RunUp", "Rvsl", "Halts", "BKG-News"]
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
            headers = [("SYMBOL", 0), ("TIME", 1), ("REASON", 2), ("PRICE", 3), ("%", 4), ("NEWS", 5), ("ALERT", 6)]
        else:
            headers = [("TICKER", 0), ("TIME", 1), ("PRICE", 2), ("GAP%", 3), ("CB-VOL", 4), ("VOL", 5), ("FLOAT", 6), ("RVOL", 7), ("NEWS", 8)]
        
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
        # Handle CBVOL (4) and VOL (5) columns with B/M/K formatting
        if column_index in [4, 5]:
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
        
        # Handle numeric values (int/float)
        if isinstance(value, (int, float)):
            return value
        
        # Handle all other columns (remove special characters and parse)
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
                    stocks.sort(key=lambda x: ticker_timestamp_registry.get(x[0], {}).get('datetime', datetime.datetime.min))
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
        
        # Column 1: SYMBOL (width 0.12)
        row.add_widget(Label(text=str(halt_data[0]), font_size=12, color=(1, 0.3, 0.3, 1), size_hint=(0.10, 1)))
        
        # Column 2: TIME (width 0.18)
        row.add_widget(Label(text=str(halt_data[1]), font_size=12, color=(0.9, 0.9, 0.9, 1), size_hint=(0.10, 1)))
        
        # Column 3: REASON (width 0.25)
        row.add_widget(Label(text=str(halt_data[2]), font_size=12, color=(0.9, 0.9, 0.9, 1), size_hint=(0.10, 1)))
        
        # Column 4: PRICE (width 0.12)
        row.add_widget(Label(text='N/A', font_size=12, color=(0.9, 0.9, 0.9, 1), size_hint=(0.10, 1)))
        
        # Column 5: % (width 0.08)
        row.add_widget(Label(text='N/A', font_size=12, color=(0.9, 0.9, 0.9, 1), size_hint=(0.10, 1)))
        
        # Column 6: NEWS (width 0.15)
        if ticker in self.stock_news:
            tier = self.stock_news[ticker].get('tier', 3)
            if tier == 2:
                btn_color = (0, 0, 0.5, 1)
                btn_text = "BREAK"
                btn_text_color = (1, 1, 1, 1)
            else:
                btn_color = (1, 1, 0, 1)
                btn_text = "NEWS"
                btn_text_color = (0, 0, 0, 1)
        else:
            btn_color = (0, 0, 0, 1)
            btn_text = "NONE"
            btn_text_color = (1, 1, 1, 1)
        
        btn = Button(text=btn_text, font_size=11, size_hint=(0.10, 1), background_color=btn_color, 
                     background_normal='', bold=True, color=btn_text_color)
        news_content = self.stock_news.get(ticker, {}).get('title', 'No news available')
        btn.bind(on_release=lambda x, t=ticker, n=news_content: self.show_news_popup(t, n))
        row.add_widget(btn)
        
        # Column 7: ALERT button (width 0.10)
        # ALERT button (Column 7) - Toggle resumption alerts
        reason = halt_data[2] if len(halt_data) > 2 else "N/A"
        alert_key = f"{ticker}:{reason}"
        
        if alert_key in halt_resumption_alerts:
            alert_btn = Button(
                text='ON',
                font_size=11,
                size_hint=(0.10, 1),
                background_color=(0, 1, 0, 1),
                background_normal='',
                bold=True,
                color=(0, 0, 0, 1)
            )
        else:
            alert_btn = Button(
                text='NONE',
                font_size=11,
                size_hint=(0.10, 1),
                background_color=(0, 0, 0, 1),
                background_normal='',
                bold=True,
                color=(1, 1, 1, 1)
            )
        
        alert_btn.bind(on_release=lambda x, t=ticker, r=reason: self.toggle_halt_alert(t, r))
        row.add_widget(alert_btn)
        
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
            if i == 8:
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
                row.add_widget(btn)
        
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
        
        # Deep Check button (Perplexity) - MOVED OUT OF IF BLOCK
        deep_btn = Button(
            text="🔍 Deep Check (Perplexity)",
            size_hint=(1, 0.1),
            background_color=(0.2, 0.6, 1, 1),
            color=(1, 1, 1, 1),
            bold=True
        )
        deep_btn.bind(on_release=lambda x: self.show_perplexity_deep_check(ticker))
        content.add_widget(deep_btn)
        
        # Usage stats
        if hasattr(self, 'perplexity_manager'):
            stats = self.perplexity_manager.get_usage_stats()
            usage_label = Label(
                text=f"Perplexity: ${stats['spent']:.2f}/${stats['limit']:.2f} | ~{stats['queries_remaining_estimate']} queries left",
                size_hint=(1, 0.06),
                font_size='11sp',
                color=(0.7, 0.7, 0.7, 1)
            )
            content.add_widget(usage_label)

        close_btn = Button(text="Close", size_hint=(1, 0.1))
        content.add_widget(close_btn)
        
        popup = Popup(title=f"{ticker}", content=content, size_hint=(0.8, 0.6))
        close_btn.bind(on_release=popup.dismiss)

        popup.open()

    def show_perplexity_deep_check(self, ticker):
        """Show Perplexity deep news check in a popup"""
        
        if not hasattr(self, 'perplexity_manager'):
            popup = Popup(
                title="Not Available",
                content=Label(text="Perplexity Manager not initialized"),
                size_hint=(0.5, 0.3)
            )
            popup.open()
            return
        
        # Check if can query
        if not self.perplexity_manager.can_query():
            stats = self.perplexity_manager.get_usage_stats()
            popup = Popup(
                title="Budget Limit Reached",
                content=Label(text=f"Monthly limit reached\n${stats['spent']:.2f}/${stats['limit']:.2f} used"),
                size_hint=(0.6, 0.3)
            )
            popup.open()
            return
        
        # Show loading popup
        loading_label = Label(text=f"Analyzing {ticker} with Perplexity...\n\nThis may take 5-10 seconds")
        loading_popup = Popup(
            title="Deep News Check",
            content=loading_label,
            size_hint=(0.7, 0.4),
            auto_dismiss=False
        )
        loading_popup.open()
        
        # Perform query in background thread
        def query_thread():
            success, news_text, cost = self.perplexity_manager.get_deep_news(ticker)
            
            # Close loading popup and show results on main thread
            Clock.schedule_once(lambda dt: loading_popup.dismiss(), 0)
            Clock.schedule_once(lambda dt: self.display_perplexity_results(ticker, success, news_text, cost), 0.1)
        
        threading.Thread(target=query_thread, daemon=True).start()

    def display_perplexity_results(self, ticker, success, news_text, cost):
        """Display Perplexity deep check results"""
        
        layout = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        # Title
        title = Label(
            text=f"Deep News Analysis: {ticker}",
            size_hint=(1, None),
            height=40,
            font_size='18sp',
            bold=True
        )
        layout.add_widget(title)
        
        # Results in scrollview
        scroll = ScrollView(size_hint=(1, 1))
        result_label = Label(
            text=news_text if success else f"Error: {news_text}",
            size_hint=(1, None),
            markup=True,
            text_size=(700, None),
            halign='left',
            valign='top',
            padding=(10, 10)
        )
        result_label.bind(texture_size=result_label.setter('size'))
        scroll.add_widget(result_label)
        layout.add_widget(scroll)
        
        # Cost and usage info
        stats = self.perplexity_manager.get_usage_stats()
        info_text = f"Cost: ${cost:.4f} | Monthly: ${stats['spent']:.2f}/${stats['limit']:.2f} ({stats['percent_used']:.1f}%) | Queries: {stats['query_count']}"
        info_label = Label(
            text=info_text,
            size_hint=(1, None),
            height=30,
            font_size='12sp',
            color=(0.7, 0.7, 0.7, 1)
        )
        layout.add_widget(info_label)
        
        # Close button
        close_btn = Button(text="Close", size_hint=(1, None), height=40)
        layout.add_widget(close_btn)
        
        popup = Popup(
            title="Perplexity Deep Check Results",
            content=layout,
            size_hint=(0.85, 0.85)
        )
        close_btn.bind(on_press=popup.dismiss)
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
        
        self.local_time_label.text = f"Local Time: {local_hour}:{local_time.strftime('%M:%S')} {local_ampm}"
        self.nyc_time_label.text = f"NYC Time: {nyc_hour}:{nyc_time.strftime('%M:%S')} {nyc_ampm}"
        
        state, color = self.get_market_state_and_color(nyc_time)
        self.market_state_label.text = state
        self.market_state_label.color = color
        
        countdown = self.get_countdown(nyc_time)
        self.countdown_label.text = countdown
        self.countdown_label.color = color
        
        self.sound_manager.check_market_bells(nyc_time)

    def check_midnight_reset(self, dt=None):
        """Reset all ticker rows at midnight EST"""
        now_est = datetime.datetime.now(NY_TZ)
        
        # Check if it's midnight EST (12:00 AM)
        if now_est.hour == 0 and now_est.minute == 0:
            # Clear ticker timestamp registry
            ticker_timestamp_registry.clear()
            
            # Clear all live data tabs
            for channel in self.root.live_data:
                self.root.live_data[channel] = []
            
            # Refresh display
            self.refresh_data_table()
            self.candidate_alerted.clear()
            alerted_count = len(self.candidate_alerted)
            print(f"[RESET] Midnight EST reset complete - {alerted_count} candidate alerts cleared, all ticker rows cleared")

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
            if i == 8:
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
        
        # Deep Check button (Perplexity)
        deep_btn = Button(
            text="🔍 Deep Check (Perplexity)",
            size_hint=(1, 0.1),
            background_color=(0.2, 0.6, 1, 1),
            color=(1, 1, 1, 1),
            bold=True
        )
        deep_btn.bind(on_release=lambda x: self.show_perplexity_deep_check(ticker))
        content.add_widget(deep_btn)
        
        # Usage stats
        if hasattr(self, 'perplexity_manager'):
            stats = self.perplexity_manager.get_usage_stats()
            usage_label = Label(
                text=f"Perplexity: ${stats['spent']:.2f}/${stats['limit']:.2f} | ~{stats['queries_remaining_estimate']} queries left",
                size_hint=(1, 0.06),
                font_size='11sp',
                color=(0.7, 0.7, 0.7, 1)
            )
            content.add_widget(usage_label)
        
        close_btn = Button(text="Close", size_hint=(1, 0.1))
        content.add_widget(close_btn)
        
        popup = Popup(title=f"{ticker}", content=content, size_hint=(0.8, 0.6))
        close_btn.bind(on_release=popup.dismiss)
        popup.open()

    def get_market_state_and_color(self, now_est):
        day_of_week = now_est.weekday()
        if day_of_week >= 5:
            return "Weekend", (1, 0.6, 0, 1)
        
        if now_est.hour < 4:
            return "Closed", (1, 0.6, 0, 1)
        elif now_est.hour >= 4 and (now_est.hour < 9 or (now_est.hour == 9 and now_est.minute < 30)):
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

if __name__ == "__main__":
    # MANUAL PIPELINE TEST HARNESS

    # Sample universe for test: swap your preferred tickers here
    universe = ["AAPL", "MSFT", "TSLA", "AMD"]

    # 1. Test yfinance bulk download
    candidates = yfinance_bulk_download(universe)
    print("\n[YFINANCE] Candidates:")
    for c in candidates:
        print(f"{c['symbol']} gap: {c['gap_percent']:.2f}% price: {c['current_price']} float: {c['float']}")

    # 2. Test Alpaca validation (see console output for validation results)
    def validation_callback(symbol, alpaca_price, variance):
        print(f"[ALPACA CHECK] {symbol}: Alpaca {alpaca_price} | yfinance {next((c['current_price'] for c in candidates if c['symbol'] == symbol), '?')} | Variance: {variance:.2f}%")
    start_alpaca_websocket_validate(candidates, validation_callback)

    # 3. Test Tradier session and WebSocket
    tid = get_tradier_session_id()
    if tid:
        def tradier_update_cb(symbol, data):
            print(f"[TRADIER WSS] {symbol}: {data}")
        start_tradier_websocket([c['symbol'] for c in candidates], tid, tradier_update_cb)
    else:
        print("[TRADIER] Unable to establish WebSocket session.")

    # 4. Test Tradier REST manual update
    updated = tradier_manual_update([c['symbol'] for c in candidates])
    print("\n[TRADIER REST] Manual update:")
    for s, d in updated.items():
        print(f"{s}: last {d['last']}, bid {d['bid']}, ask {d['ask']}, vol {d['volume']}")
