import os
import pandas as pd
import requests
import numpy as np
import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
import sqlite3
from sqlite3 import Error
import random
import argparse
import time
import threading
from websocket import create_connection, WebSocketConnectionClosedException
from datetime import datetime
import queue
from config import *

# Глобальный цикл событий
loop = asyncio.get_event_loop()


# Настройка логгирования
def setup_logger():
    logger = logging.getLogger('trading_bot')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s %(threadName)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S'
    )

    file_handler = RotatingFileHandler(
        'trading_bot.log',
        maxBytes=1024 * 1024 * 5,
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.stream.reconfigure(encoding='utf-8', errors='replace')

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


logger = setup_logger()


class CustomWebSocket:
    def __init__(self, symbols: list, callback, loop):
        self.symbols = symbols  # ['BTC-USDT']
        self.callback = callback
        self.loop = loop
        self._running = False
        self._ws = None
        self._lock = threading.Lock()
        self.inst_ids = [
            f"{sym}-SWAP" if INSTRUMENT_TYPE == "SWAP" else sym
            for sym in self.symbols
        ]
        now = time.time()
        self.last_data_time = {inst_id: now for inst_id in self.inst_ids}
        self._connection_thread = None
        self._monitor_thread = None
        self._ping_thread = None
        self._connect_lock = threading.Lock()
        self._reconnect_requested = False
        self._backoff_seconds = 1
        self.message_queue = queue.Queue()
        self._processing_thread = None

    def _connect(self):
        while self._running:
            with self._connect_lock:
                try:
                    logger.info("🔄 Начало процедуры подключения к WebSocket OKX...")
                    logger.debug(f"Параметры подключения: URL=wss://ws.okx.com:8443/ws/v5/public, timeout=30")
                    connect_start = time.time()
                    self._ws = create_connection(
                        "wss://ws.okx.com:8443/ws/v5/public",
                        timeout=30
                    )
                    connect_time = time.time() - connect_start
                    logger.info(f"✅ WebSocket подключен за {connect_time:.2f} сек")
                    logger.debug(f"Состояние сокета: sock={self._ws.sock}")
                    sub_args = [{"channel": "tickers", "instId": inst_id} for inst_id in self.inst_ids]
                    sub_msg = {"op": "subscribe", "args": sub_args}
                    send_start = time.time()
                    self._safe_send(sub_msg)
                    logger.info(f"📩 Отправлен запрос подписки на {len(self.inst_ids)} инструментов")
                    now = time.time()
                    for inst_id in self.inst_ids:
                        self.last_data_time[inst_id] = now
                    logger.debug(f"Сброшены last_data_time: {self.last_data_time}")
                    read_attempts = 0
                    while self._running:
                        try:
                            read_attempts += 1
                            recv_start = time.time()
                            message = self._ws.recv()
                            recv_time = time.time() - recv_start
                            if message == "pong":
                                logger.debug("🏓 Получен pong-ответ")
                                continue
                            parse_start = time.time()
                            try:
                                data = json.loads(message)
                            except json.JSONDecodeError as e:
                                logger.error(f"❌ Ошибка парсинга JSON: {e}")
                                logger.debug(f"Проблемное сообщение: {message}")
                                continue
                            if data.get("event") == "error":
                                error_msg = f"⛔ Ошибка подпики: {data.get('msg')} (код: {data.get('code')})"
                                logger.error(error_msg)
                                logger.debug(f"Полное сообщение об ошибке: {data}")
                                self._request_reconnect()
                                break
                            if data.get("event") == "subscribe":
                                logger.info(f"✅ Успешная подписка: {data.get('arg')}")
                                logger.debug(f"Подробности подписки: {data}")
                                continue
                            process_start = time.time()
                            self._handle_ws_message(data)
                        except WebSocketConnectionClosedException:
                            logger.error("‼️ Соединение WebSocket неожиданно закрыто")
                            logger.debug("Трассировка последних операций:", exc_info=True)
                            break
                        except Exception as e:
                            logger.error(f"⚠️ Неожиданная ошибка обработки: {str(e)}")
                            logger.debug("Подробности ошибки:", exc_info=True)
                            time.sleep(0.5)
                except Exception as e:
                    logger.error(f"🔥 Критическая ошибка подключения: {str(e)}")
                    logger.debug("Полная трассировка:", exc_info=True)
                    logger.debug(f"Состояние WebSocket: {self._ws}")
                    logger.debug(f"Поток alive: {threading.current_thread().is_alive()}")
                finally:
                    logger.info("🛑 Завершение соединения...")
                    self._safe_close()
                    logger.debug("Соединение закрыто")
            if not self._running:
                logger.info("🛑 Флаг _running=False, завершение потока")
                break
            delay = min(self._backoff_seconds, 30)
            logger.warning(f"⏳ Повторная попытка через {delay} сек (backoff: {self._backoff_seconds})")
            time.sleep(delay)
            self._backoff_seconds = min(self._backoff_seconds * 2, 30)

    def _safe_send(self, obj):
        try:
            if self._ws:
                if isinstance(obj, str):
                    self._ws.send(obj)
                else:
                    self._ws.send(json.dumps(obj))
        except Exception as e:
            logger.error(f"Ошибка отправки: {str(e)}")

    def _safe_close(self):
        try:
            if self._ws:
                self._ws.close()
        except:
            pass
        finally:
            self._ws = None

    def _handle_ws_message(self, data: dict):
        if "data" not in data or "arg" not in data:
            logger.warning("⚠️ Неожиданный формат сообщения без data/arg")
            return
        inst_id = data["arg"].get("instId")
        if not inst_id:
            logger.warning("⚠️ Сообщение без instId")
            return
        try:
            tick_data = data['data'][0]
            #logger.info(f"📊 Тик {inst_id}: цена={tick_data['last']} объём={tick_data.get('lastSz', 'N/A')}")
            with self._lock:
                prev_time = self.last_data_time.get(inst_id, 0)
                self.last_data_time[inst_id] = time.time()

            # Помещаем сообщение в очередь для обработки
            self.message_queue.put(data)

        except Exception as e:
            logger.error(f"💥 Ошибка обработки тика: {str(e)}")
            logger.debug("Стек вызовов:", exc_info=True)

    def _process_messages(self):
        """Поток для обработки сообщений из очереди"""
        while self._running:
            try:
                data = self.message_queue.get(timeout=1)
                if self.callback:
                    # Создаем новый event loop для обработки в этом потоке
                    try:
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        new_loop.run_until_complete(self.callback(data))
                    except Exception as e:
                        logger.error(f"Ошибка обработки сообщения: {str(e)}")
                    finally:
                        new_loop.close()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Ошибка в потоке обработки: {str(e)}")

    def _monitor_connection(self):
        while self._running:
            try:
                now = time.time()
                stale = [iid for iid, t in self.last_data_time.items() if (now - t) > 30]
                if stale:
                    logger.warning(f"Нет данных по {len(stale)} парам более 30 секунд")
                    self._request_reconnect()
                time.sleep(10)
            except Exception as e:
                logger.error(f"Ошибка мониторинга соединения: {str(e)}")
                time.sleep(2)

    def _ping_loop(self):
        while self._running:
            try:
                if self._ws:
                    self._safe_send("ping")
            except Exception as e:
                logger.error(f"Ошибка отправки ping: {str(e)}")
            time.sleep(20)

    def _request_reconnect(self):
        if self._reconnect_requested:
            return
        self._reconnect_requested = True
        self._safe_close()

    def start(self):
        if self._running:
            return
        self._running = True
        self._connection_thread = threading.Thread(target=self._connect, daemon=True)
        self._connection_thread.start()
        self._monitor_thread = threading.Thread(target=self._monitor_connection, daemon=True)
        self._monitor_thread.start()
        self._ping_thread = threading.Thread(target=self._ping_loop, daemon=True)
        self._ping_thread.start()
        self._processing_thread = threading.Thread(target=self._process_messages, daemon=True)
        self._processing_thread.start()

    def stop(self):
        self._running = False
        self._reconnect_requested = False
        self._safe_close()


class DatabaseManager:
    def __init__(self, db_file):
        logger.info("🛢️ Инициализация DatabaseManager...")
        self.db_file = db_file
        self.conn = None
        self._initialize_db()

    def _initialize_db(self):
        try:
            logger.debug("🔧 Создание/подключение к БД...")
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            tables = {
                'positions': "📊 Таблица позиций",
                'trades': "💱 Таблица сделок",
                'fiveminute_data': "⏱️ Таблица минутных данных"
            }
            for table, log_msg in tables.items():
                logger.debug(f"{log_msg}...")
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS positions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        entry_price REAL NOT NULL,
                        tp_price REAL NOT NULL,
                        sl_price REAL NOT NULL,
                        size REAL NOT NULL,
                        leverage INTEGER NOT NULL,
                        entry_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        exit_price REAL,
                        exit_time TIMESTAMP,
                        pnl REAL,
                        absolute_pnl REAL,
                        status TEXT DEFAULT 'open',
                        close_reason TEXT
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        fee REAL NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        position_id INTEGER,
                        FOREIGN KEY (position_id) REFERENCES positions (id)
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS fiveminute_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume REAL NOT NULL,
                        rsi REAL,
                        macd REAL,
                        macd_signal REAL,
                        macd_hist REAL,
                        UNIQUE(symbol, timestamp)
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS current_indicators (
                        symbol TEXT NOT NULL,
                        tick_count INTEGER NOT NULL,
                        price REAL NOT NULL,
                        rsi REAL,
                        macd REAL,
                        macd_signal REAL,
                        macd_hist REAL,
                        updated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (symbol, tick_count)
                    )
                ''')
            self.conn.commit()
            logger.info("✅ База данных успешно инициализирована")
        except Error as e:
            logger.error(f"💥 Критическая ошибка БД: {str(e)}")
            raise

    def execute_query(self, query, params=None):
        logger.debug(f"📝 Выполнение запроса: {query[:50]}...")
        try:
            cursor = self.conn.cursor()
            start_time = time.time()
            if params:
                logger.debug(f"↪️ Параметры: {params}")
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            exec_time = time.time() - start_time
            self.conn.commit()
            logger.debug(f"✅ Запрос выполнен за {exec_time:.3f} сек")
            return cursor
        except Error as e:
            logger.error(f"❌ Ошибка запроса: {str(e)}")
            logger.debug(f"💾 Проблемный запрос: {query}")
            if params:
                logger.debug(f"💾 Параметры: {params}")
            self.conn.rollback()
            raise

    def get_open_positions(self, symbol=None):
        query = "SELECT * FROM positions WHERE status = 'open'"
        params = () if not symbol else (symbol,)
        if symbol:
            query += " AND symbol = ?"
        cursor = self.execute_query(query, params)
        return cursor.fetchall()

    def save_position(self, position_data):
        query = '''
            INSERT INTO positions 
            (symbol, side, entry_price, tp_price, sl_price, size, leverage)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        params = (
            position_data['symbol'],
            position_data['side'],
            position_data['entry_price'],
            position_data['tp_price'],
            position_data['sl_price'],
            position_data['size'],
            position_data['leverage']
        )
        cursor = self.execute_query(query, params)
        return cursor.lastrowid

    def close_position(self, position_id, exit_price, close_reason):
        cursor = self.execute_query("SELECT * FROM positions WHERE id = ?", (position_id,))
        position = cursor.fetchone()
        if not position:
            logger.error(f"Позиция с ID {position_id} не найдена")
            return False
        entry_price = position[3]
        size = position[6]
        leverage = position[7]
        if position[2] == 'buy':
            pnl = (exit_price - entry_price) / entry_price
        else:
            pnl = (entry_price - exit_price) / entry_price
        pnl *= leverage
        pnl -= 2 * FEE_RATE
        absolute_pnl = pnl * size
        query = '''
            UPDATE positions 
            SET exit_price = ?,
                exit_time = CURRENT_TIMESTAMP,
                pnl = ?,
                absolute_pnl = ?,
                status = 'closed',
                close_reason = ?
            WHERE id = ?
        '''
        params = (exit_price, pnl, absolute_pnl, close_reason, position_id)
        self.execute_query(query, params)
        return True

    def save_minute_data(self, symbol, minute_data):
        query = '''
            INSERT OR IGNORE INTO fiveminute_data 
            (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        params = (
            symbol,
            datetime.fromtimestamp(minute_data['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
            minute_data['open'],
            minute_data['high'],
            minute_data['low'],
            minute_data['close'],
            minute_data['volume']
        )
        self.execute_query(query, params)
        return True

    def update_indicators(self, symbol, timestamp, indicators):
        try:
            query = '''
                UPDATE fiveminute_data 
                SET rsi = ?, macd = ?, macd_signal = ?, macd_hist = ?
                WHERE symbol = ? AND timestamp = ?
            '''
            # Преобразуем timestamp в правильный формат
            timestamp_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

            params = (
                indicators['rsi'],
                indicators['macd'],
                indicators['macd_signal'],
                indicators['macd_hist'],
                symbol,
                timestamp_str
            )
            self.execute_query(query, params)
            logger.debug(f"✅ Обновлены индикаторы для {symbol} на {timestamp_str}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка обновления индикаторов: {str(e)}")
            return False

    def get_last_minute(self, symbol):
        query = '''
            SELECT * FROM fiveminute_data 
            WHERE symbol = ? 
            ORDER BY timestamp DESC 
            LIMIT 1
        '''
        cursor = self.execute_query(query, (symbol,))
        return cursor.fetchone()

    def get_minutes_for_indicators(self, symbol, limit):
        query = '''
            SELECT timestamp, close 
            FROM fiveminute_data 
            WHERE symbol = ? 
            ORDER BY timestamp DESC 
            LIMit ?
        '''
        cursor = self.execute_query(query, (symbol, limit))
        return cursor.fetchall()


    def __del__(self):
        if self.conn:
            self.conn.close()


class TradingBot:
    def __init__(self, initial_balance=INITIAL_BALANCE):
        logger.info("🤖 Инициализация TradingBot...")
        self.db = DatabaseManager(DB_FILE)
        self.data = {}
        self.tick_buffer = {}
        self.current_candle = {}
        self.last_minute_time = {}
        self.tick_count = {}  # Счетчик тиков
        self.balance = initial_balance
        self.ws = None
        self.symbols = SYMBOLS  # ['BTC-USDT']
        self.last_macd = {}
        self.last_signal = {}
        self._tick_lock = threading.Lock()  # Блокировка для счетчика тиков

        for symbol in self.symbols:
            logger.debug(f"📊 Подготовка структур данных для {symbol}...")
            self.data[symbol] = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            self.tick_buffer[symbol] = []
            self.current_candle[symbol] = None
            self.last_minute_time[symbol] = 0
            self.tick_count[symbol] = 0
            self.last_macd[symbol] = None  # вместо 0
            self.last_signal[symbol] = None  # вместо None

        logger.info(f"💰 Бот инициализирован с балансом: {self.balance:.2f} USDT")

    def get_account_balance(self):
        logger.info(f"Текущий баланс: {self.balance:.2f} USDT")
        return self.balance

    def get_random_slippage(self):
        if random.random() < SLIPPAGE_PROBABILITY:
            slippage = random.uniform(-SLIPPAGE_RANGE, SLIPPAGE_RANGE)
            logger.info(f"Применено проскальзывание: {slippage * 100:.4f}%")
            return 1 + slippage
        return 1.0

    def calculate_rsi(self, closes, period=RSI_PERIOD):
        """
        Реализация RSI как в TradingView (Wilder's Smoothing)
        """
        try:
            # Конвертируем в numpy array для точности
            prices = closes.values if isinstance(closes, pd.Series) else np.array(closes)

            if len(prices) < period + 1:
                return None

            deltas = np.diff(prices)

            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)

            # Первые значения
            avg_gain = np.mean(gains[:period])
            avg_loss = np.mean(losses[:period])

            if avg_loss == 0:
                return 100 if avg_gain > 0 else 50

            # Wilder's smoothing
            for i in range(period, len(gains)):
                avg_gain = (avg_gain * (period - 1) + gains[i]) / period
                avg_loss = (avg_loss * (period - 1) + losses[i]) / period

            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            return rsi

        except Exception as e:
            logger.error(f"Ошибка расчета RSI: {str(e)}")
            return None

    def calculate_atr(self, symbol):
        """
        Расчет ATR на основе последней закрытой свечи (период = ATR_PERIOD = True Range).
        """
        try:
            # Получаем последнюю закрытую свечу из self.data[symbol]
            if self.data[symbol].empty or len(self.data[symbol]) < 1:
                logger.warning(f"⚠️ Нет данных о закрытых свечах для {symbol}")
                return None

            last_closed_candle = self.data[symbol].iloc[-1]
            true_range = last_closed_candle['high'] - last_closed_candle['low']

            logger.debug(
                f"📊 Расчет ATR для {symbol}: High={last_closed_candle['high']:.2f}, Low={last_closed_candle['low']:.2f}, True Range={true_range:.4f}")
            return true_range

        except Exception as e:
            logger.error(f"Ошибка расчета ATR: {str(e)}")
            return None

    def calculate_macd(self, closes, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
        """
        Реализация MACD как в TradingView
        """
        try:
            prices = closes.values if isinstance(closes, pd.Series) else np.array(closes)

            # Нужно больше данных для расчета всех EMA
            if len(prices) < slow + signal + 10:  # Добавляем буфер
                return None, None, None

            # EMA расчет как в TradingView
            def calculate_ema(data, period):
                k = 2.0 / (period + 1.0)
                ema_values = [data[0]]
                for i in range(1, len(data)):
                    ema_values.append(data[i] * k + ema_values[-1] * (1 - k))
                return np.array(ema_values)

            # Рассчитываем EMA
            ema_fast = calculate_ema(prices, fast)
            ema_slow = calculate_ema(prices, slow)

            # MACD линия (разница быстрой и медленной EMA)
            # Берем только ту часть, где обе EMA существуют
            min_length = min(len(ema_fast), len(ema_slow))
            macd_line = ema_fast[-min_length:] - ema_slow[-min_length:]

            # Signal line (EMA от MACD линии)
            signal_line = calculate_ema(macd_line, signal)

            # Histogram (разница MACD и Signal)
            histogram = macd_line[-len(signal_line):] - signal_line

            return macd_line[-1], signal_line[-1], histogram[-1]

        except Exception as e:
            logger.error(f"Ошибка расчета MACD: {str(e)}")
            return None, None, None

    async def handle_tick(self, message):
        try:
            inst_id = message['arg']['instId']
            tick = message['data'][0]
            logger.debug(f"📥 Получен тик: inst_id={inst_id}, price={tick['last']}, timestamp={tick['ts']}")

            if inst_id not in self.symbols and inst_id.replace('-SWAP', '') not in self.symbols:
                logger.warning(f"⚠️ Получен тик для неизвестного символа: {inst_id}")
                return

            symbol = inst_id.replace('-SWAP', '') if INSTRUMENT_TYPE == "SWAP" else inst_id

            timestamp = int(tick['ts']) // 1000
            price = float(tick['last'])
            volume = float(tick['lastSz'])

            self.tick_buffer[symbol].append({'timestamp': timestamp, 'price': price, 'volume': volume})
            if len(self.tick_buffer[symbol]) > 200:
                self.tick_buffer[symbol] = self.tick_buffer[symbol][-200:]

            if TIMEFRAME == "5m":
                candle_interval = 300
            else:
                logger.error(f"❌ Неподдерживаемый TIMEFRAME: {TIMEFRAME}")
                return
            dt = datetime.fromtimestamp(timestamp)
            minute = (dt.minute // 5) * 5
            current_candle_start = int(datetime(dt.year, dt.month, dt.day, dt.hour, minute).timestamp())

            if self.current_candle[symbol] is None or current_candle_start > self.last_minute_time[symbol]:
                if self.current_candle[symbol] is not None:
                    prev_candle = self.current_candle[symbol]
                    self.data[symbol] = pd.concat([self.data[symbol], pd.DataFrame([prev_candle])],
                                                  ignore_index=True)
                    self.db.save_minute_data(symbol, prev_candle)
                    logger.info(f"🕯️ Закрыта свеча для {symbol} на {datetime.fromtimestamp(prev_candle['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}")
                    await self.calculate_and_update_indicators_for_closed_candle(symbol, prev_candle['timestamp'])
                self.last_minute_time[symbol] = current_candle_start
                self.current_candle[symbol] = {
                    'timestamp': current_candle_start,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume
                }
                logger.info(f"🕯️ Начата новая свеча для {symbol} на {datetime.fromtimestamp(current_candle_start).strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                candle = self.current_candle[symbol]
                candle['high'] = max(candle['high'], price)
                candle['low'] = min(candle['low'], price)
                candle['close'] = price
                candle['volume'] += volume
                logger.debug(
                    f"📊 Обновлена свеча {symbol}: H={candle['high']:.2f}, L={candle['low']:.2f}, C={candle['close']:.2f}")

            # Увеличиваем счетчик тиков с блокировкой
            with self._tick_lock:
                self.tick_count[symbol] += 1
                current_tick_count = self.tick_count[symbol]

            logger.debug(f"📈 Тик #{current_tick_count} для {symbol}")

            # Обрабатываем индикаторы каждые 50 тиков
            if current_tick_count % 25 == 0:
                logger.info(f"🔍 Расчет индикаторов для {symbol} (тик #{current_tick_count})")
                await self.process_indicators(symbol)

            await self.monitor_positions(symbol, price)

        except Exception as e:
            logger.error(f"💣 Ошибка обработки тика: {str(e)}")

    async def calculate_and_update_indicators_for_closed_candle(self, symbol, candle_timestamp):
        """Рассчитывает и обновляет индикаторы для закрытой свечи"""
        try:
            logger.info(f"📊 Расчет индикаторов для закрытой свечи {symbol}")

            # Получаем данные для расчета (последние N свечей включая закрытую)
            min_data_required = MACD_SLOW + MACD_SIGNAL + 20
            cursor = self.db.execute_query(
                "SELECT timestamp, close FROM fiveminute_data WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?",
                (symbol, min_data_required)
            )
            data = cursor.fetchall()

            if len(data) < min_data_required:
                logger.warning(f"⚠️ Недостаточно данных для расчета индикаторов: {len(data)} < {min_data_required}")
                return

            # Преобразуем данные
            closes = [row[1] for row in reversed(data)]  # В хронологическом порядке
            closes_series = pd.Series(closes)

            # Рассчитываем индикаторы
            rsi = self.calculate_rsi(closes_series)
            macd, signal_line, hist = self.calculate_macd(closes_series)

            if rsi is None or macd is None:
                logger.warning(f"⚠️ Не удалось рассчитать индикаторы для {symbol}")
                return

            # Обновляем индикаторы в БД
            indicators = {
                'rsi': rsi,
                'macd': macd,
                'macd_signal': signal_line,
                'macd_hist': hist
            }

            self.db.update_indicators(symbol, candle_timestamp, indicators)
            logger.info(f"✅ Обновлены индикаторы для свечи {symbol}: RSI={rsi:.2f}, MACD={macd:.4f}")

        except Exception as e:
            logger.error(f"❌ Ошибка расчета индикаторов для закрытой свечи: {str(e)}")

    async def process_indicators(self, symbol):
        logger.debug(f"🔍 Начало расчёта индикаторов для {symbol}")
        if self.data[symbol].empty:
            logger.debug(f"⚠️ Нет исторических данных для {symbol}")
            return
        if self.current_candle[symbol] is None:
            logger.debug(f"⚠️ Текущая свеча для {symbol} отсутствует")
            return

        # Получаем текущее значение счетчика
        with self._tick_lock:
            current_tick_count = self.tick_count[symbol]

        min_data_required = MACD_SLOW + MACD_SIGNAL + 20
        historical_closes = self.data[symbol]['close'].tail(min_data_required)
        logger.debug(f"🔎 Исторических свечей: {len(historical_closes)}")
        current_close = self.current_candle[symbol]['close']
        logger.debug(f"💸 Текущая цена: {current_close:.2f}")
        all_closes = pd.Series(list(historical_closes) + [current_close])
        logger.debug(f"📊 Всего точек для расчета: {len(all_closes)}")

        if len(historical_closes) < min_data_required:
            logger.debug(f"⚠️ Недостаточно исторических данных: {len(historical_closes)} < {min_data_required}")
            return

        try:
            rsi = self.calculate_rsi(all_closes)
            if rsi is None:
                logger.debug(f"⚠️ RSI не рассчитан")
                return

            macd, signal_line, hist = self.calculate_macd(all_closes)
            if macd is None:
                logger.debug(f"⚠️ MACD не рассчитан")
                return

            logger.info(
                f"📈 Индикаторы для {symbol}: RSI={rsi:.2f}, MACD={macd:.4f}, Signal={signal_line:.4f}, Hist={hist:.4f}, Price={current_close:.2f}"
            )
            logger.debug(f"📊 Условия: RSI < {RSI_BUY_THRESHOLD} = {rsi < RSI_BUY_THRESHOLD}, "
                         f"RSI > {RSI_SELL_THRESHOLD} = {rsi > RSI_SELL_THRESHOLD}, "
                         f"MACD > Signal = {macd > signal_line}, "
                         f"MACD < Signal = {macd < signal_line}")

            # Сохранение текущих индикаторов в БД
            current_timestamp = int(time.time())
            current_minute = (current_timestamp // 60) * 60
            indicators = {
                'rsi': rsi,
                'macd': macd,
                'macd_signal': signal_line,
                'macd_hist': hist
            }
            self.save_current_indicators(symbol, current_tick_count, indicators, current_close)
            logger.debug(f"💾 Текущие индикаторы сохранены в БД (тик #{current_tick_count})")

            prev_macd = self.last_macd[symbol]

            buy_condition = (rsi < RSI_BUY_THRESHOLD and
                             macd > signal_line and
                             prev_macd is not None and
                             prev_macd <= signal_line)

            sell_condition = (rsi > RSI_SELL_THRESHOLD and
                              macd < signal_line and
                              prev_macd is not None and
                              prev_macd >= signal_line)

            logger.debug(f"🎯 ДЕТАЛЬНЫЙ АНАЛИЗ УСЛОВИЙ ДЛЯ {symbol}:")
            logger.debug(f"   Цена: {current_close:.2f}")
            logger.debug(f"   RSI: {rsi:.2f}")
            logger.debug(f"   📈 BUY УСЛОВИЯ:")
            logger.debug(f"     RSI {rsi:.2f} < {RSI_BUY_THRESHOLD} = {rsi < RSI_BUY_THRESHOLD}")
            logger.debug(f"     MACD {macd:.4f} > Signal {signal_line:.4f} = {macd > signal_line}")
            logger.debug(f"     Prev MACD {prev_macd if prev_macd is not None else 'None'} <= Signal {signal_line:.4f} = {prev_macd is not None and prev_macd <= signal_line}")
            logger.debug(f"     📊 BUY сигнал = {buy_condition}")
            logger.debug(f"   📉 SELL УСЛОВИЯ:")
            logger.debug(f"     RSI {rsi:.2f} > {RSI_SELL_THRESHOLD} = {rsi > RSI_SELL_THRESHOLD}")
            logger.debug(f"     MACD {macd:.4f} < Signal {signal_line:.4f} = {macd < signal_line}")
            logger.debug(f"     Prev MACD {prev_macd if prev_macd is not None else 'None'} >= Signal {signal_line:.4f} = {prev_macd is not None and prev_macd >= signal_line}")
            logger.debug(f"     📊 SELL сигнал = {sell_condition}")

            if prev_macd is not None:
                logger.debug(f"   🔄 ИСТОРИЯ ПЕРЕСЕЧЕНИЙ:")
                logger.debug(f"     Предыдущий MACD: {prev_macd:.4f}")
                logger.debug(f"     Текущий MACD: {macd:.4f}")
                logger.debug(f"     Сигнальная линия: {signal_line:.4f}")
                if prev_macd <= signal_line and macd > signal_line:
                    logger.info(f"     ✅ MACD ПЕРЕСЕК СИГНАЛ СНИЗУ ВВЕРХ!")
                elif prev_macd >= signal_line and macd < signal_line:
                    logger.info(f"     ✅ MACD ПЕРЕСЕК СИГНАЛ СВЕРХУ ВНИЗ!")
                else:
                    logger.debug(f"     ❌ Пересечения нет")
            else:
                logger.info(f"   ⏭️ Нет данных о предыдущем MACD")

            # Обновляем значения ПОСЛЕ проверки условий
            self.last_macd[symbol] = macd
            self.last_signal[symbol] = signal_line

            if buy_condition:
                logger.info(f"🚀 ОБНАРУЖЕН BUY СИГНАЛ ДЛЯ {symbol}!")
                logger.info(f"   RSI: {rsi:.2f} < {RSI_BUY_THRESHOLD}")
                logger.info(f"   MACD: {macd:.4f} > Signal: {signal_line:.4f}")
                logger.info(f"   Пересечение: {prev_macd if prev_macd is not None else 'None'} → {macd:.4f}")
                await self.open_position(symbol, 'buy')
            elif sell_condition:
                logger.info(f"🚀 ОБНАРУЖЕН SELL СИГНАЛ ДЛЯ {symbol}!")
                logger.info(f"   RSI: {rsi:.2f} > {RSI_SELL_THRESHOLD}")
                logger.info(f"   MACD: {macd:.4f} < Signal: {signal_line:.4f}")
                logger.info(f"   Пересечение: {prev_macd if prev_macd is not None else 'None'} → {macd:.4f}")
                await self.open_position(symbol, 'sell')
            else:
                logger.debug(f"📊 Сигналов для {symbol} нет")

        except TypeError as e:
            logger.error(f"❌ Ошибка типа при расчете индикаторов: {str(e)}")
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка при расчете индикаторов: {str(e)}")

    def save_current_indicators(self, symbol, tick_count, indicators, current_price):
        try:
            current_timestamp = int(time.time())  # timestamp в секундах
            query = '''
                INSERT OR REPLACE INTO current_indicators 
                (symbol, tick_count, price, rsi, macd, macd_signal, macd_hist, updated_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            '''
            params = (
                symbol,
                tick_count,
                current_price,
                indicators['rsi'],
                indicators['macd'],
                indicators['macd_signal'],
                indicators['macd_hist']
            )
            self.db.execute_query(query, params)
            logger.debug(f"💾 Текущие индикаторы для {symbol} сохранены/обновлены (tick_count={tick_count})")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения текущих индикаторов: {str(e)}")

    async def open_position(self, symbol, side):
        logger.info(f"🚀 Попытка открыть позицию {side} по {symbol}...")
        open_positions = self.db.get_open_positions(symbol)
        if open_positions:
            logger.warning(f"⏸ Уже есть открытая позиция по {symbol}")
            return
        try:
            current_price = self.current_candle[symbol]['close']
            slippage = self.get_random_slippage()
            adjusted_price = current_price * slippage
            balance = self.get_account_balance()
            if balance is None:
                logger.error(f"Не удалось открыть позицию по {symbol}: баланс недоступен")
                return
            position_size = balance * POSITION_SIZE_PERCENT
            leverage = random.randint(LEVERAGE_MIN, LEVERAGE_MAX)

            # Расчет ATR на основе последней закрытой свечи с учетом ATR_PERIOD
            atr = self.calculate_atr(symbol)
            if atr is None:
                logger.warning(
                    f"⚠️ Не удалось рассчитать ATR для {symbol} с периодом {ATR_PERIOD}, используются стандартные значения TP и SL")
                tp_adjustment = TAKE_PROFIT / leverage
                sl_adjustment = STOP_LOSS / leverage
            else:
                tp_adjustment = (ATR_MULTIPLIER_TP * atr) / adjusted_price
                sl_adjustment = (ATR_MULTIPLIER_SL * atr) / adjusted_price
                logger.info(
                    f"📊 ATR для {symbol} (последние {ATR_PERIOD} свечи): {atr:.4f}, TP Adjustment: {tp_adjustment:.4f}, SL Adjustment: {sl_adjustment:.4f}")

            if side == 'buy':
                tp_price = adjusted_price * (1 + tp_adjustment)
                sl_price = adjusted_price * (1 - sl_adjustment)
            else:
                tp_price = adjusted_price * (1 - tp_adjustment)
                sl_price = adjusted_price * (1 + sl_adjustment)

            position_data = {
                'symbol': symbol,
                'side': side,
                'entry_price': adjusted_price,
                'tp_price': tp_price,
                'sl_price': sl_price,
                'size': position_size,
                'leverage': leverage
            }
            position_id = self.db.save_position(position_data)
            logger.info(f"Открыта позиция {side} по {symbol}")
            logger.info(f"ID позиции: {position_id}")
            logger.info(f"Цена входа: {adjusted_price:.2f} (без проскальзывания: {current_price:.2f})")
            logger.info(f"Размер позиции: {position_size:.2f} USDT")
            logger.info(f"Плечо: {leverage}x")
            logger.info(
                f"TP: {tp_price:.2f} ({tp_adjustment * 100:.4f}%), SL: {sl_price:.2f} ({sl_adjustment * 100:.4f}%)")
            logger.info(f"✅ Успешно открыта позиция {side} по {symbol}")
            logger.info(f"📌 ID: {position_id} | Цена: {adjusted_price:.2f}")
            logger.info(f"💵 Размер: {position_size:.2f} USDT | Плечо: {leverage}x")
        except Exception as e:
            logger.error(f"Ошибка открытия позиции: {str(e)}")

    async def monitor_positions(self, symbol, last_price):
        logger.debug(f"👀 Мониторинг позиций для {symbol}...")
        open_positions = self.db.get_open_positions(symbol)
        if not open_positions:
            logger.debug(f"🔄 Нет открытых позиций по {symbol}")
            return
        slippage = self.get_random_slippage()
        adjusted_price = last_price * slippage
        for position in open_positions:
            position_id = position[0]
            side = position[2]
            tp_price = position[4]
            sl_price = position[5]
            if side == 'buy':
                if adjusted_price >= tp_price:
                    await self.close_position(position_id, adjusted_price, 'take_profit')
                elif adjusted_price <= sl_price:
                    await self.close_position(position_id, adjusted_price, 'stop_loss')
            else:
                if adjusted_price <= tp_price:
                    await self.close_position(position_id, adjusted_price, 'take_profit')
                elif adjusted_price >= sl_price:
                    await self.close_position(position_id, adjusted_price, 'stop_loss')
            logger.debug(f"🔍 Проверено {len(open_positions)} позиций")

    async def close_position(self, position_id, exit_price, close_reason):
        logger.info(f"🛑 Закрытие позиции {position_id}...")
        try:
            success = self.db.close_position(position_id, exit_price, close_reason)
            if success:
                cursor = self.db.execute_query("SELECT * FROM positions WHERE id = ?", (position_id,))
                position = cursor.fetchone()
                logger.info(f"Закрыта позиция {position_id} по {position[1]}")
                logger.info(f"Причина: {close_reason}")
                logger.info(f"Сторона: {position[2]}")
                logger.info(f"Вход: {position[3]:.2f}, Выход: {exit_price:.2f}")
                logger.info(f"PnL: {position[11]:.2%}, Абсолютный PnL: {position[12]:.2f} USDT")
                self.balance += position[12]
        except Exception as e:
            logger.error(f"Ошибка закрытия позиции: {str(e)}")

    async def connect_websocket(self):
        self.ws = CustomWebSocket(SYMBOLS, self.handle_tick, loop)
        self.ws.start()

    async def load_initial_data(self):
        min_candles_required = MACD_SLOW + MACD_SIGNAL + 30  # 16 + 7 + 30 = 53 свечи
        for symbol in self.symbols:
            try:
                logger.info(f"🔎 Проверка инструмента {symbol} на бирже OKX...")
                url_instruments = "https://www.okx.com/api/v5/public/instruments"
                params_instruments = {'instType': INSTRUMENT_TYPE}
                response = requests.get(url_instruments, params=params_instruments, timeout=10)
                response.raise_for_status()
                instruments_data = response.json()
                if instruments_data.get('code') != '0':
                    logger.error(f"❌ Ошибка API OKX при проверке инструментов: {instruments_data.get('msg')}")
                    raise Exception(f"Instrument check error: {instruments_data.get('msg')}")
                inst_ids = [inst['instId'] for inst in instruments_data.get('data', [])]
                if symbol not in inst_ids:
                    logger.error(f"❌ Инструмент {symbol} не найден в списке SPOT-инструментов OKX")
                    raise Exception(f"Instrument {symbol} not found")
                logger.info(f"✅ Инструмент {symbol} подтверждён")
                logger.info(f"📂 Загрузка последних {min_candles_required} свечей для {symbol}...")
                url = "https://www.okx.com/api/v5/market/candles"  # Изменено на /market/candles для недавних данных
                candles = []
                remaining = min_candles_required
                before = None  # Для пагинации более старых данных
                while remaining > 0:
                    params = {
                        'instId': symbol,
                        'bar': TIMEFRAME,
                        'limit': str(min(remaining, 300)),  # Макс. лимит для /candles - 300
                    }
                    if before:
                        params['before'] = before  # Запрашиваем данные раньше этого ts
                    response = requests.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                    if data.get('code') != '0':
                        logger.error(f"❌ Ошибка API OKX: {data.get('msg')}")
                        raise Exception(f"API error: {data.get('msg')}")
                    fetched_candles = data.get('data', [])
                    logger.info(f"🔎 Получено {len(fetched_candles)} свечей для {symbol}")
                    if not fetched_candles:
                        break
                    candles.extend(fetched_candles)
                    remaining -= len(fetched_candles)
                    if remaining > 0:
                        before = fetched_candles[-1][0]  # ts последней (самой старой) свечи
                if len(candles) < min_candles_required:
                    logger.warning(
                        f"⚠️ Получено только {len(candles)} свечей < {min_candles_required}, генерируем тестовые данные для дополнения")
                    current_time = int(time.time())
                    for i in range(min_candles_required - len(candles)):
                        candle = {
                            'timestamp': current_time - 300 * (i + 1),
                            'open': 50000 + random.uniform(-50, 50),
                            'high': 50050 + random.uniform(-50, 50),
                            'low': 49950 + random.uniform(-50, 50),
                            'close': 50000 + random.uniform(-50, 50),
                            'volume': random.uniform(0.1, 1.0)
                        }
                        self.db.save_minute_data(symbol, candle)
                        self.data[symbol] = pd.concat([self.data[symbol], pd.DataFrame([candle])], ignore_index=True)
                        logger.debug(
                            f"🕯️ Добавлена тестовая свеча для {symbol}: timestamp={candle['timestamp']}, close={candle['close']:.2f}")
                else:
                    df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'volCcy',
                                                        'volCcyQuote', 'confirm'])
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                    df = df.astype({
                        'timestamp': int,
                        'open': float,
                        'high': float,
                        'low': float,
                        'close': float,
                        'volume': float
                    })
                    df['timestamp'] = df['timestamp'].div(1000).astype(int)
                    df = df.sort_values('timestamp')
                    self.data[symbol] = pd.concat([self.data[symbol], df], ignore_index=True)
                    self.last_minute_time[symbol] = df['timestamp'].iloc[-1]
                    for _, row in df.iterrows():
                        candle = {
                            'timestamp': row['timestamp'],
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row['volume']
                        }
                        self.db.save_minute_data(symbol, candle)
                    # Инициализация last_macd и last_signal на основе загруженных данных
                    closes = df['close']
                    if len(closes) >= min_candles_required:
                        rsi = self.calculate_rsi(closes)
                        macd, signal_line, hist = self.calculate_macd(closes)
                        if macd is not None:
                            self.last_macd[symbol] = macd
                            self.last_signal[symbol] = signal_line
                            logger.info(
                                f"✅ Инициализированы last_macd={macd:.4f} и last_signal={signal_line:.4f} для {symbol}")
                    last_candle = df.iloc[-1]
                    current_time = int(time.time())
                    dt = datetime.fromtimestamp(current_time)
                    if TIMEFRAME == "5m":
                        minute = (dt.minute // 5) * 5
                        current_candle_start = int(datetime(dt.year, dt.month, dt.day, dt.hour, minute).timestamp())
                    self.current_candle[symbol] = {
                        'timestamp': current_candle_start,
                        'open': last_candle['close'],
                        'high': last_candle['close'],
                        'low': last_candle['close'],
                        'close': last_candle['close'],
                        'volume': 0.0
                    }
                    logger.info(
                        f"🕯️ Инициализирована текущая свеча для {symbol}: timestamp={current_candle_start}, close={last_candle['close']:.2f}")
            except Exception as e:
                logger.error(f"💥 Ошибка загрузки данных для {symbol}: {str(e)}")
                # Генерация тестовых данных в случае ошибки (как в оригинале)
                current_time = int(time.time())
                for i in range(min_candles_required):
                    candle = {
                        'timestamp': current_time - 300 * (i + 1),
                        'open': 50000 + random.uniform(-50, 50),
                        'high': 50050 + random.uniform(-50, 50),
                        'low': 49950 + random.uniform(-50, 50),
                        'close': 50000 + random.uniform(-50, 50),
                        'volume': random.uniform(0.1, 1.0)
                    }
                    self.db.save_minute_data(symbol, candle)
                    self.data[symbol] = pd.concat([self.data[symbol], pd.DataFrame([candle])], ignore_index=True)
                    logger.debug(
                        f"🕯️ Добавлена тестовая свеча для {symbol}: timestamp={candle['timestamp']}, close={candle['close']:.2f}")
                self.last_minute_time[symbol] = candle['timestamp']
                self.current_candle[symbol] = {
                    'timestamp': current_time,
                    'open': candle['close'],
                    'high': candle['close'],
                    'low': candle['close'],
                    'close': candle['close'],
                    'volume': 0.0
                }
                logger.info(
                    f"🕯️ Инициализирована текущая свеча для {symbol}: timestamp={current_time}, close={candle['close']:.2f}")

    async def run(self, duration_seconds=None):
        logger.info("🚀 Запуск основного цикла бота")
        if duration_seconds:
            logger.info(f"⏳ Бот будет работать {duration_seconds} сек")
        try:
            await self.load_initial_data()
            await asyncio.sleep(5)
            for symbol in self.symbols:
                if not self.data[symbol].empty:
                    logger.info(f"🔍 Принудительный расчет индикаторов для {symbol}")
                    await self.process_indicators(symbol)
            await self.connect_websocket()
            if duration_seconds:
                await asyncio.sleep(duration_seconds)
                logger.info("🕒 Время работы истекло")
            else:
                logger.info("♾️ Бесконечный режим работы")
                while True:
                    await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {str(e)}")
        finally:
            logger.info("🛑 Завершение работы бота...")
            if self.ws:
                self.ws.stop()
            for symbol in self.symbols:
                open_positions = self.db.get_open_positions(symbol)
                for position in open_positions:
                    try:
                        current_price = self.current_candle[symbol]['close'] if self.current_candle[symbol] else \
                            self.data[symbol]['close'].iloc[-1]
                        slippage = self.get_random_slippage()
                        adjusted_price = current_price * slippage
                        await self.close_position(position[0], adjusted_price, 'test_timeout')
                    except IndexError:
                        logger.error(f"Не удалось закрыть позицию для {symbol}: нет данных о цене")
            logger.info("👋 Бот остановлен")


if __name__ == "__main__":
    logger.info("🛠️ Инициализация парсера аргументов...")
    parser = argparse.ArgumentParser(description="Торговый бот")
    parser.add_argument('--duration', type=int, default=86400, help='Длительность теста в секундах')
    parser.add_argument('--balance', type=float, default=INITIAL_BALANCE, help='Начальный баланс в USDT')
    args = parser.parse_args()
    logger.info(f"⚙️ Параметры запуска: duration={args.duration}, balance={args.balance}")
    logger.info("🤖 Создание экземпляра бота...")
    bot = TradingBot(initial_balance=args.balance)
    try:
        asyncio.run(bot.run(duration_seconds=args.duration))
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал KeyboardInterrupt")
    except Exception as e:
        logger.error(f"💥 Необработанное исключение: {str(e)}")
    finally:
        logger.info("👋 Завершение работы программы")