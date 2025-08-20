import os
import pandas as pd
import numpy as np
from pybit.unified_trading import HTTP
import random
import time
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score
from datetime import datetime
import matplotlib.pyplot as plt

# Конфигурация
API_KEY = os.getenv('BYBIT_API_KEY', 'Fjg2TuolKIIGRn122D')
API_SECRET = os.getenv('BYBIT_API_SECRET', 'WpPEVeJAj6zSlScqx6Ro4ZadnR5oLtaevBsA')
USE_ML_MODEL = True  # Включить/выключить использование нейронной сети
STARTING_BALANCE = 1000.0  # Начальный баланс в USDT
POSITION_PERCENT = 0.02  # 2% от депозита на сделку
FIXED_POSITION_SIZE = 100  # Фиксированная сумма входа в сделку в USDT
USE_FIXED_SIZE = False  # True для фиксированной суммы, False для % от депозита
SLIPPAGE_PROBABILITY = 0.5  # Вероятность проскальзывания (30%)
SLIPPAGE_RANGE = (-0.0002, 0.0002)  # Диапазон проскальзывания: от -0.2% до +0.2%

# Список торговых пар
SYMBOLS = [
    'BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'BNBUSDT', 'SOLUSDT', 'DOGEUSDT', 'TRXUSDT',
    'ADAUSDT', 'HYPEUSDT', 'LINKUSDT', 'XLMUSDT', 'SUIUSDT', 'BCHUSDT', 'HBARUSDT',
    'AVAXUSDT', 'LTCUSDT', 'TONUSDT',
]

# Параметры стратегии
TIMEFRAME = 5  # 5-минутные свечи
HISTORY_DAYS = 1  # Количество дней исторических данных
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
LEVERAGE_RANGE = (25,40)  # Диапазон кредитного плеча
TAKE_PROFIT = 0.06  # Скорректировано для ~1% прибыли с учетом плеча
STOP_LOSS = 0.02  # 1% стоп-лосс
FEE_RATE = 0.0005  # Комиссия 0.05%


class TradingBot:
    def __init__(self):
        self.session = HTTP(
            testnet=False,
            api_key=API_KEY,
            api_secret=API_SECRET
        )
        self.model = None
        self.scaler = None

    def log(self, message):
        """Логирование с временной меткой"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    def get_random_slippage(self):
        """Генерация случайного проскальзывания с заданной вероятностью"""
        if random.random() < SLIPPAGE_PROBABILITY:
            return random.uniform(SLIPPAGE_RANGE[0], SLIPPAGE_RANGE[1])
        return 0.0

    def get_historical_data(self, symbol, days):
        """Загрузка исторических данных"""
        self.log(f"Загрузка данных для {symbol} за {days} дней...")
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = end_time - days * 24 * 60 * 60 * 1000
        all_data = []
        request_count = 0
        max_requests = 100

        while end_time > start_time and request_count < max_requests:
            try:
                self.log(f"Запрос {request_count + 1} для {symbol}, end_time: {end_time}")
                response = self.session.get_kline(
                    category="linear",
                    symbol=symbol,
                    interval=TIMEFRAME,
                    end=end_time,
                    limit=1000
                )
                request_count += 1
                self.log(f"Запрос {request_count}: {response['retMsg']}")

                if response['retMsg'] == 'OK' and response['result']['list']:
                    chunk = pd.DataFrame(response['result']['list'],
                                         columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                    chunk['timestamp'] = chunk['timestamp'].astype(int)

                    if chunk.empty:
                        self.log("Получен пустой ответ от API")
                        break

                    chunk = chunk.sort_values('timestamp')
                    earliest_timestamp = int(chunk['timestamp'].iloc[0])
                    self.log(f"Получено {len(chunk)} свечей, самая ранняя временная метка: {earliest_timestamp}")

                    if earliest_timestamp >= end_time:
                        self.log(
                            f"Зацикливание: самая ранняя временная метка ({earliest_timestamp}) не продвинулась назад")
                        break

                    all_data.append(chunk)
                    end_time = earliest_timestamp - 1
                    self.log(f"Новый end_time: {end_time}")

                else:
                    self.log(f"Ошибка API или пустой ответ: {response['retMsg']}")
                    break

                time.sleep(1)

            except Exception as e:
                self.log(f"Ошибка при загрузке данных для {symbol}: {str(e)}")
                break

        if all_data:
            df = pd.concat(all_data)
            df = df.astype(float)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.sort_values('timestamp').drop_duplicates()
            self.log(f"Загружено {len(df)} свечей для {symbol}")
            return df

        self.log(f"Данные для {symbol} не загружены")
        return pd.DataFrame()

    def calculate_indicators(self, df):
        """Расчет технических индикаторов"""
        df = df.copy()
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0).rolling(RSI_PERIOD).mean()
        loss = -delta.where(delta < 0, 0).rolling(RSI_PERIOD).mean()
        rs = gain / loss
        rs = rs.replace([np.inf, -np.inf], np.nan)
        df['rsi'] = 100 - (100 / (1 + rs))
        df['rsi'] = df['rsi'].fillna(50)  # Заполняем NaN средним значением RSI

        ema_fast = df['close'].ewm(span=MACD_FAST, adjust=False).mean()
        ema_slow = df['close'].ewm(span=MACD_SLOW, adjust=False).mean()
        df['macd'] = ema_fast - ema_slow
        df['macd_signal'] = df['macd'].ewm(span=MACD_SIGNAL, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']

        df['range'] = df['high'] - df['low']
        df['volatility'] = df['close'].rolling(20).std()
        df['volatility'] = df['volatility'].fillna(df['volatility'].mean())  # Заполняем NaN

        df['support'] = df['low'].rolling(20).min()
        df['resistance'] = df['high'].rolling(20).max()
        df['support'] = df['support'].fillna(df['close'].mean())
        df['resistance'] = df['resistance'].fillna(df['close'].mean())

        return df

    def generate_signals(self, df):
        df['signal'] = np.nan

        # Условия для покупки
        buy_cond = (
                (df['rsi'] < 50) &
                (df['macd'] > df['macd_signal']) &
                (df['close'] > df['open'])
                #(df['volume'] > df['volume'].rolling(20).mean())  # Высокий объём
        )

        # Условия для продажи
        sell_cond = (
                (df['rsi'] > 50) &
                (df['macd'] < df['macd_signal']) &
                (df['close'] < df['open'])
                #(df['volume'] > df['volume'].rolling(20).mean()) # Высокий объём
        )

        df.loc[buy_cond, 'signal'] = 'buy'
        df.loc[sell_cond, 'signal'] = 'sell'

        return df

    def backtest(self, df, symbol, current_balance):
        """Бэктестинг стратегии с лимитными ордерами и случайным проскальзыванием"""
        trades = []
        open_positions = {}
        df = df.sort_values('timestamp')

        for i in range(len(df)):
            if symbol in open_positions:
                pos = open_positions[symbol]
                for j in range(i, min(i + 11, len(df))):
                    high = df['high'].iloc[j]
                    low = df['low'].iloc[j]
                    hit = False
                    slippage = self.get_random_slippage()

                    if pos['side'] == 'buy':
                        if high >= pos['tp']:
                            pos['exit_price'] = pos['tp'] * (1 + slippage)
                            hit = True
                        elif low <= pos['sl']:
                            pos['exit_price'] = pos['sl'] * (1 - slippage)
                            hit = True
                    else:
                        if low <= pos['tp']:
                            pos['exit_price'] = pos['tp'] * (1 - slippage)
                            hit = True
                        elif high >= pos['sl']:
                            pos['exit_price'] = pos['sl'] * (1 + slippage)
                            hit = True

                    if hit:
                        pos['exit_time'] = df['timestamp'].iloc[j]
                        pos['slippage'] = slippage
                        if pos['side'] == 'buy':
                            delta = (pos['exit_price'] - pos['entry_price']) / pos['entry_price']
                        else:
                            delta = (pos['entry_price'] - pos['exit_price']) / pos['entry_price']
                        pos['pnl'] = delta * pos['leverage'] - 2 * FEE_RATE
                        pos['absolute_pnl'] = pos['pnl'] * pos['position_size']
                        trades.append(pos)
                        current_balance += pos['absolute_pnl']
                        self.log(
                            f"Сделка закрыта для {symbol}: PNL = {pos['pnl']:.4f}, Абсолютный PNL = {pos['absolute_pnl']:.2f} USDT, Проскальзывание = {slippage:.6f}")
                        del open_positions[symbol]
                        break

            if pd.notna(df['signal'].iloc[i]) and symbol not in open_positions:
                side = df['signal'].iloc[i]
                entry_price = df['close'].iloc[i]
                leverage = random.randint(*LEVERAGE_RANGE)
                position_size = FIXED_POSITION_SIZE if USE_FIXED_SIZE else POSITION_PERCENT * current_balance
                self.log(
                    f"Открыта сделка для {symbol}: Сторона = {side}, Цена входа = {entry_price:.2f}, Плечо = {leverage}")

                if side == 'buy':
                    tp = entry_price * (1 + TAKE_PROFIT / leverage)
                    sl = entry_price * (1 - STOP_LOSS / leverage)
                else:
                    tp = entry_price * (1 - TAKE_PROFIT / leverage)
                    sl = entry_price * (1 + STOP_LOSS / leverage)

                open_positions[symbol] = {
                    'symbol': symbol,
                    'side': side,
                    'entry_time': df['timestamp'].iloc[i],
                    'entry_price': entry_price,
                    'leverage': leverage,
                    'position_size': position_size,
                    'tp': tp,
                    'sl': sl,
                    'exit_price': None,
                    'exit_time': None,
                    'pnl': None,
                    'absolute_pnl': None,
                    'slippage': 0.0
                }

        if symbol in open_positions:
            pos = open_positions[symbol]
            last_idx = len(df) - 1
            slippage = self.get_random_slippage()
            pos['exit_price'] = df['close'].iloc[last_idx] * (1 + slippage)
            pos['exit_time'] = df['timestamp'].iloc[last_idx]
            pos['slippage'] = slippage
            if pos['side'] == 'buy':
                delta = (pos['exit_price'] - pos['entry_price']) / pos['entry_price']
            else:
                delta = (pos['entry_price'] - pos['exit_price']) / pos['entry_price']
            pos['pnl'] = delta * pos['leverage'] - 2 * FEE_RATE
            pos['absolute_pnl'] = pos['pnl'] * pos['position_size']
            trades.append(pos)
            current_balance += pos['absolute_pnl']
            self.log(
                f"Сделка закрыта для {symbol} (конец данных): PNL = {pos['pnl']:.4f}, Абсолютный PNL = {pos['absolute_pnl']:.2f} USDT, Проскальзывание = {slippage:.6f}")

        return trades, current_balance

    def plot_balance(self, trades):
        """Построение графика изменения баланса"""
        if not trades:
            self.log("Нет данных для построения графика баланса")
            return

        df_trades = pd.DataFrame(trades)
        df_trades = df_trades.sort_values('exit_time')

        # Создаем временной ряд баланса
        balance = [STARTING_BALANCE]
        times = [df_trades['exit_time'].iloc[0]]

        current_balance = STARTING_BALANCE
        for _, trade in df_trades.iterrows():
            current_balance += trade['absolute_pnl']
            balance.append(current_balance)
            times.append(trade['exit_time'])

        # Построение графика
        plt.figure(figsize=(12, 6))
        plt.plot(times, balance, label='Баланс (USDT)', color='blue')
        plt.title('Изменение баланса во времени')
        plt.xlabel('Время')
        plt.ylabel('Баланс (USDT)')
        plt.grid(True)
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Сохранение графика
        plt.savefig('balance_plot.png')
        plt.close()
        self.log("График баланса сохранен в balance_plot.png")

    def train_model(self, features, labels):
        """Обучение нейронной сети"""
        mask = np.all(np.isfinite(features) & ~np.isnan(features), axis=1)
        features = features[mask]
        labels = labels[mask]

        if len(features) == 0:
            self.log("Ошибка: нет валидных данных для обучения модели")
            return

        X_train, X_test, y_train, y_test = train_test_split(
            features, labels, test_size=0.2, random_state=42
        )

        self.scaler = StandardScaler()
        X_train = self.scaler.fit_transform(X_train)
        X_test = self.scaler.transform(X_test)

        if np.any(np.isnan(X_train)) or np.any(np.isinf(X_train)):
            self.log("Ошибка: NaN или inf в нормализованных данных X_train")
            return
        if np.any(np.isnan(X_test)) or np.any(np.isinf(X_test)):
            self.log("Ошибка: NaN или inf в нормализованных данных X_test")
            return

        class Net(nn.Module):
            def __init__(self, input_size):
                super().__init__()
                self.fc1 = nn.Linear(input_size, 64)
                self.fc2 = nn.Linear(64, 32)
                self.fc3 = nn.Linear(32, 1)
                self.sigmoid = nn.Sigmoid()

            def forward(self, x):
                x = torch.relu(self.fc1(x))
                x = torch.relu(self.fc2(x))
                x = self.sigmoid(self.fc3(x))
                return x

        self.model = Net(X_train.shape[1])
        criterion = nn.BCELoss()
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)

        X_train_t = torch.tensor(X_train, dtype=torch.float32)
        y_train_t = torch.tensor(y_train, dtype=torch.float32).unsqueeze(1)
        X_test_t = torch.tensor(X_test, dtype=torch.float32)
        y_test_t = torch.tensor(y_test, dtype=torch.float32).unsqueeze(1)

        self.log("Начало обучения модели...")
        for epoch in range(100):
            self.model.train()
            optimizer.zero_grad()
            outputs = self.model(X_train_t)

            if torch.any(outputs < 0) or torch.any(outputs > 1):
                self.log(f"Ошибка: Выходы модели вне диапазона [0,1] на эпохе {epoch + 1}")
                return

            loss = criterion(outputs, y_train_t)
            loss.backward()
            optimizer.step()

            if epoch % 10 == 0:
                with torch.no_grad():
                    test_outputs = self.model(X_test_t)
                    test_loss = criterion(test_outputs, y_test_t)
                    acc = accuracy_score(y_test, (test_outputs > 0.5).float())
                    self.log(
                        f"Эпоха {epoch + 1} | Потери: {loss.item():.4f} | Тестовые потери: {test_loss.item():.4f} | Точность: {acc:.2f}")

        self.log("Обучение модели завершено!")

    def run(self):
        """Основной цикл работы бота"""
        self.log("🚀 Запуск торгового бота")
        all_trades = []
        ml_features = []
        ml_labels = []
        current_balance = STARTING_BALANCE

        for symbol in SYMBOLS:
            try:
                self.log(f"\n🔍 Анализ {symbol}...")
                df = self.get_historical_data(symbol, HISTORY_DAYS)
                if df.empty:
                    continue

                df = self.calculate_indicators(df)
                df = self.generate_signals(df)
                signals = df['signal'].notna().sum()
                self.log(f"Найдено сигналов: {signals}")

                trades, current_balance = self.backtest(df, symbol, current_balance)
                if trades:
                    all_trades.extend(trades)
                    self.log(f"Протестировано сделок: {len(trades)}")

                if USE_ML_MODEL:
                    for i in range(len(df)):
                        if pd.notna(df['signal'].iloc[i]):
                            features = [
                                df['rsi'].iloc[i],
                                df['macd'].iloc[i],
                                df['macd_hist'].iloc[i],
                                df['volatility'].iloc[i],
                                (df['close'].iloc[i] - df['support'].iloc[i]) / df['support'].iloc[i] if
                                df['support'].iloc[i] != 0 else 0,
                                (df['resistance'].iloc[i] - df['close'].iloc[i]) / df['close'].iloc[i] if
                                df['close'].iloc[i] != 0 else 0
                            ]
                            if all(np.isfinite(features)) and not any(np.isnan(features)):
                                ml_features.append(features)
                                trade_pnl = next(
                                    (t['pnl'] for t in trades if t['entry_time'] == df['timestamp'].iloc[i]), 0)
                                ml_labels.append(1 if trade_pnl > 0 else 0)
                            else:
                                self.log(f"Пропущены некорректные данные для {symbol}: {features}")

            except Exception as e:
                self.log(f"⚠️ Ошибка при анализе {symbol}: {str(e)}")
                continue

        if USE_ML_MODEL and ml_features:
            self.train_model(np.array(ml_features), np.array(ml_labels))

        if all_trades:
            df_trades = pd.DataFrame(all_trades)
            profitable = df_trades[df_trades['pnl'] > 0]
            total_pnl = df_trades['pnl'].sum()
            total_absolute_pnl = df_trades['absolute_pnl'].sum()
            avg_slippage = df_trades['slippage'].mean()

            self.log("\n📊 ИТОГОВЫЙ ОТЧЕТ:")
            self.log(f"Начальный баланс: {STARTING_BALANCE:.2f} USDT")
            self.log(f"Конечный баланс: {current_balance:.2f} USDT")
            self.log(f"Режим размера позиции: {'Фиксированная сумма' if USE_FIXED_SIZE else 'Процент от депозита'}")
            self.log(f"Всего сделок: {len(df_trades)}")
            self.log(f"Прибыльных сделок: {len(profitable)} ({len(profitable) / len(df_trades):.1%})")
            self.log(f"Средний PnL: {df_trades['pnl'].mean():.2%}")
            self.log(f"Суммарный PnL: {total_pnl:.2%}")
            self.log(f"Средний абсолютный PnL: {df_trades['absolute_pnl'].mean():.2f} USDT")
            self.log(f"Суммарный абсолютный PnL: {total_absolute_pnl:.2f} USDT")
            self.log(f"Лучшая сделка: {df_trades['pnl'].max():.2%} ({df_trades['absolute_pnl'].max():.2f} USDT)")
            self.log(f"Худшая сделка: {df_trades['pnl'].min():.2%} ({df_trades['absolute_pnl'].min():.2f} USDT)")
            self.log(f"Среднее проскальзывание: {avg_slippage:.6f}")

            df_trades.to_csv('trading_results3.csv', index=False)
            self.log("Результаты сохранены в trading_results3.csv")

            # Построение графика баланса
            self.plot_balance(all_trades)

        self.log("✅ Работа бота завершена")


if __name__ == "__main__":
    bot = TradingBot()
    bot.run()