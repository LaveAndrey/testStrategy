# Используем официальный образ Python как базовый
FROM python:3.9-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл зависимостей в контейнер
COPY requirements.txt .

# Устанавливаем системные зависимости и Python-пакеты
RUN apt-get update && apt-get install -y \
    build-essential \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копируем код приложения в контейнер
COPY . .

# Устанавливаем переменную окружения для неббуферизированного вывода Python
ENV PYTHONUNBUFFERED=1

# Команда для запуска торгового бота
CMD ["python", "teststrategy.py"]