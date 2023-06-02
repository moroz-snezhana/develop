import requests
import json
import os
from aiogram.dispatcher import FSMContext
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import Message
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from aiogram.contrib.middlewares.logging import LoggingMiddleware
import psycopg2
from aiogram.types.bot_command_scope import BotCommandScopeDefault
import numpy
import asyncio

conn = psycopg2.connect(database='RGR_8',user='postgres',password='postgres',port='5432')
cursor = conn.cursor()

# Создание курсора
cur = conn.cursor()

# # Определение CREATE TABLE
# create_table_query = """
# CREATE TABLE paper (
# id serial PRIMARY KEY,
# paperName varchar(200),
# dailyEarnValue numeric,
# dailyEarnError numeric
# )
# """

# # Выполнение CREATE TABLE
# cur.execute(create_table_query)

# # Фиксирование изменений в БД
# conn.commit()

os.environ['API_TOKEN'] = '5694086843:AAFPQA0YNxRU4djK1YcpdJwUpUcqlmRriho'

API_TOKEN = os.environ['API_TOKEN']
bot = Bot(API_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

commands = [
    types.BotCommand(command='/start', description='Начало'),
    types.BotCommand(command='/load', description='Добавить ценную бумагу к портфелю'),
    types.BotCommand(command='/show', description='Показатели отслеживаемых ценных бумаг'),
    types.BotCommand(command='/delete', description='Удалить ценную бумагу из портфеля')
]


async def setup_bot_commands(arg):
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())


class States(StatesGroup):
    start = State()
    load_name = State()
    delete_name = State()
    showState = State()


@dp.message_handler(commands=['start'])
async def fun1(message: Message):
    await States.start.set()
    await message.reply('Выберите действие из меню')


@dp.message_handler(commands=['load'])
async def fun3(message: Message):
    await States.load_name.set()
    await message.reply('Введите имя ценной бумаги')


@dp.message_handler(state=States.load_name.state)
async def fun10(message: Message, state: FSMContext):
    name = message.text
    cursor.execute("INSERT INTO paper (paperName, dailyEarnValue, dailYEarnError) VALUES (%s, %s, %s)", (name, 0, 0))
    conn.commit()
    cursor.execute("SELECT lastval()")  # Получение последнего вставленного ID
    paper_id = cursor.fetchone()[0]
    await message.reply('Ценная бумага ' + str(name) + ' добавлена к отслеживаемым')
    await state.finish()


@dp.message_handler(commands=['delete'])
async def fun11(message: Message):
    await States.delete_name.set()
    await message.reply('Введите имя ценной бумаги, которую нужно удалить')


@dp.message_handler(state=States.delete_name.state)
async def fun12(message: Message, state: FSMContext):
    name = message.text
    cursor.execute("DELETE FROM paper WHERE paperName = %s", (name,))
    conn.commit()
    await message.reply('Ценная бумага ' + str(name) + ' удалена из портфеля')
    await state.finish()


@dp.message_handler(commands=['show'])
async def show_papers(message: Message):
    cursor.execute(f'SELECT paperName, dailyEarnValue, dailYEarnError FROM paper')
    results = cursor.fetchall()
    
    # Проверяем, есть ли результаты запроса
    if results:
        response = "Бумага | Ежедневная доходность | Стандартное отклонение\n"

        # Обрабатываем каждый результат запроса
        for result in results:
            paper_name = result[0]
            daily_earnings = result[1]
            earnings_error = result[2]

            # Формируем строку для каждого результата
            line = f"{paper_name} | {daily_earnings} | {earnings_error}\n"
            response += line

        # Отправляем ответ с информацией о ценных бумагах
        await message.reply(response)
    else:
        # Если результаты запроса отсутствуют, отправляем сообщение об отсутствии данных
        await message.reply("Нет доступных данных о ценных бумагах.")


APIKey = "CH61N6Y2QA0BGBHU"


async def calculate_paper(paper_id):
    # Получение имени ценной бумаги по ее идентификатору из базы данных
    cursor.execute("SELECT paperName FROM paper WHERE id = %s", (paper_id,))
    result = cursor.fetchone() # Получение одной строки результата выполнения запроса
    if result: # Проверка, что результат существует и не является пустым
        blank = result[0]
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={blank}&apikey={APIKey}'
        response = requests.get(url)
        data = json.loads(response.text) # Преобразование ответа в структуру данных Python
        series = data['Time Series (Daily)'] # Извлечение временного ряда из полученных данных
        lst = []
        counter = 0
        temp = "" # Инициализация временной переменной
        
        # Обработка данных временного ряда
        for i in series:
            if counter == 30: # Если счетчик достиг 30, выходим из цикла
                break
            if temp == "": # Если temp пустая строка, то это первая итерация
                temp = series[i]['4. close']
                lst.append(float(temp)) # Преобразуем значение temp во float и добавляем в список lst
                continue

            current_close = series[i]['4. close'] # Если temp не пустая строка, то выполняем следующий блок кода
            yest = temp
            dailyEarn = ((float(yest) - float(current_close)) / float(current_close)) # Рассчитываем ежедневную доходность
            dailyEarn += dailyEarn # Добавляем ежедневную доходность к сумме ежедневных доходностей
            counter += 1
            temp = current_close # Обновляем значение temp
            lst.append(float(temp)) # Преобразуем значение temp во float (плавающая точка) и добавляем в список lst

        # Обновление данных ценной бумаги в базе данных
        dailyEarnValue = dailyEarn / 30
        dailYEarnError = numpy.std(numpy.array(lst))
        cursor.execute("UPDATE paper SET dailyEarnValue = %s, dailYEarnError = %s WHERE id = %s",
                       (dailyEarnValue, dailYEarnError, paper_id))
        conn.commit()



async def daily_task():
    while True:
        # Получение всех ценных бумаг из базы данных
        cursor.execute(f"SELECT * FROM paper")
        blanks = cursor.fetchall()

        for i in blanks:
            # Вызов функции calculate_paper() для каждой ценной бумаги
            await calculate_paper(i[0])  # Ожидание функции calculate_paper()

        await asyncio.sleep(20)  # Подождите 20 секунд перед повторным запуском задания


async def start_daily_task():
    await asyncio.sleep(10)  # Подождать 10 секунд, чтобы убедиться, что бот полностью запущен
    asyncio.create_task(daily_task())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(start_daily_task())
    # Запуск бота с помощью функции start_polling()
    # skip_updates=True указывает на пропуск необработанных обновлений при запуске
    # on_startup=setup_bot_commands вызывает функцию setup_bot_commands() при запуске бота
    executor.start_polling(dp, skip_updates=True, on_startup=setup_bot_commands)
