import asyncio

from aiogram import Bot, Dispatcher, executor, types
import logging
import requests
from aiogram.dispatcher.webhook import SendMessage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from bs4 import BeautifulSoup as BS
from pymongo import MongoClient
import aioschedule
from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,
    UniqueIdProperty, RelationshipTo, db)


API_TOKEN = "5276253794:AAGgC9RfqOmlnTiiIwijpUaW9IK3X2RcNic"
delay = 900
config.DATABASE_URL = 'bolt://neo4j:12345@localhost:7687/users'
# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
loop = asyncio.get_event_loop()

#client = MongoClient("mongodb://localhost:27017/")
#db = client['tgbot']
#userdata = db['userdata']
#users = db['users']

class Url(StructuredNode):
    # userid = StringProperty(unique_index=True, required=True)
    url = StringProperty(index=True, required=True)

    # car_url = StringProperty(index=True, required=True)


class User(StructuredNode):
    userid = StringProperty(unique_index=True, required=True)
    subscribe = RelationshipTo(Url, 'SUBSCRIBE')


class CarUrl(StructuredNode):
    car_url = StringProperty(index=True, required=True)
    parsed = RelationshipTo(Url, 'PARSED_FROM')
    # traverse outgoing IS_FROM relations, inflate to Country objects
    # country = RelationshipTo(Country, 'IS_FROM')

choice = InlineKeyboardMarkup(row_width=2,
                              inline_keyboard=[
                                  [
                                      InlineKeyboardButton(
                                          text="Отписаться",
                                          callback_data="cancel"
                                      )
                                  ]
                              ])


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    userids = []
    userids.append(db.cypher_query('MATCH (u: User) return u.userid'))
    #u = User()
    #if str(u.userid) != str(message.from_user.id):
    if message.from_user.id not in userids:
    #if users.find_one({'userid': message.from_user.id}) is None:
        #save_user(message.from_user.id)
        #User.create_or_update(userid = message.from_user.id)
        User(userid = message.from_user.id).save()
    await message.reply("Привет! Пришли мне ссылку вида https://cars.av.by/filter?brands[0][brand]=6&brands[0][model]=9&sort=4 чтобы я подобрал подходящие объявления.\n/help для более подробной инструкции.", disable_web_page_preview=True)

@dp.message_handler(commands=['help'])
async def send_welcome(message: types.Message):
    await message.reply(f'Для настройки фильтра выполните следующее:\n'
                              f'1. Зайдите на сайт av.by\n'
                              f'2. Настройте фильтр на сайте (не забудьте установить сортировку "Сначала новые")\n'
                              f'3. Нажмите кнопку "Показать"\n'
                              f'4. Скопируйте получившуюся ссылку из Вашего браузера\n'
                              f'5. Отправьте ссылку мне\n\n')



@dp.callback_query_handler(lambda c: c.data.startswith('cancel'))
async def cancel(callback: types.CallbackQuery):
    remove_all_userdata(callback.from_user.id)
    await callback.message.answer('Вы успешно отписались!')
    await callback.answer()


@dp.message_handler()
async def return_car_data(message: types.Message):
    data = get_data(url=str(message.text))
    if len(data) == 0:
        await message.answer('Попробуйте еще')
    elif len(data) != 0:
        await message.answer('Последние ' + str(len(data)) + ' объявлений по вашему фильтру:')
        for car in data:
            save_userdata(message.from_user.id, str(message.text), car['link'])
            await message.answer(get_car_str(car), disable_web_page_preview=True, parse_mode='html')
        await message.answer('Рассылка включена, я отправлю новые объявления как только они появятся.')


def get_data(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/99.0.4844.84 Safari/537.36 OPR/85.0.4341.47 '
    }
    response = requests.get(url, headers=headers)
    soup = BS(response.content, 'html.parser')
    items = soup.findAll('div', class_='listing-item__wrap')
    cars = []

    for item in items:
        cars.append({
            'title': item.find('a', class_='listing-item__link').get_text(strip=True),
            'price': item.find('div', class_='listing-item__priceusd').get_text(strip=True),
            'location': item.find('div', class_='listing-item__location').get_text(strip=True),
            'params': text_fix(item.find('div', class_='listing-item__params').get_text(strip=True)),
            'link': item.find('a', class_='listing-item__link').get('href')
        })
    return cars[:5]

def text_fix(text):
    for i in range(len(text) - 1, -1, -1):
        if (text[i] == ',') and (text[i + 1] != ' '):
            text = text[:i + 1] + ' ' + text[i + 1:]
        if (text[i] == '.') and (text[i+1] not in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '0')):
            text = text[:i + 1] + ' ' + text[i + 1:]
        #if (text[i+1] in ('1', '2', '3', '4', '5', '6', '7','8', '9')) and (text[i] not in ('1', '2', '3', '4', '5', '6', '7', '8', '9', '0', ' ', '.')):
        #    text = text[:i + 1] + ' ' + text[i + 1:]
    return text

def save_user(userid: str):
    User(userid = userid).save()
    #users.insert_one({'userid': userid})


def save_userdata(userid: str, url: str, car_url: str):
    UserData(userid = userid, url = url, car_url = car_url).save()
    #userdata.insert_one({'userid': userid, 'url': url, 'car_url': car_url})


def remove_all_userdata(userid: str):
    #UserData.nodes.delete(ueserid = userid)
    UserData(userid = userid).delete()
    #userdata.delete_many({'userid': userid})


async def check_updates():
    user_list = []
    user_list.append(User.userid)
    #user_list = users.find({})
    for user in user_list:
        #user(userid = User(userid = userid))
        ud = userdata.find({'userid': user['userid']})
        cars = []
        urls = []
        for data in ud:
            cars.append(data['car_url'])
            #if data['ulr'] not in urls:
            urls.append(data['url'])
        for url in urls:
            for car in get_data(url):
                if car['link'] not in cars:
                    cars.append(car['link'])
                    userdata.insert_one({'userid': user['userid'], 'url': url, 'car_url': car['link']})
                    await notify_user(user['userid'], "Новое объявление! \n" + get_car_str(car))


async def notify_user(userid: str, message: str):
    await bot.send_message(chat_id=userid, text=message, disable_web_page_preview=True, parse_mode='html', reply_markup=choice)


def get_car_str(car: dict):
    return car['title'] + '\nЦена: ' + car['price'] + "\nГород: " + car['location'] + "\nИнфо: " + car['params'] \
           + "\n\n" + f'<a href="{"cars.av.by" + car["link"]}">Смотреть объявление на сайте</a>\n\n'


async def my_func():
    await check_updates()
    when_to_call = loop.time() + delay
    loop.call_at(when_to_call, my_callback)


def my_callback():
    asyncio.ensure_future(my_func())


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=my_callback())

