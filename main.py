import asyncio

from aiogram import Bot, Dispatcher, executor, types
import logging
import requests
from aiogram.dispatcher.webhook import SendMessage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from bs4 import BeautifulSoup as BS
import aioschedule
#from neomodel import (config, StructuredNode, StringProperty, IntegerProperty,
#   UniqueIdProperty, RelationshipTo, db)
from neo4j import GraphDatabase, AsyncGraphDatabase


API_TOKEN = "5276253794:AAGgC9RfqOmlnTiiIwijpUaW9IK3X2RcNic"
delay = 900
URI = "bolt://localhost:7687"
#AUTH = ("neo4j", "12345")
driver = GraphDatabase.driver(URI, auth=("neo4j", "12345"))
print("connected: {}".format(driver.verify_connectivity()))


# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
loop = asyncio.get_event_loop()

def checkUserTx():

        with driver.session(database="users") as session:
            users = session.execute_read(checkUser)
        return users

def checkUser(tx):
    result = tx.run(
        "MATCH (n:User) RETURN n.userid")
    value = result.value()
    #users = list(result)
    return value

def create_user_tx(userid: str):
    with driver.session(database="users") as session:
        session.execute_write(create_user, userid)

def create_user(tx, userid: str):
    tx.run("MERGE (u:User {userid: $userid})",
           userid=userid)


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
    users = checkUserTx()

    if message.from_user.id not in users:
        create_user_tx(message.from_user.id)

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
    remove_all_userdata_tx(callback.from_user.id)
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
            save_url_tx(message.from_user.id, str(message.text))
            save_car_tx(str(message.text), car['link'])
            #save_userdata_tx(message.from_user.id, str(message.text), car['link'])
            await message.answer(get_car_str(car), disable_web_page_preview=True, parse_mode='html')
        await message.answer('Рассылка включена, я отправлю новые объявления как только они появятся.')

def save_url_tx(userid: str, url: str):
    with driver.session(database="users") as session:
        session.execute_write(save_url, userid, url)

def save_url(tx, userid: str, url: str):
    tx.run("MATCH (a:User) WHERE a.userid = $userid "
           "MERGE (u:Url {url: $url})<-[:SUBSCRIBED]- (a) ",
           userid=userid, url=url)

def save_car_tx(url: str, car_url: str):
    with driver.session(database="users") as session:
        session.execute_write(save_url, url, car_url)

def save_car(tx, url: str, car_url: str):
    tx.run("MATCH (u:Url) WHERE u.url = $url "
           "MERGE (u)<-[:PARSED]- (c:CarUrl {car_url: $car_url}) ",
           car_url=car_url, url=url)


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


#def save_userdata_tx(userid: str, url: str, car_url: str):
#    with driver.session(database="users") as session:
#        session.execute_write(save_userdata, userid, url, car_url)


#def save_userdata(tx, userid: str, url: str, car_url: str):
#    tx.run("MATCH (a:User) WHERE a.userid = $userid "
#           "CREATE (u:Url {url: $url})"
#           "MERGE (a)-[:SUBSCRIBED]->(u:Url {url: $url}) "
#           "MERGE (c:CarUrl {car_url: $car_url}) - [:PARSED]-> (u:Url {url: $url}) ",
#           userid=userid, url=url, car_url=car_url)

'''"MATCH (a:User) WHERE a.userid = $userid "
           "MATCH (u:Url) WHERE u.url = $url "
           "CREATE (a)-[:SUBSCRIBED]->(u:Url {url: $url}) "
           "MATCH (c:CarUrl) WHERE c.car_url = $car_url "
           "CREATE (c)-[:PARSED]->(u:Url {url: $url})",
           userid=userid, url=url, car_url=car_url'''

def remove_all_userdata_tx(userid :str):
    with driver.session(database="users") as session:
        session.execute_write(remove_all_userdata_tx, userid)

def remove_all_userdata(tx, userid: str):
    #tx.run("MATCH (a:User {userid: $userid}) DETACH DELETE a ",
    #       userid=userid)
    tx.run("MATCH (n{userid: $userid})-[r: SUBSCRIBED]->() DELETE r ",
           userid=userid)


async def check_updates():
    user_list = checkUserTx()
    #u = str(user_list)
    for user in user_list:
        #user(userid = User(userid = userid))
        #urls = []

        #ud = userdata.find({'userid': user['userid']})
        #сюда запилить ссылки на объявы конкретного юзера
        urls = get_urls_from_user_tx(user)#сюда запилить ссылки на подписки конкретного юзера
        #for data in ud:
        #    cars.append(data['car_url'])
        #    #if data['ulr'] not in urls:
        #    urls.append(data['url'])


        for url in urls:
            cars = get_cars_tx(url)
            for car in get_data(url):
                if car['link'] not in cars:
                    cars.append(car['link'])
                    save_url_tx(user, url)
                    save_car_tx(url, car['link'])
                    #save_userdata_tx(user, url, car['link'])
                    #userdata.insert_one({'userid': user['userid'], 'url': url, 'car_url': car['link']})
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

def get_urls_from_user_tx(userid: str):
    with driver.session(database="users") as session:
        urls = session.execute_read(get_urls_from_user, userid)
    return urls

def get_urls_from_user(tx, userid: str):
    result = tx.run("MATCH (n:User {userid: $userid})-[:SUBSCRIBED]->(u:Url) RETURN u.url ", userid=userid)
    urls = list(result)
    urls = result.value()
    return urls



def get_cars_tx(url: str):
    with driver.session(database="users") as session:
        cars = session.execute_read(get_cars, url)
    return cars

def get_cars(tx, url:str):
    result = tx.run("MATCH (c:CarUrl)-[:PARSED]->(u:Url {url: $url}) RETURN c.car_url ", url=url)
    cars = list(result)
    cars = result.value()
    return cars

#def add_car_to_user_tx(userid: str, url: str, car_url: str):
#    with driver.session(database="users") as session:
#        session.execute_write(add_car_to_user, userid, url, car_url)

#def add_car_to_user(tx, userid: str, url: str, car_url: str):
#    tx.run()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=my_callback())

