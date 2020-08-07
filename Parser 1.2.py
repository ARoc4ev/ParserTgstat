import asyncio
import asyncio
import requests
import time
from asgiref.sync import sync_to_async
import datetime
from bs4 import BeautifulSoup
import csv

MONTHS = dict(Jan='01', Feb='02', Mar='03', Apr='04', May='05', Jun='06', Jul='07', Aug='08', Sep='09', Oct=10, Nov=11,
              Dec=12)


class Data:
    def __init__(self):
        self.year = datetime.datetime.now().year
        self.month = []

    def DataYear(self, monthData):

        try:
            if self.month[-1] != monthData:
                self.month.append(monthData)
                try:
                    if self.month[-2] == 'Jan' and self.month[-1] == 'Dec':
                        self.year -= 1
                except IndexError:
                    pass
        except IndexError:
            self.month.append(monthData)

        return self.year


def urls(name):
    return 'https://tgstat.ru/channel/' + name + '#hourly'


def urls2(name):
    return 'https://tgstat.ru/channel/' + name + '/hourly'


async def say_after(url, heders):
    r = await sync_to_async(requests.get)(url, headers=heders, timeout=(5, 10))
    print(r)


async def get_html(url, heders, data=None):
    try:
        r = requests.get(url, headers=heders, data=data, timeout=(5, 10))
        return r
    except (requests.exceptions.ConnectTimeout):
        print(url, data, 'Во аремя запроса произашла ошибка  ConnectTimeout.(Засыпаю на 30 сек)')
        """ Если ошибка засыпаем на 10 сек"""
        time.sleep(5)
        return False
    except requests.exceptions.ReadTimeout:
        print(url, data, 'Во аремя запроса произашла ошибка ReadTimeout.(Засыпаю на 30 сек')
        time.sleep(5)
        return False
    except requests.exceptions.ConnectionError:
        print(url, data, 'Во аремя запроса произашла ошибка  ConnectionError.(Засыпаю на 30 сек)')
        time.sleep(5)
        return False


async def request_get(url, heders, params=None):
    r = False
    while r == False:
        r = await get_html(url, heders, params)
    return r


async def post_html(url, heders, data):
    try:
        r = await sync_to_async(requests.post)(url, headers=heders, data=data, timeout=(5, 10))
        return r
    except requests.exceptions.ConnectTimeout:
        print(url, data, 'Во аремя запроса произашла ошибка  ConnectTimeout.(Засыпаю на 30 сек)')
        """ Если ошибка засыпаем на 30 сек"""
        await time.sleep(5)
        return False
    except requests.exceptions.ReadTimeout:
        print(url, data, 'Во аремя запроса произашла ошибка ReadTimeout.(Засыпаю на 30 сек')
        await time.sleep(5)
        return False
    except requests.exceptions.ConnectionError:
        print(url, data, 'Во аремя запроса произашла ошибка  ConnectionError.(Засыпаю на 30 сек)')
        await time.sleep(5)
        return False


async def request_post(url, heders, data):
    r = False
    while r == False:
        r = await post_html(url, heders, data)
    return r


def referer(name):
    return 'https://tgstat.ru/channel/' + name


def parsertoken(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('meta')[3]['content']
    return items


def get_content(html, name, datetime):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='views-statistics-day js-collapsable-block active-parent')
    myFile = open(name + '.csv', 'a')
    if len(items) == 0:
        myFile.close()
        return False
    for test in items:
        data = (test.find('div', class_='views-statistics-header-2 js-collapsable-sticky-header').get_text()).split(
            ' ')  # дата
        for i in range(len(test.find_all('div', class_='views-statistics-hours-row'))):
            time = test.find_all('div', class_='views-statistics-hours-row')[i].find_all(
                'div')  # событие[0], часы[1],  -+подписчики[3],  число подписчикjd[4]

            Event, Post_link, Post_id, Outer_post_link, Outer_post_channel, Outer_post_channel_name, Outer_post_id, Multiple_publications_count = '', '', '', '', '', '', '', ''

            """Проверка на событие"""

            evintList = time[0].find_all('a')

            for event in evintList:
                Event, Post_link, Post_id, Outer_post_link, Outer_post_channel, Outer_post_channel_name, Outer_post_id, Multiple_publications_count = '', '', '', '', '', '', '', ''
                if event['data-milestone-type'] == 'new_post':
                    if event['class'] == ['popup_ajax', 'in_popup']:
                        Event = 'Post'
                        Post_link = event['data-src']
                        Post_id = Post_link.split('/')[-1]
                    else:
                        Event = 'Multiple_publications'
                        Multiple_publications_count = event.find('img')['title'].split('<b>')[1].split('</b>')[0]




                elif event['data-milestone-type'] == 'mention':
                    Event = 'Outer_mention'
                    Outer_post_link = event['data-src']
                    ftest = event.find('img')['title'].split('<b>')[1]
                    Outer_post_channel = Outer_post_link.split('/')[2]
                    Outer_post_channel_name = ftest.split('</b>')[0]  ##название канала
                    Outer_post_id = Outer_post_link.split('/')[-1]

                elif event['data-milestone-type'] == 'forward':
                    Event = 'Outer_repost'
                    Outer_post_link = event['data-src']
                    ftest = event.find('img')['title'].split('<b>')[1]
                    Outer_post_channel = Outer_post_link.split('/')[2]
                    Outer_post_channel_name = ftest.split('</b>')[0]  ##название канала
                    Outer_post_id = Outer_post_link.split('/')[-1]

                time2 = event.find('img')['title'].split('<i>')  ## время пуликации
                if Event == 'Multiple_publications':
                    timev = time[1].get_text().replace(' ', '')[1:]
                    if ((len(data[1]))) < 2:
                        day = '0' + data[1]
                    else:
                        day = data[1]

                    Timestamp = day + '.' + str(MONTHS[data[2]]) + '.' + str(datetime.DataYear(data[2])) + ' ' + str(
                        timev)


                else:

                    if len(data[1]) < 2:
                        day = '0' + data[1]
                    else:
                        day = data[1]

                    Timestamp = day + '.' + str(MONTHS[data[2]]) + '.' + str(datetime.DataYear(data[2])) + ' ' + str(
                        time2[1].split('</i>')[0].split(' ')[2])

                try:

                    Change = time[3].span.get_text().replace(' ', '')
                    Members_count = time[4].get_text()
                except AttributeError:
                    Change = time[5].span.get_text().replace(' ', '')
                    Members_count = time[6].get_text()

                file(name,
                    [Timestamp, Change, Members_count, Event, Post_link, Post_id, Outer_post_link,
                     Outer_post_channel,
                     Outer_post_channel_name, Outer_post_id, Multiple_publications_count]
                )

            if len(evintList) == 0:
                if len(data[1]) < 2:

                    day = '0' + data[1]
                else:
                    day = data[1]

                timev = time[1].get_text().replace(' ', '')[1:]  ## часы

                Timestamp = day + '.' + str(MONTHS[data[2]]) + '.' + str(datetime.DataYear(data[2])) + ' ' + str(timev)

                try:
                    Change = time[3].span.get_text().replace(' ', '')
                    Members_count = time[4].get_text()
                except AttributeError:
                    Change = time[5].span.get_text().replace(' ', '')
                    Members_count = time[6].get_text()

                myFile = open(name + '.csv', 'a')

            file(name,
                [Timestamp, Change, Members_count, Event, Post_link, Post_id, Outer_post_link,
                 Outer_post_channel,
                 Outer_post_channel_name, Outer_post_id, Multiple_publications_count]
            )
    return True


async def parse(name, data, datatime, heders):
    start_time = time.time()
    html = await request_post(urls2(name), heders, data=data)
    t = "--- %s seconds ---" % (time.time() - start_time)
    if html.status_code == 200:
        print('Ok', name, t)
        cars = await sync_to_async(get_content)(html.text, name, datatime)
        return cars
    else:

        return 'Error'


def file(name, data=['Timestamp', 'Change', 'Members_count', 'Event', 'Post_link', 'Post_id', 'Outer_post_link',
                     'Outer_post_channel',
                     'Outer_post_channel_name', 'Outer_post_id', 'Multiple_publications_count']):
    myFile = open(name + '.csv', 'a')
    with myFile:
        writer = csv.writer(myFile)
        writer.writerows([data])
        myFile.close()


async def start_parser(channel):
    await sync_to_async(file)(channel)

    url = await sync_to_async(urls)(channel)

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Cookie': '',
        'Host': 'tgstat.ru',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',

    }
    headers_post = {
        'Accept': '*/*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Length': '10',
        'Cookie': '',
        'Host': 'tgstat.ru',
        'Referer': '',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
        'X-CSRF-Token': '',

    }
    html = await request_get(url, headers)
    token = await sync_to_async(parsertoken)(html.text)
    cookie = ''
    for i in html.cookies:
        cookie = cookie + i.name
        cookie = cookie + '='
        cookie = cookie + i.value
        cookie = cookie + ';'

    headers_post['Cookie'] = cookie
    headers_post['X-CSRF-Token'] = token
    headers_post['Referer'] = await sync_to_async(referer)(channel)
    data = {'page': '0', 'offset': '0',
            'group_by': 'hour'}
    Flag = True
    dataTime = Data()
    x = 1
    while Flag:
        time.sleep(x)
        Flag = await parse(channel, data, dataTime, headers_post)
        data['page'], data['offset'] = int(data['page']) + 1, int(data['offset']) + 10

    print('Парсинг канала', channels , 'завершен', data)







async def worker(name, queue):
    while True:
        # Get a "work item" out of the queue.
        channel = await queue.get()
        await start_parser(channel)

        # Sleep for the "sleep_for" seconds.

        # Notify the queue that the "work item" has been processed.
        queue.task_done()



async def main(channels):
    # Create a queue that we will use to store our "workload".
    queue = asyncio.Queue()

    # Generate random timings and put them into the queue.

    for _ in channels:
        queue.put_nowait(_)
    print(queue.qsize())

    # Create three worker tasks to process the queue concurrently.
    tasks = []
    for i in range(queue.qsize()):
        task = asyncio.create_task(worker(i, queue))
        tasks.append(task)

    # Wait until the queue is fully processed.
    await queue.join()
    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
    # Wait until all worker tasks are cancelled.
    await asyncio.gather(*tasks, return_exceptions=True)


def f(lst, n):
     return [lst[i:i + n] for i in range(0, len(lst), n)]

channels = ['@theworldisnoteasy', 'AAAAAEwcdHGofxANp6zSOQ', '@breakingmash', '@Cbpub',' @kinogoo_film', 'AAAAAEN9nMLxZYeXTVYbSA']


for i in f(channels, 4):
    asyncio.run(main(i))
