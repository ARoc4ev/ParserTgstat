import requests
from bs4 import BeautifulSoup
import csv
import time
import datetime


class Channel:
    def __init__(self, name):
        self.name = name
        self.heders_get = {
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
        self.heders_post = {
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Length': '10',
            'Cookie': '',
            'Host': 'tgstat.ru',
            'Referer': self.referer(name),
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
            'X-CSRF-Token': '',

        }
        self.months = dict(Jan='01', Feb='02', Mar='03', Apr='04', May='05', Jun='06', Jul='07', Aug='08', Sep='09',
                           Oct=10, Nov=11,
                           Dec=12)
        self.year = datetime.datetime.now().year
        self.month = []
        self.urls_get = self.urls(self.name)
        self.urls_post = self.urls2(self.name)
        self.data = {'page': '0', 'offset': '0',
                     'group_by': 'hour'}
        self.file()

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

    def urls(self, name):
        return 'https://tgstat.ru/channel/' + name + '#hourly'

    def urls2(self, name):
        return 'https://tgstat.ru/channel/' + name + '/hourly'

    def referer(self, name):
        return 'https://tgstat.ru/channel/' + name

    def file(self, data=['Timestamp', 'Change', 'Members_count', 'Event', 'Post_link', 'Post_id', 'Outer_post_link',
             'Outer_post_channel',
             'Outer_post_channel_name', 'Outer_post_id', 'Multiple_publications_count']):
        myFile = open(self.name + '.csv', 'a')
        with myFile:
            writer = csv.writer(myFile)
            writer.writerows([data])
            myFile.close()


def html_get(url, heders, data=None):
    try:
        r = requests.get(url, headers=heders, data=data, timeout=(5, 10))
        return r
    except requests.exceptions.ConnectTimeout:
        print('Error get')
        """ Если ошибка засыпаем на 5 сек"""
        time.sleep(5)
        return False


def htmls(url, heders, data=None):
    r = False
    while r == False:
        r = html_get(url, heders, data)
    return r


def parsertoken(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('meta')[3]['content']
    return items


def get_content(html, ObChannel):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='views-statistics-day js-collapsable-block active-parent')
    if len(items) == 0:
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

                    Timestamp = day + '.' + str(ObChannel.months[data[2]]) + '.' + str(
                        ObChannel.DataYear(data[2])) + ' ' + str(timev)


                else:

                    if len(data[1]) < 2:
                        day = '0' + data[1]
                    else:
                        day = data[1]

                    Timestamp = day + '.' + str(ObChannel.months[data[2]]) + '.' + str(
                        ObChannel.DataYear(data[2])) + ' ' + str(
                        time2[1].split('</i>')[0].split(' ')[2])

                try:

                    Change = time[3].span.get_text().replace(' ', '')
                    Members_count = time[4].get_text()
                except AttributeError:
                    Change = time[5].span.get_text().replace(' ', '')
                    Members_count = time[6].get_text()

                ObChanel.file([Timestamp, Change, Members_count, Event, Post_link, Post_id, Outer_post_link,
                              Outer_post_channel,
                              Outer_post_channel_name, Outer_post_id, Multiple_publications_count])

            if len(evintList) == 0:
                if len(data[1]) < 2:

                    day = '0' + data[1]
                else:
                    day = data[1]

                timev = time[1].get_text().replace(' ', '')[1:]  ## часы

                Timestamp = day + '.' + str(ObChannel.months[data[2]]) + '.' + str(
                    ObChannel.DataYear(data[2])) + ' ' + str(timev)

                try:
                    Change = time[3].span.get_text().replace(' ', '')
                    Members_count = time[4].get_text()
                except AttributeError:
                    Change = time[5].span.get_text().replace(' ', '')
                    Members_count = time[6].get_text()

                ObChanel.file([Timestamp, Change, Members_count, Event, Post_link, Post_id, Outer_post_link,
                               Outer_post_channel,
                               Outer_post_channel_name, Outer_post_id, Multiple_publications_count])
    return True


def parse(ObChanel):
    start_time = time.time()
    html = htmls(ObChanel.urls_post, ObChanel.heders_post, data=ObChanel.data)
    t = "--- %s seconds ---" % (time.time() - start_time)
    if html.status_code == 200:
        print('Ok', ObChanel.name, t)
        cars = get_content(html.text, ObChanel, )
        return cars
    else:
        print(html.status_code, ObChanel.name, t)
        return "Error"


channels = ['@theworldisnoteasy']
for channel in channels:
    ObChanel = Channel(channel)
    html = htmls(ObChanel.urls_get, ObChanel.heders_get)

    token = parsertoken(html.text)
    cookie = ''
    for i in html.cookies:
        cookie = cookie + i.name
        cookie = cookie + '='
        cookie = cookie + i.value
        cookie = cookie + ';'

    ObChanel.heders_post['Cookie'] = cookie
    ObChanel.heders_post['X-CSRF-Token'] = token

    Flag = True
    x = 1 # задержка между запросами
    while Flag:
        time.sleep(x)
        Flag = parse(ObChanel)
        if Flag == True:
            ObChanel.data['page'], ObChanel.data['offset'] = int(ObChanel.data['page']) + 1, int(
                ObChanel.data['offset']) + 10
            x = 1
        elif Flag == "Error":
            x = 60
        print(ObChanel.data)
