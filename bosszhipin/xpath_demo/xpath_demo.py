import requests
from lxml import etree
from multiprocessing import Pool
import pymysql.cursors
import re
from datetime import datetime, date
import time
import random
from fake_useragent import UserAgent

conn = pymysql.connect(host='localhost', user='root', password='root', db='bosszhipin', charset='utf8',
                       cursorclass=pymysql.cursors.DictCursor)

ua = UserAgent()


def get_links(url):
    base_url = 'http://www.zhipin.com'
    headers = {'User-Agent': ua.random}
    r = requests.get(url, headers=headers)
    html = etree.HTML(r.text)
    with open('test.html', 'w', encoding='utf8') as f:
        f.write(r.text)
    links = html.xpath(r'//div[@class="job-list"]/ul/li/a/@href')
    print(links)
    with open('links.txt', 'a', encoding='utf8') as f:
        for link in links:
            print(link)
            f.write(f'{base_url}{link}\n')


def get_jobs(link):
    headers = {'User-Agent': ua.random}
    proxies = {'http': 'socks5://127.0.0.1:1080'}
    r = requests.get(link, headers=headers, proxies=proxies)
    # print(r.status_code)
    html = etree.HTML(r.text)
    try:
        pubdate_params = re.findall(r'发布于(\d+?)月(\d+?)日', html.xpath(r'//span[@class="time"]')[0].text)
        pubdate = datetime.strptime(f'2017-{pubdate_params[0][0]}-{pubdate_params[0][1]}', '%Y-%m-%d')
    except Exception as e:
        print(f'{e}时间出错，昨天！')
        pubdate = date(2017, 7, 1)
    name = html.xpath(r'//div[@class="job-primary"]//div[@class="name"]')[0].text
    salary = html.xpath(r'//div[@class="job-primary"]//span[@class="badge"]')[0].text
    desc_list = html.xpath(r'//div[@class="job-primary"]/div[@class="info-primary"]//p/text()')
    place = desc_list[0]
    work_year = desc_list[1]
    degree = desc_list[2]
    tags_list = html.xpath(r'//div[@class="job-tags"]/span')
    tags = ','.join([i.text for i in tags_list])
    company = html.xpath(r'//div[@class="info-company"]//p')[0].text
    detail = ''.join(html.xpath(r'//div[@class="job-sec"]/div[@class="text"]/text()'))
    address = html.xpath(r'//div[@class="location-address"]')[0].text

    # with open('test.txt', 'w', encoding='utf8') as f:
    #     f.write(
    #         f'pubdate:{pubdate},name:{name},salary:{salary},place:{place},work_year:{work_year},degree:{degree}, tags:{tags},company:{company},detail:{detail},address:{address}\n')

    try:
        with conn.cursor() as cursor:
            sql = """insert into jobs (name, salary, place, work_year, degree, tags, company, detail, address, pubdate)
              VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s)"""
            cursor.execute(sql, (name, salary, place, work_year, degree, tags, company, detail, address, pubdate))
            conn.commit()
    finally:
        pass


if __name__ == '__main__':
    # url = 'https://www.zhipin.com/c101020100-p100109/?page=4&ka=page-4'
    # get_links(url)
    # pool = Pool()
    # for page in range(1, 31):
    #     link = f'https://www.zhipin.com/c101020100-p100109/?page={page}&ka=page-{page}'
    #     pool.map_async(get_links, (link,))
    # pool.close()
    # pool.join()

    # url = 'http://www.zhipin.com/job_detail/1400268587.html'
    # get_jobs(url)

    # pool = Pool()
    # with open('links.txt', encoding='utf8') as f:
    #     for num, link in enumerate(f.readlines()):
    #         pool.map_async(get_jobs, (link,))
    #         print(f'爬完了第{num}个, 链接{link}')
    #     pool.close()
    #     pool.join()

    with open('links.txt', encoding='utf8') as f:
        for num, link in enumerate(f.readlines()):
            get_jobs(link)
            print(f'爬完了第{num}个, 链接{link}')
            time.sleep(random.uniform(3, 5))
