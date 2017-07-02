import re
import requests
import time
from multiprocessing import Pool


def get_jobs(page):
    # for page in range(1, 31):
    url = f'https://www.zhipin.com/c101020100-p100109/?page={page}&ka=page-{page}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    r = requests.get(url, headers=headers)
    print(r.url, r.status_code)
    jobs = re.findall(
        r'<h3 class="name">(.+?)<span class="red">(.+?)</span></h3>\s+?<p>(.+?)<em.+?</em>(.+?)<em.+?</em>(.+?)</p>',
        r.text)
    with open('jobs.txt', 'a', encoding='utf8') as f:
        for job in jobs:
            f.write(f'{page}\t{job}\n')
            # time.sleep(2)


if __name__ == '__main__':
    pool = Pool(processes=3)
    pool.map_async(get_jobs, range(1, 31))
    pool.map()
    pool.close()
    pool.join()
