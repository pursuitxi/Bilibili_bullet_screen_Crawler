import csv
import datetime
import argparse
import hashlib
import re
import sys
from urllib.parse import quote
import execjs
import requests
from bs4 import BeautifulSoup
from playwright.async_api import BrowserContext, Page, async_playwright
import asyncio
import config
from tools import utils
from login import BilibiliLogin

class BilibiliSpider:

    def __init__(self, login_type: str, timeout=10):
        # self.ua = UserAgent()
        self.headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'accept-language': 'zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7,en-GB;q=0.6,en-US;q=0.5',
                        'sec-ch-ua': '"Microsoft Edge";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'document',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-site': 'none',
                        'sec-fetch-user': '?1',
                        'upgrade-insecure-requests': '1',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
                        }
        self.login_type = login_type
        self.timeout = timeout
        self.playwright_page = None
        self.cookies = None

    async def start_crawling(self):
        await self.login()
        await self.get_danmu_list()
    async def login(self):
        """ login """
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            bilibililogin = BilibiliLogin(login_type=self.login_type, browser=browser)
            await bilibililogin.begin()
            self.cookies = bilibililogin.cookies

    async def get_oid_and_public_month(self, bvid):
        response = requests.get(f'https://www.bilibili.com/video/{bvid}/', cookies=self.cookies, headers=self.headers)
        cid_match = re.search(r'"cid":\s*(\d+)', response.text)
        soup = BeautifulSoup(response.text, 'html.parser')
        public_date = soup.find('div', class_='pubdate-ip-text').text
        public_month = public_date[:7]
        if cid_match:
            cid = cid_match.group(1)
            utils.logger.info(f"[BilibiliCrawler.get_oid] begin crawling oid: {cid} and public month: {public_month}!")
            return {'bvid':bvid,'oid':cid, 'public_month':public_month}
        return None

    async def get_oid_and_public_month_task(self, bvid, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                result = await self.get_oid_and_public_month(bvid)
                return result
            except KeyError as ex:
                utils.logger.error(
                    f"[BilibiliCrawler.get_oid_task] have not fund video detail bvid: {bvid}, err: {ex}")
                return None


    async def get_oid_and_public_month_list(self, bvid_list = config.BILI_BVID_LIST):
        """ get oid list """
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_oid_and_public_month_task(bvid=bvid, semaphore=semaphore) for bvid in
            bvid_list
        ]
        oid_and_public_month_list = await asyncio.gather(*task_list)
        return oid_and_public_month_list

    async def get_year_month_range(self, start_year_month):
        start_datetime = datetime.datetime.strptime(start_year_month, '%Y-%m')
        current_datetime = datetime.datetime.now()
        year_months = []
        while start_datetime <= current_datetime:
            year_months.append(start_datetime.strftime('%Y-%m'))
            start_datetime = start_datetime.replace(day=1)
            start_datetime = (start_datetime + datetime.timedelta(days=32)).replace(day=1)
        return year_months

    async def get_date_range(self, months, oid):

        date_range = []
        for month in months:
            params = {
                'month': month,
                'type': '1',
                'oid': oid,
            }

            response = requests.get('https://api.bilibili.com/x/v2/dm/history/index', params=params, cookies=self.cookies,
                                    headers=self.headers).json()
            data = response['data']
            if data is not None:
                for item in data:
                    date_range.append(item)
        return date_range

    async def get_danmu_single(self, oid_and_public_month):
        """ get danmu single """
        bvid = oid_and_public_month['bvid']
        oid = oid_and_public_month['oid']
        public_month = oid_and_public_month['public_month']
        months = await self.get_year_month_range(start_year_month=public_month)
        date_range = await self.get_date_range(oid=oid, months=months)
        for date in date_range:
            params = {
                'type': '1',
                'oid': oid,
                'date': date,
            }

            response = requests.get('https://api.bilibili.com/x/v2/dm/web/history/seg.so', params=params, cookies=self.cookies,
                                    headers=self.headers)
            # print(response.text)
            danmus = re.findall(r'.*?([\u4E00-\u9FA5]+).*?', response.text)
            await self.store_csv(danmus, bvid=bvid)
        utils.logger.info(f"[BilibiliCrawler.get_danmu_single] bvid: {bvid} crawler finish !")

    async def get_danmu_task(self, oid_and_public_month, semaphore: asyncio.Semaphore):
        async with semaphore:
            try:
                result = await self.get_danmu_single(oid_and_public_month)
                return result
            except KeyError as ex:
                oid = oid_and_public_month['oid']
                utils.logger.error(
                    f"[BilibiliCrawler.get_danmu_task] have not fund danmu detail oid:{oid}, err: {ex}")
                return None

    async def get_danmu_list(self):
        oid_and_public_month_list = await self.get_oid_and_public_month_list()
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_danmu_task(oid_and_public_month=oid_and_public_month, semaphore=semaphore) for oid_and_public_month in
            oid_and_public_month_list
        ]
        await asyncio.gather(*task_list)
        utils.logger.info(f"[BilibiliCrawler.get_danmu_list] bilibili danmu crawler finish !")

    async def store_csv(self, danmus, bvid):

        file_name = 'data/' + bvid + '-' + str(datetime.date.today()) + '.csv'
        with open(file_name, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            for danmu in danmus:
                utils.logger.info(f"[BilibiliCrawler.get_danmu_list] get bvid: {bvid}, danmu content : {danmu}")
                writer.writerow([danmu])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bilibili danmu crawler program.')
    parser.add_argument('--lt', type=str, help='Login type (qrcode | cookie)',
                        choices=["qrcode", "cookie"], default=config.LOGIN_TYPE)

    args = parser.parse_args()

    spider = BilibiliSpider(login_type=args.lt)
    asyncio.run(spider.start_crawling())
