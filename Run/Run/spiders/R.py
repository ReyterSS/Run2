import scrapy
from scrapy import Request
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
import time
import datetime
from datetime import datetime
import mysql
from mysql import connector
import time

Base = declarative_base()



#
# surname = 'McCarthy'  # 'Blunt'#'Perry'  #f/n: Stuarts/n: Bluntclub: Epsom Oddballs
# firstname = 'Patrick'  # 'Stuart'#'Edmund'
# club = 'Epsom oddballs'  # 'Epsom Oddballs' #'Ranelagh'

class Character(Base):
    __tablename__ = 'performance_history'
    id = Column(Integer, primary_key=True, autoincrement=True)
    event = Column(String(255), primary_key=True)
    time = Column(String(255))
    race = Column(String(255))
    SSS = Column(String(255))
    vSSS = Column(String(255))
    date = Column(String(255))
    last = Column(String(255))


class RSpider(scrapy.Spider):
    name = "R"


    def __init__(self, surname=None,firstname=None, club=None,*args, **kwargs):#,
        super(RSpider, self).__init__(*args, **kwargs)
        self.surname = surname
        self.firstname = firstname
        self.club = club

    def start_requests(self):
        url = "https://runbritainrankings.com/runners/runnerslookup.aspx"
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://runbritainrankings.com',
            'Pragma': 'no-cache',
            'Referer': 'https://runbritainrankings.com/runners/runnerslookup.aspx',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Cookie': '_ga=GA1.1.79144015.1742471176; __gads=ID=050ff401fb8f9a31:T=1742471176:RT=1742471176:S=ALNI_MY-z6abHDVU2E-IMzcRi9WkEqMN8g; __gpi=UID=000010678a0ecbd3:T=1742471176:RT=1742471176:S=ALNI_MaSfBsZMbw2uNUlzvX8hJcFlvZOTA; __eoi=ID=ed19047d3f32abb4:T=1742471176:RT=1742471176:S=AA-AfjZm7ItHG6IviounBY35HroD; _ga_5ST66NS5G2=GS1.1.1742471176.1.1.1742471286.0.0.0'
        }
        club = str(self.club).replace('_',' ')
        payload = '__EVENTTARGET=&__EVENTARGUMENT=&__VIEWSTATE=%2FwEPDwULLTE5MjY0NTk0NDcPZBYCZg9kFgQCAQ9kFgICCQ9kFgJmDxYCHgRUZXh0BaACPCEtLSBHb29nbGUgdGFnIChndGFnLmpzKSAtLT4NPHNjcmlwdCBhc3luYyBzcmM9Imh0dHBzOi8vd3d3Lmdvb2dsZXRhZ21hbmFnZXIuY29tL2d0YWcvanM%2FaWQ9Ry01U1Q2Nk5TNUcyIj48L3NjcmlwdD4NPHNjcmlwdD4NICB3aW5kb3cuZGF0YUxheWVyID0gd2luZG93LmRhdGFMYXllciB8fCBbXTsNICBmdW5jdGlvbiBndGFnKCl7ZGF0YUxheWVyLnB1c2goYXJndW1lbnRzKTt9DSAgZ3RhZygnanMnLCBuZXcgRGF0ZSgpKTsNDSAgZ3RhZygnY29uZmlnJywgJ0ctNVNUNjZOUzVHMicpOw08L3NjcmlwdD4NZAIDD2QWAgIBDxYCHgVhbGlnbgUGY2VudGVyFgICAQ8WAh4Fd2lkdGgFBTk3NXB4FgJmD2QWAmYPZBYEAgEPZBYGAg0PDxYCHgdWaXNpYmxlaBYCHgdvbmNsaWNrBThyZXR1cm4gZm5BcmVZb3VTdXJlKCdBcmUgeW91IHN1cmUgeW91IHdhbnQgdG8gbG9nb3V0ID8nKWQCDw8PFgIfAAVeRm9yZ290dGVuIHlvdXIgbG9naW4gZGV0YWlscz8gPGEgaHJlZj0iL3VzZXIvdXNlcnBhc3N3b3JkcmVzZXRyZXF1ZXN0LmFzcHgiPlJlc2V0IHBhc3N3b3JkPC9hPmRkAhEPFgIfAAUTQ2xhaW0geW91ciBIYW5kaWNhcGQCAw9kFgICCw8PFgIfA2dkZBgCBR5fX0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WAQUaY3RsMDAkYmFubmVyJGNoa1JlbWVtYmVyTWUFF2N0bDAwJGNwaEJvZHkkZ3ZSdW5uZXJzDzwrAAwBCGZkeOBeDGciSW4Qp1exFIuQF5%2BtIm2QPVph32lUvA%2FPiPc%3D&__VIEWSTATEGENERATOR=EF3E82A1&__EVENTVALIDATION=%2FwEdAAmcTn9UUizv40ZhPnX4QLWeVgiRa%2BCk6Skj7uv4bP83L79G4uluF6q3ur81q5yYP5dwbHtrMmfbQaoWKtWM7%2F4UJ7%2FQPpCW3USfbvZI9vOo3nVJaMzMPl2d%2FlFnbzIbEwJkI9Ay1pz%2FKDqm8qyv%2BQAB%2Fwu7AotYdBfwvy%2FLQPwN4p6TAmskBHZomb7P3u6YRGRCUfHpqN9pRiC0TI8PkaADHDrZffhbsZNZ5OEJ0390mw%3D%3D&ctl00%24banner%24txtEmail=&ctl00%24banner%24txtPassword=&ctl00%24cphBody%24txtSurname='f'{self.surname}''&ctl00%24cphBody%24txtFirstName='f'{self.firstname}'f'&ctl00%24cphBody%24txtClub={club}''&ctl00%24cphBody%24btnLookup=Lookup'
        print(payload)
        yield Request(
            url=url,
            headers=headers,
            body=payload,
            method='POST',
            callback=self.parse
        )

    def parse(self, response):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Cookie': '_ga=GA1.1.79144015.1742471176; __gads=ID=050ff401fb8f9a31:T=1742471176:RT=1742471176:S=ALNI_MY-z6abHDVU2E-IMzcRi9WkEqMN8g; __gpi=UID=000010678a0ecbd3:T=1742471176:RT=1742471176:S=ALNI_MaSfBsZMbw2uNUlzvX8hJcFlvZOTA; __eoi=ID=ed19047d3f32abb4:T=1742471176:RT=1742471176:S=AA-AfjZm7ItHG6IviounBY35HroD; _ga_5ST66NS5G2=GS1.1.1742471176.1.1.1742471319.0.0.0'
        }
        url = response.urljoin(response.xpath('//td[@align="center"]/a/@href').get())
        if url is not None:
            yield response.follow(url=url, headers=headers, callback=self.parse_items)
        if response.xpath('//span[@id="cphBody_lblResultsErrorMessage"][contains(text(), "No runners match criteria")]').get():
            print('User not found')
        if response.status == 404:
            print('404')

    @staticmethod
    def database():
        username = 'root'
        password = '1111'
        host = 'localhost'
        port = '3306'  # Порт по умолчанию для MySQL
        database = 'bbb'
        DATABASE_URL = f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(DATABASE_URL)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    def parse_items(self, response):
        all_blocks = response.xpath(
            '//div[@class="profile-panel"]/div[@id="cphBody_performancespanel_pnlPerformancesMain"]//tbody/tr')
        session = self.database()
        for i in all_blocks:
            try:
                event = i.xpath("td[@sorttable_customkey]//text()").get()
            except:
                event = ''
            try:
                time = i.xpath("td[@sorttable_customkey][2]//text()").get()
            except:
                time = ''
            try:
                race = i.xpath("td[3]//text()").get()
            except:
                race = ''
            try:
                SSS = i.xpath('.//td[@align]/text()').get()
            except:
                SSS = ''
            try:
                vSSS = i.xpath('.//td[@align][2]//text()').get()
            except:
                vSSS = ''
            try:
                date = i.xpath('.//td[@align][3]//text()').get()
                date_obj = datetime.strptime(date, "%d %b %y")
                date = date_obj.strftime("%Y-%m-%d")
            except:
                date = ''
            try:
                last = i.xpath('.//td[@align="center"]//span[@title]//text()').get()
            except:
                last = ''
            try:
                insertion_query = Character(event = event, time = time, race=race, SSS=SSS, vSSS=vSSS, date = date, last=last) # # race = Column(String(255))
                session.add(insertion_query)
            except:
                pass
            session.commit()

        session.close()

# yield {
#     'Event': event,
#     'Time': time,
#     'Race': race,
#     'SSS': SSS,
#     'vSSS': vSSS,
#     'Date': date,
#     'Last': last
# }