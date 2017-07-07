from pyspider.libs.base_handler import *
import random
# import MySQLdb
import pymongo
from mongoengine import *

'''
方法2： pymongo 连接 mongodb数据库

# 连接mongodb
# def get_coll():
#     client = pymongo.MongoClient('127.0.0.1', 27017)
#     db = client.spider
#     coll = db.spider_coll
#     return coll
'''

'''
方法3: mongoengine连接mongodb数据库
'''
connect('v2ex')  # mongoengine连接mongodb


class Question(Document):
    title = StringField()
    content = StringField()

    def __str__(self):
        return '<Question %s>'%self.title


class Handler(BaseHandler):
    crawl_config = {

    }
    '''
    方法1： 连接mysql数据库

    # def __init__(self):
    #     self.db = MySQLdb.connect('localhost','root', 'workhard', 'wenda', charset='utf-8')
    #
    # def add_question(self,title, content):
    #     try:
    #         cursor = self.db.cursor()
    #         sql = 'insert into question(title, content, user_id, created_date, comment_count) values ("%s","%s",%d, %s, 0)' % (
    #             title, content, random.randint(1, 10), 'now()')
    #         cursor.execute(sql)
    #         self.db.commit()
    #     except Exception as e:
    #         print(e)
    #         self.db.roolback()
    '''

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://v2ex.com', callback=self.index_page, validate_cert=False)

    @config(age=10 * 24 * 60 *60)
    def index_page(self, response):
        for each in response.doc('a[href^="https://www.v2ex.com/?tab="]').items():
            self.crawl(each.attr.href, callback=self.tag_page,validate_cert=False)

    @config(priority=2)
    def tag_page(self,response):
        for each in response.doc('a[href^="https://www.v2ex.com/go/"]').items():
            self.crawl(each.attr.href, callback=self.board_page, validate_cert=False)

    @config(priority=2)
    def board_page(self,response):
        for each in response.doc('a[href^="https://www.v2ex.com/t"').items():
            url = each.attr.href
            if url.find('#reply')>0:
                self.crawl(url, callback=self.detail_page, validate_cert=False)
        for each in response.doc('page_normal').items():
            self.crawl(each.attr.href, callback=self.detail_page, validate_cert=False)

    @config(priority=2)
    def detail_page(self,response):
        title = response.doc('h1').text()
        content = response.doc('div.topic_content').html().replace('"', '\"')
        # 方法1
        # self.add_question(title,content)
        # 方法2
        # question = {'title': title, 'content': content}
        # coll =get_coll()
        # coll.insert(question)
        #方法3
        question = Question(title=title, content=content)
        question.save()

        return {
            'url': response.url,
            'title': title,
            'content': content
        }
