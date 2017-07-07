from pyspider.libs.base_handler import *
import random
import MySQLdb


class Handler(BaseHandler):
    crawl_config = {

    }

    def __init__(self):
        self.db = MySQLdb.connect('localhost','root', 'workhard', 'wenda', charset='utf-8')

    def add_question(self,title, content):
        try:
            cursor = self.db.cursor()
            sql = 'insert into question(title, content, user_id, created_date, comment_count) values ("%s","%s",%d, %s, 0)' % (
                title, content, random.randint(1, 10), 'now()')
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.roolback()

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
        self.add_question(title,content)
        return {
            'url': response.url,
            'title': title,
            'content': content
        }
