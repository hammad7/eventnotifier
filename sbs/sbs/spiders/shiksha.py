import scrapy, csv, re, nltk
import tldextract
from scrapy.linkextractors import LinkExtractor
import re,nltk,duckling,json
from . import Db
from . import conf

# targetUrl = "https://pglaw.puchd.ac.in"
# targetDomain = "puchd.ac.in"

class ShikshaSpider(scrapy.Spider):
    name = 'shiksha'
    # allowed_domains = ["nsit.ac.in"]
    allowed_domains = [conf.targetDomain]
    start_urls = [
        # "http://www.nsit.ac.in/",
        # "http://www.uceed.iitb.ac.in/2022/",
        # "https://pglaw.puchd.ac.in"
        conf.targetUrl
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.05, # +random
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1 #8 defualt
    }

    dateparser = duckling.wrapper.DucklingWrapper()
    db = Db.Db()

    def parse(self, response):
        # self.db.set_notify([3,2])
        # continue
        depth = response.meta.get('depth')
        # yield { "url": response.url, "body": response.body} ### Mysql query to update
        ####### yield diff_callback
        le = LinkExtractor()
        pagelinks = le.extract_links(response)
        # master_domain = tldextract.extract(response.url).domain

        print(self.doMainStuff(response)) ### yield ??
        yield {"url": response.url, "depth":depth, "queue":len(self.crawler.engine.slot.scheduler),"running":len(self.crawler.engine.slot.inprogress)}
        with open("test.txt", "a") as myfile:
            writer = csv.writer(myfile)
            writer.writerow([response.url, depth])
            # writer.writerow([response.url, depth, response.xpath('/html/body//text()[not(ancestor::table)]').extract(), response.css("table").getall(), response.body])
            # myfile.write(response.url + "" + depth})
        if depth < 2000: ## tested
            for a in pagelinks:
                # current_domain = tldextract.extract(a.url).domain
                # if master_domain == current_domain and depth < 2:
                    yield response.follow(a.url, callback=self.parse)

    def doMainStuff(self, response):
        tableLessDoc = response.xpath('/html/body//text()[not(ancestor::table)]').extract()
        tableLessParaCleaned = self.cleanText(tableLessDoc)
        docSentences = self.sentencise(tableLessParaCleaned) #### this can be totally avoided, changes can be para ise
        docSentencesDates = self.parse_dates(docSentences)
        self.persist(docSentencesDates,response.url)
        return docSentencesDates,response.url 

    def persist(self, data,url):
        ## select for this url
        ## if nothing, add- url,"",newSent,[intent, typeOpenClosed,oldDate,NewDate], 0 "later render new urls by old=blank" 
        ## if found- replace oldSent by new below if notification not pending,update new by newHtml
        # url, oldSent, newSent , [intent, typeOpenClosed,oldDate,NewDate], notify-1  
        # oldv = self.db.getData(response.url)
        # if olfv empty, ie new mappings, set oldVal = ""
        dbData = []
        for row in data:
            dbData.append([url,"",row[0],json.dumps(row[1]),0])
        self.db.insert(dbData)

    def cleanText(self, textStrings): 
        tableLessDocCleaned = []
        for row in textStrings:
            cleaned = re.sub(r"[\n\t\r ]+" ," ",row).strip()
            if cleaned !="":
                tableLessDocCleaned.append(cleaned)
        return tableLessDocCleaned


    def sentencise(self, paraList):
        sentences = []
        for para in paraList:
            pSent = nltk.sent_tokenize(para)
            sentences+=[sent.strip() for sent in pSent if sent.strip()!=""]
        return sentences


    def parse_dates(self, docSentences):
        docSentencesDates=[]
        for docSentence in docSentences:
            sdateList = self.dateparser.parse_time(docSentence)
            dateInfoList = {}
            for sdate in sdateList:
                to_ = sdate["value"]["value"]["to"] if (isinstance(sdate["value"]["value"], dict) and "to" in sdate["value"]["value"]) else ""
                from_ = sdate["value"]["value"]["from"] if (isinstance(sdate["value"]["value"], dict) and "from" in sdate["value"]["value"]) else ""
                val_ = sdate["value"]["value"] if (isinstance(sdate["value"]["value"], str) ) else ""
                grain_ = sdate["value"]["grain"] if ("grain" in sdate["value"]) else ""
                dateInfoList[sdate["text"]+"_"+str(sdate["start"])] = {
                    "text":sdate["text"],
                    "start":sdate["start"],
                    "end":sdate["end"],
                    "grain": grain_,
                    "from": from_,
                    "to": to_,
                    "val":val_}
            if len(dateInfoList)>0: ## ignore with no date
                docSentencesDates.append((docSentence, dateInfoList))
        return docSentencesDates




# import csv
# import lxml.html
# with open("test.txt", "r") as myfile:
#     asd = csv.reader(myfile)
#     utf8_parser = lxml.html.HTMLParser(encoding='utf-8')
#     for a in asd:
#         # doc = lxml.html.parse(a[2])
#         html_tree = lxml.html.document_fromstring(a[2] , parser=utf8_parser)
#         print(html_tree.text_content())
#         break



# https://pglaw.puchd.ac.in/#:~:text=(3 YEARS) 2022 Entrance Test
# https://pglaw.puchd.ac.in/#:~:text=last%20date%20for%20registration:%20July%2008,%202022
# Or*** to avoid text mismatch, and raw data storage, just the date["text"]
# https://pglaw.puchd.ac.in/#:~:text=after%20July%2019,%202022


# tables = response.css("table").getall()








