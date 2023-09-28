import time
import pandas as pd
import datetime
import csv 
import scrapy
import shutil
import requests
from scrapy.http import TextResponse
from scrapy.http import HtmlResponse
from scrapy.selector import Selector
import os
import re



class ScrapBingJobs():       
    
    def __init__(self,param):
        
        self.param=param
        # function to scrap data from website using request and response method
    def data_scrap(self,param):
        self.param=param
        search_word = param
        [company_name,job_title,posted_date,locations,data_urls,img_url,description,applyurl,job_location,job_type,description1,salary_list,scrap_date,search_words]=[[] for _ in range(14)]

        url='https://www.bing.com/jobs/getjob?q='
        for index in range(1,5):
            
            url1=url+search_word+'&rb='+str(index*19)
            response = requests.get(url1)
            response=TextResponse(url=response.url, body=response.text, encoding='utf-8')
            company_name=company_name+(response.xpath("//div[@class='jb_l2_cardlist']//@data-company").getall())
            job_title=job_title+response.xpath("//div[@class='jb_l2_cardlist']//@data-stdtitle").getall()
            posted_date=posted_date+response.xpath("//div[@class='b_footnote jb_postedDate']/text()").getall()
            locations=locations+response.xpath("//li//div[@class='jbovrly_lj b_snippet']/text()").getall()
            data_urls=data_urls+response.xpath("//div[@class='jb_l2_cardlist']//@data-url").getall()
            img_url=img_url+response.xpath("//div[@class='cico']//@src").getall()
        for url in data_urls:
            url1='https://www.bing.com/jobs/jobdetails?&'+url
            response1 = requests.get(url1)
            response1=TextResponse(url=response1.url, body=response1.text, encoding='utf-8')
            description=description+(response1.xpath("//div[@class='jbpnl_description']/text()").getall())    
            applyurl=applyurl+(response1.xpath("//div[@class='jb_applyBtnContainer']//@href").getall())
        for location in locations:
                index = location.find('·')                          # splitting location and job type in two data sets
                str12=[]
                if index!=-1:
                    str1_spit=location.split('·')
                    str11=str1_spit[0]
                    str12=str1_spit[1]
                else:
                    str11=location
                job_location.append(str11)
                job_type.append(str12)
        salary_pattern = r'\₹\d{1,3}(?:,\d{3})*(?:\.\d{2})?'
        html_tags_pattern = r'<[^>]+>'

        for des in description:
            cleaned_string = re.sub(html_tags_pattern, '',str(des))                         # cleaning description by removing web element symbols
            salary_list.append(re.findall(salary_pattern, str(des)))
            description1.append(cleaned_string)
        time1=datetime.datetime.now()
        for i in range(len(img_url)):                                                               # scrap image data and storing in local_folder
            img_url[i]='https://www.bing.com/'+img_url[i]+'.jpg'
            scrap_date.append(time1.date())
            search_words.append(search_word)
            page = requests.get(img_url[i])
            pattern = r'[^a-zA-Z0-9\s]'
            cn= re.sub(pattern, '', company_name[i])
            f_ext = '.jpg'
            f_name = 'img'+str(cn)+'{}'.format(f_ext)
            with open("local_folder1/"+f_name, 'wb') as f:
                f.write(page.content)
        data=list(zip(scrap_date,company_name,job_title,posted_date,job_location,job_type,img_url,search_words,applyurl,salary_list,description1))
        return data
    def create_table(self,parameter):
        self.param=parameter
        Dictionary = {}                                                 #creating data frame for each search and combining and save in new data frame 
        for i in range(len(parameter)):
            data=self.data_scrap(parameter[i])
            column=['scrape_date','company_name','job_title','posted date','location','type_job','logoimage_url','searchword','applyurl','Salary','description']
            Dictionary[i]=pd.DataFrame(data,columns=column)
            
        newtable=pd.concat([Dictionary[i] for i in range(len(parameter))], ignore_index=True)        
        return newtable

def call():
    try:
        shutil.rmtree('./local_folder1')
        print("removing old filestore")
    except OSError:
        pass
    time.sleep(0.5)
    try:
        os.makedirs('./local_folder1')
        print("making new filestore")
    except OSError:
        pass
    parameter=['developer','HR','data scientist','android developer','software engineer','team lead']
    search=ScrapBingJobs(parameter)
    newtable=search.create_table(parameter)
    time1=datetime.datetime.now().strftime("%H_%M_%S")
    filename=parameter[0].replace(" ","")+time1+'.csv'                    
    newtable.to_csv(filename)
    return newtable

         
