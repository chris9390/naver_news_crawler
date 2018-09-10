import requests
from bs4 import BeautifulSoup
import datetime
from dateutil.relativedelta import relativedelta
import re

try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen # py3k

#utility functions
def format_Date_month(value, digit='2'):
    if digit=='2':
        return  '{:02d}'.format(value)
    else:
        return  '{:04d}'.format(value)

def format_all_date(year, month, day):
    format_Date_month(year, '4') +"" + format_Date_month(month) +"" + format_Date_month(day)


#Program starts here

print ("Start Crawling"+str( datetime.datetime.today()))
#category numbers
sid1 = 105
# category = [230]
# category = [264,265,266,267,268] # sid1=100 excluding category 269
# category = [259,258,261,771,260,262,310] # sid1=101 excluding category 263
# category = [249,250,251,254,252,'59b',255,256] # sid1=102 excluding category 257
# category = [241,239,240,237,238,376,242,243,244,248] # sid1=103 excluding category 245
# category = [231,232,233,234] # sid1=104 excluding category 322
# category = [731,226,227,732,283,229,228] # sid1=105 excluding category 230
category = [226,227,283,229,228] # sid1=105 excluding category 230

#start date
start_date = datetime.datetime(2011, 1, 1)
#end date
end_date = datetime.datetime(2011, 12, 31)
# print(start_date.month, start_date.day)
href_base_1 = "http://news.naver.com/main/list.nhn"
href_base_1_catId = "?sid2="
href_base_2 ="&sid1={}&mid=shm&mode=LS2D".format(sid1)
href_date ="&date="
href_page = "&page=" 

start_page =1

#function that displays data
def Get_page_Content(_startPage, soup, url_in_use):
    #method
    # source_code =  requests.get(href_use + href_page+ _startPage)
    # plain_text = source_code.text
    # soup = BeautifulSoup(plain_text, "lxml")
    content_list = []

    print("on the **"+str( _startPage)+" **page")
    print("\n\n\n ******************************************************************************")
    print(url_in_use+"\n\n")
    for main_contents in soup.findAll('div', {'class': 'list_body newsflash_body'}):
        main_contents = main_contents.select('ul li dl')
        for contents_final in main_contents:
            print ("\n\nNew article detail" )
            try:
                #Title
                print("DT is "+ str(len(contents_final.select('dt'))))

                if( len(contents_final.select('dt')) == 2):
                    print(contents_final.select('dt a')[1].text.strip())
                else:
                    print(contents_final.select('dt a')[0].text.strip())

                #href - no needed
                print(contents_final.select('dt a')[0]['href'])

                #content
                main_content_after_opening_link = requests.get(contents_final.select('dt a')[0]['href'])
                plain_text__after_opening_link = main_content_after_opening_link.text
                soup_after_opening_link = BeautifulSoup(plain_text__after_opening_link, "lxml")
                for main_contents_after_opening_link in soup_after_opening_link.findAll('div', {'class': 'article_body font1 size4'}):
                    text = main_contents_after_opening_link.select('div#articleBodyContents')[0].text.strip()
                    content_list.append(text)

                #written by
                print(contents_final.select('dd span.writing')[0].text)
                #date
                print(contents_final.select('dd span.date')[0].text)

            except:
                print("Error in the HTML Tag")
            else:
                print("")
    return content_list

def preprocess_text(text):
    text = text.replace('// flash 오류를 우회하기 위한 함수 추가', '')
    text = text.replace('function _flash_removeCallback() {}', '')
    text = text.strip()
    return text


news_count = 0

#main code start here
#for each category in the list
for cat in category:
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    filename = 'output_{}_{}_{}_{}.txt'.format(start_date_str, end_date_str, sid1, cat)
    fw = open(filename, 'w', encoding='utf-8')
    
    current_date = start_date
    
    while current_date <= end_date:
        current_page = start_page
        # print("1")
    
        while True:
            try:
                # print ("2")
                #make the paging to the current one, so that you will compare it latter with the previous .....
                #if current page loaded index is less than it was expected to be... Break
                Prev_calculated_page = current_page
                href_use = href_base_1 +href_base_1_catId
                href_use+= str(cat) + href_base_2 + href_date +  format_Date_month(current_date.year, '4') +"" + format_Date_month(current_date.month) +"" + format_Date_month(current_date.day)
                print ("link: " +href_use + href_page+ str( current_page))
                total_total = href_use + href_page+str(current_page)
                source_code =  requests.get(total_total)
                print("3")
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, "lxml")
                print ("4")

                # PAGING
                main_paging = ''
                for main_contents_pager in soup.findAll('div', {'class': 'paging'}):
                    main_paging = main_contents_pager.select('strong')  # to select valid points

                str_main = str(main_paging).replace('[', '').replace(']', '').split(',')
                int_v = str(re.findall(r"[\">](\d*)[</]", str_main[0])).strip('[').strip(
                    '\']')  # regular expression to select the paging
                # if error happens, set it the original
                if (int_v == ""):
                    int_v = current_page

                current_page = int(int_v)
                if (current_page < Prev_calculated_page):
                    break  # end of paging Index

                content_list = Get_page_Content(current_page, soup,total_total)

                for content in content_list:
                    content = preprocess_text(content)
                    news_count += 1
                    content = ' '.join(content.split())
                    fw.write(content)
                    fw.write('\n')

                # increment the page index so that u will load the next page
                current_page += 1

                print('### DEBUG - current_page : {}'.format(current_page))
            except:
                print("Error happen in the HTML tag")

        current_date += relativedelta(days=1) # to loop back till the end of the the selected date, it addes one day per iteration to reach

    fw.close()
    
print('total news count :', news_count)
print("Done Crawling " + str(datetime.datetime.today()))
