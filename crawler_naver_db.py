import requests
from urllib import parse
from bs4 import BeautifulSoup
import datetime
from dateutil.relativedelta import relativedelta
import re
from db_helper import DB_Helper
import pymysql

conn = pymysql.connect(host='163.239.169.54',
                       port=3306,
                       user='s20131533',
                       passwd='s20131533',
                       db='number_to_word',
                       charset='utf8',
                       cursorclass=pymysql.cursors.DictCursor)

db_helper = DB_Helper(conn)



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
start_date = datetime.datetime(2018, 9, 18)
#end date
end_date = datetime.datetime(2018, 9, 18)
# print(start_date.month, start_date.day)
href_base_1 = "http://news.naver.com/main/list.nhn"
href_base_1_catId = "?sid2="
href_base_2 ="&sid1={}&mid=shm&mode=LS2D".format(sid1)
href_date ="&date="
href_page = "&page=" 


#function that displays data
def Get_page_Content(_startPage, soup, url_in_use):
    #method
    # source_code =  requests.get(href_use + href_page+ _startPage)
    # plain_text = source_code.text
    # soup = BeautifulSoup(plain_text, "lxml")

    content_list = []
    content_id_list = []

    # 기사가 이미 DB에 등록되어 있으면 이 딕셔너리에 기사 아이디(article_aid) : 1 로 저장이 된다.
    article_updated_check = {}

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

                    article_title = contents_final.select('dt a')[1].text.strip()
                else:
                    print(contents_final.select('dt a')[0].text.strip())

                    article_title = contents_final.select('dt a')[0].text.strip()

                #href - no needed
                print(contents_final.select('dt a')[0]['href'])


                article_url = contents_final.select('dt a')[0]['href']
                article_collected_date = datetime.datetime.now()


                url = parse.urlparse(article_url)
                article_aid = parse.parse_qs(url.query)['aid'][0]


                rows = db_helper.select_one_column_from_table('article_aid','ArticleTable')
                for row in rows:
                    if article_aid in row.values():
                        already_exist = 1
                        article_updated_check[article_aid] = 1
                        break
                    else:
                        already_exist = 0
                        article_updated_check[article_aid] = 0



                #content
                main_content_after_opening_link = requests.get(contents_final.select('dt a')[0]['href'])
                plain_text__after_opening_link = main_content_after_opening_link.text
                soup_after_opening_link = BeautifulSoup(plain_text__after_opening_link, "lxml")

                article_uploaded_date = soup_after_opening_link.findAll('span', {'class':'t11'})[0].text


                k = soup_after_opening_link.findAll('ul', {'class':'u_likeit_layer _faceLayer'})

                article_good = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list good'})[0].findAll('span')[1].text)
                article_warm = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list warm'})[0].findAll('span')[1].text)
                article_sad = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list sad'})[0].findAll('span')[1].text)
                article_angry = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list angry'})[0].findAll('span')[1].text)
                article_want = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list want'})[0].findAll('span')[1].text)


                text = ''
                article_raw = ''
                for main_contents_after_opening_link in soup_after_opening_link.findAll('div', {'id' : 'articleBody'}):
                    article_raw = article_raw + str(main_contents_after_opening_link.select('div#articleBodyContents')[0])
                    text = text + main_contents_after_opening_link.select('div#articleBodyContents')[0].text.strip()


                content_list.append(text)
                content_id_list.append(article_aid)



                #written by
                print(contents_final.select('dd span.writing')[0].text)
                #date
                print(contents_final.select('dd span.date')[0].text)


                if already_exist == 1:
                    print("UPDATE")
                    article_title_escaped = conn.escape_string(article_title)
                    article_raw_escaped = conn.escape_string(article_raw)

                    db_helper.update_crawled_article(article_url,
                                                     article_title_escaped,
                                                     article_uploaded_date,
                                                     article_collected_date,
                                                     article_good,
                                                     article_warm,
                                                     article_sad,
                                                     article_angry,
                                                     article_want,
                                                     article_aid,
                                                     article_raw_escaped)
                else:
                    print("\tINSERT")
                    article_title_escaped = conn.escape_string(article_title)
                    article_raw_escaped = conn.escape_string(article_raw)

                    db_helper.insert_crawled_article(article_url,
                                                     article_title_escaped,
                                                     article_uploaded_date,
                                                     article_collected_date,
                                                     article_good,
                                                     article_warm,
                                                     article_sad,
                                                     article_angry,
                                                     article_want,
                                                     article_aid,
                                                     article_raw_escaped)




            except:
                print("Error")
            else:
                print("")

    return content_list, content_id_list, article_updated_check

def preprocess_text(text):
    text = text.replace('// flash 오류를 우회하기 위한 함수 추가', '')
    text = text.replace('function _flash_removeCallback() {}', '')
    text = text.strip()
    return text


news_count = 0
start_page =1


pattern_email = re.compile(r'([(]*[0-9a-zA-Z]([-_.]?[0-9a-zA-Z])*[@][0-9a-zA-Z]([-_.]?[0-9a-zA-Z])*.[a-zA-Z]{2,3}[)]*)')
pattern_many_points = re.compile(r'[.]{2,}')
pattern_kor = re.compile(r'[^0-9]*')


#main code start here
#for each category in the list
for cat in category:
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')

    current_date = start_date
    
    while current_date <= end_date:
        current_page = start_page

        while True:
            try:
                #make the paging to the current one, so that you will compare it latter with the previous .....
                #if current page loaded index is less than it was expected to be... Break
                Prev_calculated_page = current_page
                href_use = href_base_1 +href_base_1_catId
                href_use+= str(cat) + href_base_2 + href_date +  format_Date_month(current_date.year, '4') +"" + format_Date_month(current_date.month) +"" + format_Date_month(current_date.day)
                print ("link: " +href_use + href_page+ str( current_page))
                total_total = href_use + href_page+str(current_page)
                source_code =  requests.get(total_total)

                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, "lxml")


                # PAGING
                main_paging = ''
                for main_contents_pager in soup.findAll('div', {'class': 'paging'}):
                    main_paging = main_contents_pager.select('strong')  # to select valid points

                str_main = str(main_paging).replace('[', '').replace(']', '').split(',')
                int_v = str(re.findall(r"[\">](\d*)[</]", str_main[0])).strip('[').strip('\']')  # regular expression to select the paging

                # if error happens, set it the original
                if (int_v == ""):
                    int_v = current_page

                current_page = int(int_v)
                if (current_page < Prev_calculated_page):
                    break  # end of paging Index


                # 기사목록의 한 페이지 내에 있는 여러 기사들을 가져온다.
                # content_list : 그 페이지의 모든 기사 리스트
                # content_id_list : 각 기사에 대응하는 고유 id 리스트
                # article_updated_check : 각 기사id의 업데이트 여부 딕셔너리
                content_list, content_id_list, article_updated_check = Get_page_Content(current_page, soup, total_total)


                # content : 기사 각각
                for content, article_aid in zip(content_list, content_id_list):
                    article_id = db_helper.select_data_from_table_by_something('article_id', 'ArticleTable', 'article_aid', article_aid)

                    for i in pattern_email.findall(content):
                        if type(i) == tuple:
                            pat_string = i[0]
                        else:
                            pat_string = i

                    # 이메일 부분 제거
                    content = content.replace(pat_string, '')


                    for i in pattern_many_points.findall(content):
                        if type(i) == tuple:
                            pat_string = i[0]
                        else:
                            pat_string = i


                    # 연속적인 2개 이상의 점 제거
                    content = content.replace(pat_string, '')


                    content = preprocess_text(content)
                    news_count += 1
                    content = ' '.join(content.split())


                    no_more = 0
                    content_list_div_by_point = content.split('.')
                    for sentence in content_list_div_by_point:

                        # 숫자가 포함되지 않은 문장이면 DB에 입력하지 않는다.
                        if pattern_kor.search(sentence).group() == sentence:
                            continue

                        # 이미 존재해서 추가되지 않고 업데이트만 된 기사라면
                        if article_updated_check[article_aid] == 1:

                            # 하나의 기사에서 처음 한번만 실행되면 되는 부분
                            if no_more == 0:
                                sent_id = db_helper.select_smallest_sent_id_of_one_article(article_id)
                                no_more = 1

                            print("SENT UPDATE\tsent_id: " + str(sent_id))

                            sentence_escaped = conn.escape_string(sentence)
                            db_helper.update_crawled_sentence(sentence_escaped, article_id, sent_id)
                            sent_id += 1

                        # 새로 추가된 기사라면
                        else:
                            print("\tSENT INSERT")
                            sentence_escaped = conn.escape_string(sentence)
                            db_helper.insert_crawled_sentence(sentence_escaped, article_id)

                    article_id += 1


                # increment the page index so that u will load the next page
                current_page += 1


                # =========================================
                if current_page >= 4:
                    break
                # =========================================


                print('\n### DEBUG - current_page : {}'.format(current_page))
            except:
                print("Error")

        current_date += relativedelta(days=1) # to loop back till the end of the the selected date, it addes one day per iteration to reach



        # =========================================
        if current_page >= 4:
            break
        # =========================================
    # =========================================
    if current_page >= 4:
        break
    # =========================================


    
print('total news count :', news_count)
print("Done Crawling " + str(datetime.datetime.today()))
