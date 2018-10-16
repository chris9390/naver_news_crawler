import requests
from urllib import parse
from bs4 import BeautifulSoup
import datetime
from dateutil.relativedelta import relativedelta
import re
from db_helper import DB_Helper
import pymysql
import my_news_normalizer


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

print ("Start Crawling" + str( datetime.datetime.today()))

#category numbers
# 100 : 정치 , 101 : 경제 , 102 : 사회 , 103 : 생활/문화 , 104 : 세계 , 105 : IT/과학
sid1 = 104


# sid1 : 104
category = [231, 232, 233, 234, 322]
# sid1 : 105
#category = [731, 226,227,230, 732, 283,229,228]



#start date
start_date = datetime.datetime(2018, 10, 5)
#end date
end_date = datetime.datetime(2018, 10, 5)

href_base_1 = "http://news.naver.com/main/list.nhn"
href_base_1_catId = "?sid2="
href_base_2 ="&sid1={}&mid=shm&mode=LS2D".format(sid1)
href_date ="&date="
href_page = "&page="



naver_news_category_sid1 = {'100' : '정치', '101' : '경제', '102' : '사회', '103' : '생활/문화', '104' : '세계', '105' : 'IT/과학'}

# 정치
naver_news_category_100_sid2 = {'264' : '청와대', '265' : '국회/정당', '268' : '북한', '266' : '행정', '267' : '국방/외교', '269' : '정치 일반'}
# 경제
naver_news_category_101_sid2 = {'259' : '금융', '258' : '증권', '261' : '산업/재계', '771' : '중기/벤처', '260' : '부동산', '262' : '글로벌 경제', '310' : '생활경제', '263' : '경제 일반'}
# 사회
naver_news_category_102_sid2 = {'249' : '사건사고', '250' : '교육', '251' : '노동', '254' : '언론', '252' : '환경', '59b' : '인권/복지', '255' : '식품/의료', '256' : '지역', '276' : '인물', '257' : '사회 일반'}
# 생활/문화
naver_news_category_103_sid2 = {'241' : '건강정보', '239' : '자동차/시승기', '240' : '도로/교통', '237' : '여행/레저', '238' : '음식/맛집', '376' : '패션/뷰티', '242' : '공연/전시', '243' : '책', '244' : '종교', '248' : '날씨', '245' : '생활문화 일반'}
# 세계
naver_news_category_104_sid2 = {'231' : '아시아/호주', '232' : '미국/중남미', '233' : '유럽', '234' : '중동/아프리카', '322' : '세계 일반'}
# IT/과학
naver_news_category_105_sid2 = {'731' : '모바일', '226' : '인터넷/SNS', '227' : '통신/뉴미디어', '230' : 'IT 일반', '732' : '보안/해킹', '283' : '컴퓨터', '229' : '게임/리뷰', '228' : '과학 일반'}



#function that displays data
def Get_page_Content(_startPage, soup, url_in_use):

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

                article_sid1 = parse.parse_qs(url.query)['sid1'][0]
                article_sid1_kor = naver_news_category_sid1[article_sid1]

                article_sid2 = parse.parse_qs(url.query)['sid2'][0]
                if article_sid1 == '100':
                    article_sid2_kor = naver_news_category_100_sid2[article_sid2]
                elif article_sid1 == '101':
                    article_sid2_kor = naver_news_category_101_sid2[article_sid2]
                elif article_sid1 == '102':
                    article_sid2_kor = naver_news_category_102_sid2[article_sid2]
                elif article_sid1 == '103':
                    article_sid2_kor = naver_news_category_103_sid2[article_sid2]
                elif article_sid1 == '104':
                    article_sid2_kor = naver_news_category_104_sid2[article_sid2]
                elif article_sid1 == '105':
                    article_sid2_kor = naver_news_category_105_sid2[article_sid2]



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


                # <br>, <p>태그는 '#br#', '#p#' 로 표시를 바꿔서 표시를 남긴다음 my_news_normalizer.py 에서 개행해준다.
                plain_text__after_opening_link = plain_text__after_opening_link.replace('<br>', '#br#')
                plain_text__after_opening_link = plain_text__after_opening_link.replace('<p>', '#p#')


                soup_after_opening_link = BeautifulSoup(plain_text__after_opening_link, "lxml")

                if soup_after_opening_link.findAll('span', {'class':'t11'}):
                    article_uploaded_date = soup_after_opening_link.findAll('span', {'class':'t11'})[0].text
                # 기사 업로드 날짜의 태그 형식이 다른 경우 별도의 가공이 필요(네이버 스포츠인 경우)
                elif soup_after_opening_link.findAll('div', {'class':'news_headline'})[0].findAll('div', {'class' : 'info'})[0].findAll('span')[1].text:
                    date_temp = soup_after_opening_link.findAll('div', {'class':'news_headline'})[0].findAll('div', {'class' : 'info'})[0].findAll('span')[1].text

                    # '최종수정' 이란 말 잘라내고 양옆 공백 제거
                    date_temp = (date_temp.strip())[4:].strip()

                    date_temp = date_temp.replace('.', '-')

                    if '오전' in date_temp:
                        date_temp = date_temp.replace('오전', '')
                    elif '오후' in date_temp:
                        date_temp = date_temp.replace('오후', '')
                        time_part = date_temp.split(':')[0].strip()[-2:]

                        # ':'를 붙인 이유는 날짜 부분을 replace하는 것을 방지하기 위해
                        date_temp = date_temp.replace(time_part+":", str(int(time_part) + 12)+":")

                    article_uploaded_date = date_temp



                # 기사의 사진에 관한 설명 부분
                about_photo_text = ''
                about_photo_lst = soup_after_opening_link.findAll('em', {'class' : 'img_desc'})
                if about_photo_lst:
                    about_photo_text = about_photo_lst[0].text


                '''
                k = soup_after_opening_link.findAll('ul', {'class':'u_likeit_layer _faceLayer'})

                article_good = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list good'})[0].findAll('span')[1].text)
                article_warm = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list warm'})[0].findAll('span')[1].text)
                article_sad = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list sad'})[0].findAll('span')[1].text)
                article_angry = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list angry'})[0].findAll('span')[1].text)
                article_want = int(soup_after_opening_link.findAll('li', {'class':'u_likeit_list want'})[0].findAll('span')[1].text)
                '''


                text = ''
                article_raw = ''
                if soup_after_opening_link.findAll('div', {'id' : 'articleBody'}):
                    for main_contents_after_opening_link in soup_after_opening_link.findAll('div', {'id' : 'articleBody'}):
                        article_raw = article_raw + str(main_contents_after_opening_link.select('div#articleBodyContents')[0])
                        text = text + main_contents_after_opening_link.select('div#articleBodyContents')[0].text.strip()
                elif soup_after_opening_link.findAll('div', {'id' : 'newsEndContents'}):
                    for main_contents_after_opening_link in soup_after_opening_link.findAll('div', {'id' : 'newsEndContents'}):
                        article_raw = article_raw + str(main_contents_after_opening_link)
                        text = text + main_contents_after_opening_link.text.strip()


                article_raw = preprocess_text(article_raw)


                # 기사에 사진에 관한 설명은 삭제
                if about_photo_text != '':
                    text = text.replace(about_photo_text, '')


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
                                                     article_aid,
                                                     article_raw_escaped,
                                                     article_sid1_kor,
                                                     article_sid2_kor)

                else:
                    print("\tINSERT")
                    article_title_escaped = conn.escape_string(article_title)
                    article_raw_escaped = conn.escape_string(article_raw)

                    db_helper.insert_crawled_article(article_url,
                                                     article_title_escaped,
                                                     article_uploaded_date,
                                                     article_collected_date,
                                                     article_aid,
                                                     article_raw_escaped,
                                                     article_sid1_kor,
                                                     article_sid2_kor)



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
start_page = 1


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
                href_use = href_base_1 + href_base_1_catId
                href_use += str(cat) + href_base_2 + href_date + format_Date_month(current_date.year, '4') + "" + format_Date_month(current_date.month) + "" + format_Date_month(current_date.day)
                print ("link: " + href_use + href_page + str(current_page))
                total_total = href_use + href_page + str(current_page)
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
                    sent_ids = db_helper.select_sent_id(article_id)


                    content = preprocess_text(content)
                    news_count += 1
                    content = ' '.join(content.split())


                    no_more = 0


                    # 기사를 '.'을 포함한 패턴을 기준으로 해서 나누자.
                    content_div_by_point = my_news_normalizer.my_news_normalizer(content)

                    if content_div_by_point == None:
                        continue



                    already_exist = 0
                    sent_ids_idx = 0
                    sentence_count = 0
                    for sentence in content_div_by_point:
                        '''
                        # 숫자가 포함되지 않은 문장이면 DB에 입력하지 않는다.
                        if pattern_kor.search(sentence).group() == sentence:
                            continue
                        '''

                        # 공백 문장이면 DB에 입력하지 않는다.
                        if sentence == '':
                            continue


                        # 이미 존재해서 추가되지 않고 업데이트만 된 기사라면
                        if article_updated_check[article_aid] == 1:
                            '''
                            sent_id = sent_ids[sent_ids_idx]['sent_id']

                            print("SENT UPDATE\tsent_id: " + str(sent_id))

                            sentence_escaped = conn.escape_string(sentence)
                            db_helper.update_crawled_sentence(sentence_escaped, article_id, sent_id)
                            '''
                            print(" ** 이미 존재하는 기사 ** ")
                            already_exist = 1
                            break

                        # 새로 추가된 기사라면
                        else:
                            print("\tSENT INSERT")
                            sentence_escaped = conn.escape_string(sentence)
                            db_helper.insert_crawled_sentence(sentence_escaped, article_id)

                        sent_ids_idx += 1
                        sentence_count += 1



                    if already_exist == 0:
                        # 각 기사의 문장 개수 DB에 저장
                        db_helper.update_article_sent_count(article_id, sentence_count)



                # increment the page index so that u will load the next page
                current_page += 1



                # =========================================
                if current_page == 3:
                    break
                # =========================================


                print('\n### DEBUG - current_page : {}'.format(current_page))
            except:
                print("Error")

        current_date += relativedelta(days=1) # to loop back till the end of the the selected date, it addes one day per iteration to reach



        # =========================================
        if current_page == 3:
            break
        # =========================================

    # =========================================
    if current_page == 3:
        break
    # =========================================


    
print('total news count :', news_count)
print("Done Crawling " + str(datetime.datetime.today()))
