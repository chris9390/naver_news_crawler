import requests
from bs4 import BeautifulSoup
import datetime
from dateutil.relativedelta import relativedelta
import re
from urllib import parse
import json
import my_news_normalizer




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
start_date = datetime.datetime(2018, 11, 26)
#end date
end_date = datetime.datetime(2018, 11, 26)
# print(start_date.month, start_date.day)
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


articles = []
finish_flag = 0

#function that displays data
def Get_page_Content(_startPage, soup, url_in_use):

    content_list = []

    #articles = []

    #print("on the **"+str( _startPage)+" **page")
    #print("\n\n\n ******************************************************************************")
    #print(url_in_use+"\n\n")
    for main_contents in soup.findAll('div', {'class': 'list_body newsflash_body'}):
        main_contents = main_contents.select('ul li dl')
        for contents_final in main_contents:
            #print ("\n\nNew article detail" )
            try:

                each_article = {}

                #Title
                #print("DT is "+ str(len(contents_final.select('dt'))))

                article_title = contents_final.select('dt a')[1].text.strip()
                if '[포토]' in article_title:
                    print(article_title + '\t(포토기사 제외)')
                    continue
                each_article['title'] = article_title

                article_url = contents_final.select('dt a')[0]['href']
                url = parse.urlparse(article_url)
                article_sid1 = parse.parse_qs(url.query)['sid1'][0]
                article_sid1_kor = naver_news_category_sid1[article_sid1]

                each_article['sid1'] = article_sid1_kor

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

                each_article['sid2'] = article_sid2_kor

                '''
                if( len(contents_final.select('dt')) == 2):
                    print(contents_final.select('dt a')[1].text.strip())

                else:
                    print(contents_final.select('dt a')[0].text.strip())
                '''


                #href - no needed
                #print(contents_final.select('dt a')[0]['href'])




                #content
                main_content_after_opening_link = requests.get(contents_final.select('dt a')[0]['href'])
                plain_text__after_opening_link = main_content_after_opening_link.text
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


                each_article['uploaded_date'] = article_uploaded_date


                # 기사의 사진에 관한 설명 부분
                about_photo_text = ''
                about_photo_lst = soup_after_opening_link.findAll('em', {'class': 'img_desc'})
                if about_photo_lst:
                    about_photo_text = about_photo_lst[0].text

                text = ''
                if soup_after_opening_link.findAll('div', {'id': 'articleBody'}):
                    for main_contents_after_opening_link in soup_after_opening_link.findAll('div',{'id': 'articleBody'}):
                        text = text + main_contents_after_opening_link.select('div#articleBodyContents')[0].text.strip()
                elif soup_after_opening_link.findAll('div', {'id': 'newsEndContents'}):
                    for main_contents_after_opening_link in soup_after_opening_link.findAll('div', {'id': 'newsEndContents'}):
                        text = text + main_contents_after_opening_link.text.strip()


                # 기사에 사진에 관한 설명은 삭제
                if about_photo_text != '':
                    text = text.replace(about_photo_text, '')

                content_list.append(text)

                text = preprocess_text(text)
                text = ' '.join(text.split())
                divided_text = my_news_normalizer.my_news_normalizer(text)
                if divided_text == None:
                    continue
                each_article['text'] = divided_text


                articles.append(each_article)
                print(len(articles))


                if len(articles) == 100:
                    finish_flag = 1
                    break

                '''
                #written by
                print(contents_final.select('dd span.writing')[0].text)
                #date
                print(contents_final.select('dd span.date')[0].text)
                '''

            except:
                print("Error in the HTML Tag")
            else:
                print("")


        if len(articles) == 100:
            finish_flag = 1
            break


    article_count = len(articles)
    print('\n========== 지금까지 수집된 총 기사 개수 : ' + str(article_count) + ' ==========\n')
    #json_file_name = 'articles' + '_page' + str(_startPage) + '.json'
    with open('./articles.json', 'w') as f:
        json.dump(articles, f, ensure_ascii=False, indent=4, sort_keys=True)

    return content_list, article_count, finish_flag

def preprocess_text(text):
    text = text.replace('// flash 오류를 우회하기 위한 함수 추가', '')
    text = text.replace('function _flash_removeCallback() {}', '')
    text = text.strip()
    return text


news_count = 0
start_page =1


#main code start here
#for each category in the list
for cat in category:
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    #filename = 'output_{}_{}_{}_{}.txt'.format(start_date_str, end_date_str, sid1, cat)
    #fw = open(filename, 'w', encoding='utf-8')
    
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
                #print("3")
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, "lxml")
                #print ("4")

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




                content_list, article_count, finish_flag = Get_page_Content(current_page, soup, total_total)

                print('FINISH FLAG : ' + str(finish_flag))
                if finish_flag == 1:
                    break

                for content in content_list:
                    content = preprocess_text(content)
                    news_count += 1
                    content = ' '.join(content.split())

                    #fw.write(content)
                    #fw.write('\n')


                # increment the page index so that u will load the next page
                current_page += 1

                print('### DEBUG - current_page : {}'.format(current_page))
            except:
                print("Error happen in the HTML tag")

        if finish_flag == 1:
            break

        current_date += relativedelta(days=1) # to loop back till the end of the the selected date, it addes one day per iteration to reach

    if finish_flag == 1:
        break

    #fw.close()

    
print('total news count :', news_count)
print("Done Crawling " + str(datetime.datetime.today()))
