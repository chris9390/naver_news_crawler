import re
import hanja

fp_read = open('/home/public_data/news_corpus/2015/100/output_20150101_20151231_100_264.txt','r')
fp_write = open('result.txt', 'w')

articles_before = fp_read.readlines()
articles_after = []
result = []

for article in articles_before:
    sentences = []
    sentences_temp = []

    # 광고 및 기사 사진에 대한 설명같이 매우 짧은 기사는 생략
    if len(article) < 200:
        continue


    # 기사 내용과 관계없는 문구 제거
    pattern_trash = re.compile(r'(© AFP=뉴스1)|(© News1)|(뉴스1)|(포토공용\s*기자)|(본문\s*이미지\s*영역)|(청와대\s*사진기자단)')
    if pattern_trash.findall(article):
        article = pattern_trash.sub('', article)


    # Copyright부분 제거
    pattern_copyright = re.compile(r'[(<\[]?(Copyright|ⓒ)s?.*[)>\]]?')
    if pattern_copyright.findall(article):
        article = pattern_copyright.sub('', article)


    # 오른쪽 화살표 뒷부분 제거
    pattern_right_arrow = re.compile(r'▶')
    if pattern_right_arrow.findall(article):
        article = pattern_right_arrow.sub('\n', article)
        article = article.split('\n')[0]


    # 기사 끝부분에 기자 이메일 포함 뒷부분 제거
    pattern_email = re.compile(r'[(\[<]?[a-z0-9_+.-]+@([a-z0-9-]+[.])+[a-z0-9]{2,4}[)\]>]?')
    if pattern_email.findall(article):
        article = pattern_email.sub('\n', article)
        article = article.split('\n')[0]


    # 기사 본문 제일 앞에 "기자 =" 또는 "특파원 =" 앞부분 제거
    pattern_reporter = re.compile(r'(?<=(특파원))\s*[=]|(?<=(기자))\s*[=]')
    if pattern_reporter.findall(article):
        article = pattern_reporter.sub('\n', article)
        article = article.split('\n')[1]


    # 여러 종류의 괄호로 묶인 내용 제거
    pattern_parentheses = re.compile(r'\(.*?\)|\[.*?\]|\<.*?\>|\【.*?\】|\＜.*?\＞')
    if pattern_parentheses.findall(article):
        article = pattern_parentheses.sub('', article)



    # ================================================================================================================

    '''
    pattern_LF = re.compile(r'(?<=[겠|니|한|했|이|하]다)(\s*[^가-힝\w]*)(?!이|가|고|라|며|면|는|거나|하|만)(?=[가-힣]|\w)|((?<!\d)(\.|\?)\s*(?=[^\w]))|((?<=[가-힣])(\.|\?)(?=[가-힣]))|((?<=\s)(\.|\?)(?=[가-힣]))|((?<=[가-힣])(\.|\?)(?=\w))')
    '''
    pattern_LF = re.compile(r'(?<=다)\s*([.]|[!]|[?]|[,])\s*(?!가|고|라|며|면|는|거나|만)')
    article = pattern_LF.sub('\n', article)


    sentences = article.split('\n')



    # 문장들의 좌우 공백 제거
    for sentence in sentences:
        temp = sentence.strip()
        sentences_temp.append(temp)
    sentences = sentences_temp


    # 한글이 포함되지 않은 문장패턴
    pattern_no_kor = re.compile(r'[^가-힣]*$')


    # remove를 수행하기 위한 sentences의 복사본
    sentences_temp = sentences

    # 문장단위로 보면서 한번 더 필터링
    for sentence in sentences:

        if '공식 SNS 계정' in sentence:
            sentences_temp.remove(sentence)

        elif len(sentence) < 15 and ('입니다' in sentence or '기자' in sentence or '특파원' in sentence):
            sentences_temp.remove(sentence)

        # 한글이 포함되지 않은 문장이면 제거
        elif pattern_no_kor.findall(sentence) != ['']:
            a = pattern_no_kor.findall(sentence)
            sentences_temp.remove(sentence)

        # 문장 길이가 10글자 미만이면 제거
        elif len(sentence) < 10:
             sentences_temp.remove(sentence)


    sentences = sentences_temp


    for i in sentences:
        print(i)
        fp_write.write(i + '\n')


    result.append(sentences)


fp_read.close()
fp_write.close()