import re 
import time
import hanja
from hanja import hangul

start_time = time.time()


#fp1 = open('/home/private_data/Corpora/news_corpus/2015/105/output_20150101_20151231_105_226.txt', 'r')
fp1 = open('/home/public_data/news_corpus/2015/100/output_20150101_20151231_100_264.txt','r')

# fp2 = open('process.txt', 'w')
fp2 = open('news_norm_2015_105_226.txt', 'w')

fp3 = open('footer2.txt', 'w')

lines = fp1.readlines()
sentences = []
news=[]
footers=[]

for line in lines:
    line = re.sub('\n', 'EOS\n', line)
    enterConditions = re.compile(r"""
       (?<=[겠|니|한|했|이|하]다)(\s*[^가-힝\w]*)(?!이|고|라|며|면|는|거나|하|만)(?=[가-힝]|\w) #~이다.라고 => ~이다라고 or ~이다.그러나=>~이다\n그러나
       |((?<!\d)(\.|\?)\s*(?=[^\w]))				#숫자가 아닌 문장 .? space 영어나 숫자로 문장 시작
       |((?<=[가-힝]|\s)(\.|\?)(?=[가-힝]))			#(한글문장 또는 space) .? 한글문장
       |((?<=[가-힝])(\.|\?)(?=\w))				#한글문장.한글문장 문장사이에 space 없는경우 
       |(\(속보\))						#(속보)
       |연합뉴스 앱.*?>						#연합뉴스 footer제거
    """, re.VERBOSE)
    enterConditions2 = re.compile(r"""				#숫자나 영어로 numbering(23.1% 이런경우는 살려둠)
        (?<=[^a-z]{3})([a-z]\.)(?=[^a-z]{3})
       |(?<=[^A-Z]{3})([A-Z]\.)(?=[^A-Z]{3})
       |(?<=[^0-9]{3})([0-9]+\.)(?=[^0-9]{3})
    """, re.VERBOSE)
    line = enterConditions.sub('\n',line)
    line = re.sub("[a-z0-9_+.-]+@([a-z0-9-]+\.)+[a-z0-9]{2,4}","MAIL",line) #mail address
    line = re.sub("(?<=\()([a-zA-Z]+\w*\.)+[a-z0-9]{2,4}","URL",line) #url address
    line = enterConditions2.sub('\n',line)#url과 mail 처리 후에 처리해야함
    line = re.sub('\(.*?\)|\[.*?\]|\<.*?\>|\【.*?\】|\＜.*?\＞', '',line) 
    sentences+=line.splitlines(True)

"""
for s in sentences:
    if 'EOS' in s:
        fp3.write(s)
    else:
        fp2.write(s)
"""

for s in sentences:
    s = re.sub("^\s", '', s)
    if s.startswith('[속보]'):#거의 한줄짜리라footer가 없거나 다른 문자로되어있음
        s = re.sub('EOS','\n',s)
        s = re.sub('Copyrights*.*','',s)
        news.append(s)
    if s.endswith('EOS\n'):
        footers.append(re.sub('EOS','',s))#footer
    else:
        s = re.sub('.*(기자|특파원|사진)(\s)*=(\s)*(뉴스)*(\d)*','',s)
        s = re.sub('.*(?=MAIL)|(?<=MAIL)(?=기자).*','',s)
        s = re.sub('MAIL','',s)
        s = re.sub('(\d){4}\.(\d){1,2}.(\d){1,2}/뉴스1','',s)
        s = re.sub('(© AFP=뉴스1)|(© News1)|(/뉴스1)|(.*(JTBC 뉴스` 공식 SNS|Ltd|All Rights Reserved).*)|(포토공용 기자)|(본문 이미지 영역)|(청와대 사진기자단)','',s)
        s = re.sub('[^가-힝\w\-,.㎢%±∼\~㎞㎝ｍ㎡\+㎏㏄㎾㎎¼ｇ㎿ℓ\s]', ' ', s) #remove special character
        s = re.sub('^[^가-힝a-zA-Z0-9]*$','',s)#한문장에 한글,영어,숫자가 하나도 없으면 의미가 없으므로 문장자체를 삭제

        if any(x in s for x in ('Copyright','URL','모바일 경향','파이낸셜뉴스','GoodNews paper')):
            footers.append(s)
        else:
            news.append(s)

for news_line in news:
    news_line = re.sub("^[^가-힝\w]*", '', news_line)#remove meaningless characters of front part
    news_line = ' '.join(news_line.split()) + '\n'#substitution 2 or more spaces to one.
    if news_line is not '\n':
        news_line = hanja.translate(news_line, 'substitution')
        fp2.write(news_line)

for footer in footers:
    footer = re.sub("^\s*", '', footer)
    if footer == '\n':
        continue;
    else:
        fp3.write(footer)

fp1.close()
fp2.close()
fp3.close()


end_time = time.time()
elapsed_time = end_time - start_time
print('wall_time:\t{0:0.2f}'.format(elapsed_time))
