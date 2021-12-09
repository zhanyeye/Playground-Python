import requests                # http请求库
from lxml import etree         # 解析HTML和XML的库
import threading               # 多线程库
from bs4 import BeautifulSoup  # 爬虫库
import pandas as pd            # 数据分析库


# http 请求头
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36"}


# 获取所有"每日鸭肉价格"页面的URL
# 返回一个 List 
def get_detail_urls(url):
    # 下载 https://jiage.cngold.org/jiaqin/yarou/list_3217_all.html 页面
    tmp_html = requests.get(url,headers = headers).content.decode('utf-8')
    # 解析 https://jiage.cngold.org/jiaqin/yarou/list_3217_all.html 中的链接
    tmp_url_list = etree.HTML(tmp_html).xpath('//div[@class="history_news_content"]/ul/li/a/@href')
    
    detail_urls = []
    
    # 解析 detail_html中的 detail_url
    # 将 detail_url 添加到 detail_urls list中
    def parse_detail_url(tmp_url):
        detail_html = requests.get(tmp_url).content.decode('utf-8')
        detail_url = etree.HTML(detail_html).xpath('//div[@class="border_top"]/ul/li/a/@href')
        
        if detail_url == []:
            #因发现网站结构在十年中有改变，具体是从2018/3/20开始，旧的网站数据结构需要用下面的xpath来获取
            detail_url = etree.HTML(detail_html).xpath('//div[@class="left_info"]/ul/li/a/@href')[0]
        else:
            detail_url = detail_url[0]
        
        # 将http 替换成 https
        detail_url = detail_url.replace('http://', 'https://', 1)
        detail_urls.append(detail_url)
        print('.', end='', flush=True)
    
    # 创建执行 parse_detail_url 的线程，放进线程list中
    threads = [threading.Thread(target=parse_detail_url, args=(tmp_url,)) for tmp_url in tmp_url_list]
    
    # 开始启动多线程
    print('开启多线程下载 detail_url: ', end='', flush=True)
    for t in threads:
        t.start()
        t.join()
    print('\n下载 detail_url 完成~', flush=True)
    return detail_urls


# 根据 url 爬取鸭肉价格
# 返回 [日期， 价格]
def get_data(url):
    req = requests.get(url, timeout=60)
    req.encoding = 'utf-8'
    html = req.text
    time = url[27: 37]
    # 获取 html 中第一个table
    table = BeautifulSoup(html.replace('&nbsp;', ' '),"lxml").find_all('table')[0]
    row = table.find_all('tr')[-1]
    price = row.find_all('td')[1].string.strip().strip(' 元/斤')
    return [time, price]


# 多线程下载所有鸭肉价格
# 返回 [[日期1， 价格1], [日期2， 价格2], ...]
def concurrent_get_data(detail_urls):
    data = []
    def append_data(url):
        try:
            print('.', end='', flush=True)
            data.append(get_data(url))
        except:
            pass
    
    threads = [threading.Thread(target=append_data, args=(tmp_url,)) for tmp_url in detail_urls]
    # 开始启动多线程
    print('开启多线程爬取数据: ', end='', flush=True)
    for t in threads:
        t.start()
        t.join()
    print('\n爬取数据完成~', flush=True)
    return data


detail_urls = get_detail_urls('https://jiage.cngold.org/jiaqin/yarou/list_3217_all.html')
df = pd.DataFrame(detail_urls, columns=['url'])
# 将链接写入csv文件
df.to_csv('url.csv', index=False, encoding='utf_8_sig')

# 读取csv文件
# pd.read_csv('url.csv')

data = concurrent_get_data(detail_urls)

df = pd.DataFrame(data, columns=['日期', '价格'])
df.to_csv('data.csv', index=False, encoding='utf_8_sig')
