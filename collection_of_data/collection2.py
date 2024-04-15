import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
# 设置数据库连接
# engine = create_engine('mysql+pymysql://chengcheng:171612cgj@localhost/db')
db = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="chengcheng",
    password="171612cgj",
    database="db"
)
print("1")
# 假设您已经手动登录到微博并获取了有效的 Cookie，将其填入下面的变量中
COOKIE = '0033WrSXqPxfM725Ws9jqgMF55529P9D9WWOlrZ9ULwWFvsw_aanceXs5JpX5o275NHD95QNSo-EeK.41K54Ws4Dqcjci--Xi-zRiKn7i--Xi-iFiKyFi--Xi-z4i-2ci--NiK.XiKLsi--4iK.Ri-isi--ciKL2i-z7'


def fetch_weibo_posts(keyword, start_date, end_date):
    """从微博网页抓取带有关键词和日期范围的微博数据"""
    url = f"https://s.weibo.com/weibo?q={keyword}&typeall=1&suball=1&timescope=custom:{start_date}:{end_date}&Refer=g"
    headers = {'Cookie': COOKIE}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # 抛出异常以处理错误状态码
        soup = BeautifulSoup(response.text, 'html.parser')
        # 在这里编写解析 HTML 的代码，获取微博数据
        return parse_weibo_data(soup)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching Weibo posts: {e}")
        return []


def parse_weibo_data(soup):
    """解析微博数据"""
    weibo_posts = []
    try:
        # 假设每个微博帖子都在class为"card-wrap"的div中
        for card in soup.find_all('div', class_='card-wrap'):
            user_info = card.find('div', class_='info').find('a')
            post_info = {
                'user': {
                    'id': user_info.get('href').split('/')[-1],
                    'screen_name': user_info.text.strip()
                },
                'text': card.find('p', class_='txt').text.strip(),
                'id': card.get('mid'),  # 假设每个帖子的div有一个mid属性
                'geo': card.find('a', class_='location').text.strip() if card.find('a', class_='location') else None
            }
            weibo_posts.append(post_info)
    except Exception as e:
        print(f"Error parsing Weibo data: {e}")
    return weibo_posts


def parse_user_info(posts):
    """解析用户信息和微博内容"""
    users_info = []
    for post in posts:
        user_info = {
            'user_id': post['user']['id'],
            'username': post['user']['screen_name'],
            'post_content': post['text'],
            'post_id': post['id'],
            'location': post.get('geo', None), # 假设geo字段存在地理位置
            'school': None,  # 微博数据中可能没有学校信息，需要从其他地方获取
            'major': None,  # 同上
        }
        users_info.append(user_info)
    return users_info


def save_to_database(data_frame, table_name):
    """将数据保存到MySQL数据库"""
    try:
        cursor = db.cursor()
        for index, row in data_frame.iterrows():
            sql = f"INSERT INTO {table_name} (user_id, username, post_content, post_id, location, school, major) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (row['user_id'], row['username'], row['post_content'], row['post_id'], row['location'], row['school'], row['major'])
            cursor.execute(sql, values)
        db.commit()
        print(f"Data saved to database: {table_name}")
    except Exception as e:
        print(f"Error saving data to database: {e}")


def main():
    keywords = ["北京邮电大学", "北京师范大学"]
    start_date = "2023-01-01"
    end_date = "2024-03-31"

    for keyword in keywords:
        posts = fetch_weibo_posts(keyword, start_date, end_date)
        users_info = parse_user_info(posts)
        df = pd.DataFrame(users_info)
        save_to_database(df, 'users')


if __name__ == '__main__':
    main()
