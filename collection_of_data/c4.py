import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector

# 设置数据库连接
db = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="chengcheng",
    password="171612cgj",
    database="db"
)

def test_database_connection():
    try:
        # 连接数据库
        db = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="chengcheng",
            password="171612cgj",
            database="db"
        )
        if db.is_connected():
            print("Database connection established successfully.")
        else:
            print("Failed to establish database connection.")
    except Exception as e:
        print(f"Error connecting to database: {e}")

# 假设您已经手动登录到微博并获取了有效的 Cookie，将其填入下面的变量中
COOKIE = 'JSESSIONID=A76F85DF48894910B453C342B6B98F4C'

def fetch_weibo_posts(keyword, pages):
    """通过微博搜索栏直接搜索关键词并抓取数据"""
    weibo_posts = []
    for page in range(1, pages+1):
        url = f"https://s.weibo.com/weibo?q={keyword}&page={page}"
        headers = {'Cookie': COOKIE}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # 抛出异常以处理错误状态码
            soup = BeautifulSoup(response.text, 'html.parser')
            posts = parse_weibo_data(soup)
            if not posts:
                print(f"No more posts found for keyword: {keyword}")
                break  # 如果没有更多帖子，则退出循环
            weibo_posts.extend(posts)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Weibo posts: {e}")
    return weibo_posts

def parse_weibo_data(soup):
    """解析微博数据"""
    weibo_posts = []
    try:
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
            'location': post.get('geo', None),
            'school': None,  # 假设没有学校信息
            'major': None,  # 假设没有专业信息
        }
        users_info.append(user_info)
    return users_info

def save_to_database(data_frame, table_name):
    """将数据保存到MySQL数据库"""
    try:
        cursor = db.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (user_id VARCHAR(255), username VARCHAR(255), post_content TEXT, post_id VARCHAR(255), location VARCHAR(255), school VARCHAR(255), major VARCHAR(255))")
        for index, row in data_frame.iterrows():
            cursor.execute(f"INSERT INTO {table_name} (user_id, username, post_content, post_id, location, school, major) VALUES (%s, %s, %s, %s, %s, %s, %s)", (row['user_id'], row['username'], row['post_content'], row['post_id'], row['location'], row['school'], row['major']))
        db.commit()
        print(f"Data saved to database: {table_name}")
    except Exception as e:
        print(f"Error saving data to database: {e}")

def main():
    test_database_connection()
    keywords = ["北京邮电大学", "北京师范大学"]
    pages = 1000  # 设置一个较大的页数，以确保尽可能地抓取更多数据

    for keyword in keywords:
        posts = fetch_weibo_posts(keyword, pages)
        if not posts:
            print(f"No more posts found for keyword: {keyword}")
            continue
        users_info = parse_user_info(posts)
        df = pd.DataFrame(users_info)
        save_to_database(df, 'users')

if __name__ == '__main__':
    main()
