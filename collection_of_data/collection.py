import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

# 设置数据库连接
engine = create_engine('mysql+pymysql://chengcheng:171612cgj@localhost/db')

# 假设您已经手动登录到微博并获取了有效的 Cookie，将其填入下面的变量中
COOKIE = '0033WrSXqPxfM725Ws9jqgMF55529P9D9WWOlrZ9ULwWFvsw_aanceXs5JpX5o275NHD95QNSo-EeK.41K54Ws4Dqcjci--Xi-zRiKn7i--Xi-iFiKyFi--Xi-z4i-2ci--NiK.XiKLsi--4iK.Ri-isi--ciKL2i-z7'


def fetch_weibo_posts(keyword, start_date, end_date):
    """从微博网页抓取带有关键词和日期范围的微博数据"""
    url = f"https://s.weibo.com/weibo?q={keyword}&typeall=1&suball=1&timescope=custom:{start_date}:{end_date}&Refer=g"
    headers = {'Cookie': COOKIE}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        # 在这里编写解析 HTML 的代码，获取微博数据
        # 注意：这里的解析方式可能因网页结构变化而需要调整
        return parse_weibo_data(soup)
    else:
        return []


def parse_weibo_data(soup):
    """解析微博数据"""
    # 在这里编写解析 HTML 的代码，将微博数据提取为字典列表
    return []


def parse_user_info(posts):
    """解析用户信息和微博内容"""
    users_info = []
    for post in posts:
        user_info = {
            'user_id': post['user']['id'],
            'username': post['user']['screen_name'],
            'school': post['user'].get('education', {}).get('school'),
            'major': post['user'].get('education', {}).get('major'),
            'post_content': post['text'],
            'post_id': post['id'],
            'location': post.get('geo', None)  # 假设geo字段存在地理位置
        }
        users_info.append(user_info)
    return users_info


def save_to_database(data_frame, table_name):
    """将数据保存到MySQL数据库"""
    data_frame.to_sql(table_name, engine, if_exists='append', index=False)


def main():
    keywords = ["北京邮电大学", "北京师范大学"]
    start_date = "2023-01-01"
    end_date = "2024-03-31"

    for keyword in keywords:
        posts = fetch_weibo_posts(keyword, start_date, end_date)
        users_info = parse_user_info(posts)
        df = pd.DataFrame(users_info)
        save_to_database(df, 'users')
        print(f"Data for {keyword} saved to database.")


if __name__ == '__main__':
    main()
