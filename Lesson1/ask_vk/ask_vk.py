import argparse
from datetime import datetime

import pandas as pd
import vk_api

MAX_COUNT_POSTS = 100


def get_session(login, password):
    vk_session = vk_api.VkApi(login, password)
    vk_session.auth()
    return vk_session.get_api()


def get_batch_of_posts(session, owner_id, offset, count=100):
    return session.wall.get(owner_id=owner_id, offset=offset, count=count)


def process_post(post):
    text = post.get("text", "").replace("\n", "")
    author = post.get("from_id", None)
    date = post.get("date", 0)
    return text, author, datetime.fromtimestamp(int(date))


def print_result(posts_storage, output_file):
    df = pd.DataFrame.from_dict(posts_storage)
    df.to_excel(output_file)


def process_vk(login, password, owner_id, output_file, posts_num):
    session = get_session(login, password)
    count = MAX_COUNT_POSTS

    posts_storage = {
        "text": [],
        "author": [],
        "date": [],
    }
    for step in range(posts_num // MAX_COUNT_POSTS):
        offset = step * MAX_COUNT_POSTS
        batch_of_posts = get_batch_of_posts(
            session, owner_id, offset, count
        )
        for post in batch_of_posts["items"]:
            text, author, date = process_post(post)
            posts_storage["text"].append(text)
            posts_storage["author"].append(author)
            posts_storage["date"].append(date)

    print_result(posts_storage, output_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-file", type=str, default="vk_posts.xlsx")
    parser.add_argument("--login", type=str, default="")
    parser.add_argument("--password", type=str, default="")
    parser.add_argument(
        "--owner_id", type=str, default="-91453124",
        help="Id of group. It should starts with -."
    )
    parser.add_argument("--posts-num", type=int, default=3000)
    args = parser.parse_args()
    process_vk(
        args.login, args.password, args.owner_id,
        args.output_file, args.posts_num
    )

if __name__ == "__main__":
    main()
