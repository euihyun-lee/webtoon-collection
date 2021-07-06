from pprint import pprint
import pymysql
import requests
import ujson as json

import utils
import config


API_ADDR = 'http://localhost:8000'
TABLES = ["view_history", "star", "episode", "toon", "user"]

db = pymysql.connect(
    host=config.db.HOST,
    user=config.db.USER,
    passwd=config.db.PASSWORD,
    db=config.db.DATABASE,
    charset='utf8'
)
cursor = db.cursor(pymysql.cursors.DictCursor)


def init_table(table):
    assert table in TABLES, f"Table name {table} is not valid."
    cursor.execute(f"DELETE FROM {table};")
    if not table == "user":
        cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT=1;")
    db.commit()


def init_db():
    for table in TABLES:
        init_table(table)


def create(addr, data):
    headers = {"content-type": "application/json"}
    response = requests.post(f"{API_ADDR}/{addr}",
                             data=json.dumps(data, ensure_ascii=False).encode('utf-8'),
                             headers=headers)
    result = response.content
    return result


def test_create(dataset):
    # Create user
    for user in dataset["user"]:
        result = create("user", user).decode('utf-8')
        err_msg = "Expected {}, got: {}".format(user["user_id"], result)
        assert result == user["user_id"], err_msg
    
    # Create toon
    for index, toon in enumerate(dataset["toon"], start=1):
        result = create("toon", toon).decode('utf-8')
        err_msg = "Expected {}, got: {}".format(index, result)
        assert result == str(index), err_msg

    # Create star
    for index, star in enumerate(dataset["star"], start=1):
        result = create("star", star).decode('utf-8')
        err_msg = "Expected {}, got: {}".format(index, result)
        assert result == str(index), err_msg

    # Create episode
    for index, episode in enumerate(dataset["episode"], start=1):
        result = create("episode", episode).decode('utf-8')
        err_msg = "Expected {}, got: {}".format(index, result)
        assert result == str(index), err_msg

    # Create view_history
    for index, history in enumerate(dataset["history"], start=1):
        result = create("history", history).decode('utf-8')
        err_msg = "Expected {}, got: {}".format(index, result)
        assert result == str(index), err_msg


def read(*addrs, **kwargs):
    addr = '/'.join(addrs)
    args = '&'.join(["{}={}".format(key, value) for key, value in kwargs.items()])
    if args:
        addr = addr + '?' + args
    response = requests.get(f"{API_ADDR}/{addr}")
    result = json.loads(response.content)
    return result


def test_read(dataset):
    # Read user by user ID
    for user in dataset["user"]:
        result = read("user", user["user_id"])
        for key in user:
            err_msg = "Expected {} for {}, got: {}".format(user[key], key, result[key])
            assert user[key] == result[key], err_msg

    # Read starred toons & episodes by user ID
    stars = {user["user_id"]: [] for user in dataset["user"]}
    for index, star in enumerate(dataset["star"], start=1):
        star_with_id = {"star_id": str(index)}
        star_with_id.update(star)
        stars[star["user_id"]].append(star_with_id)

    for user in dataset["user"]:
        result = read("user", user["user_id"], "star")
        stars_by_user_id = stars[user["user_id"]]
        err_msg = "Expected {} starred toons, got: {}".format(len(stars_by_user_id), len(result))
        assert len(stars_by_user_id) == len(result)
        for item in result:
            assert "toon_id" in item, "No toon info contained"
            assert "episode_id" in item, "No latest episode info contained"

        # test for weekday specified query
        for weekday in utils.WEEKDAY_REPRS:
            result = read("user", user["user_id"], "star", weekday=weekday)
            for item in result:
                assert "toon_id" in item, "No toon info contained"
                assert "episode_id" in item, "No latest episode info contained"
                weekdays = item["weekday"].split(',')
                err_msg = "Expected the toon on {}, got: {}".format(weekday, item["weekday"])
                assert weekday in weekdays, err_msg
    
    # Read star by user ID & toon ID
    for user in dataset["user"]:
        stars_by_user_id = stars[user["user_id"]]
        for index, toon in enumerate(dataset["toon"], start=1):
            result = read("user", user["user_id"], "toon", str(index), "star")
            stars_by_uid_tid = [star for star in stars_by_user_id if star["toon_id"] == str(index)]
            assert len(result) == len(stars_by_uid_tid)
            for star in stars_by_uid_tid:
                for star_result in result:
                    if star["star_id"] == star_result["star_id"]:
                        for key in star:
                            err_msg = "Expected {} for {}, got: {}".format(star[key], key, star_result[key])
                            assert star[key] == star_result[key], err_msg        


if __name__ == "__main__":
    print("Initializing DB...")
    init_db()

    # Create user data
    users = [{"user_id": "gildong",
              "pw": "1234",
              "name": "홍길동"}]

    # Create toon data with synopsis & w/o synopsis
    toons = [{"title": "마음의 소리",
              "synopsis": "솔직 담백 최강의 개그 만화 <마음의 소리>\n날 가져요 엉엉",
              "platform": "naver",
              "weekday": "Tue,Thr",
              "url": "https://comic.naver.com/webtoon/list.nhn?titleId=20853",
              "thumbnail_url": "https://shared-comic.pstatic.net/thumb/webtoon/20853/thumbnail/thumbnail_IMAG06_89061d8c-e491-42f1-8c15-40932e5eb939.jpg"},
             {"title": "CELL",
              "platform": "daum",
              "weekday": "Wed",
              "url": "http://cartoon.media.daum.net/webtoon/view/cell",
              "thumbnail_url": "http://t1.daumcdn.net/webtoon/op/9f168e1e70d76ec3c7e59332483da22ba6b789a6"}]

    # Create star data
    stars = [{"user_id": "gildong",
              "toon_id": "1"}]

    # Create episode data
    episodes = [{"toon_id": "1",
                 "title": "마음의 소리 1화 <진실>",
                 "url": "https://comic.naver.com/webtoon/detail.nhn?titleId=20853&no=1&weekday=tue",
                 "thumbnail_url": "https://shared-comic.pstatic.net/thumb/webtoon/20853/1/inst_thumbnail_20853_1.jpg"},
                {"toon_id": "1",
                 "title": "마음의 소리 2화 <위협>",
                 "url": "https://comic.naver.com/webtoon/detail.nhn?titleId=20853&no=2&weekday=tue",
                 "thumbnail_url": "https://shared-comic.pstatic.net/thumb/webtoon/20853/2/inst_thumbnail_20853_2.jpg"},
                {"toon_id": "2",
                 "title": "1화",
                 "url": "http://cartoon.media.daum.net/webtoon/viewer/88624",
                 "thumbnail_url": "http://t1.daumcdn.net/webtoon/op/4d748a18205e10ed1bc8d59711aa7bbb40c47acb"},
                {"toon_id": "2",
                 "title": "2화",
                 "url": "http://cartoon.media.daum.net/webtoon/viewer/88637",
                 "thumbnail_url": "http://t1.daumcdn.net/webtoon/op/5901e3209c3301a5cc92796d4806368fc967f712"}]

    # Create view_history data
    histories = [{"user_id": "gildong",
                  "episode_id": "1"},
                 {"user_id": "gildong",
                  "episode_id": "3"}]
    
    # Wrap up
    dataset = {"user": users,
               "toon": toons,
               "star": stars,
               "episode": episodes,
               "history": histories}

    print("Testing Create API...")
    test_create(dataset)

    print("Testing Read API...")
    test_read(dataset)
