import pymysql
from sanic import Sanic
from sanic.response import json, text
from functools import partial

import utils
import config


app = Sanic("webtoon-collection-api")
db = pymysql.connect(
    host=config.db.HOST,
    user=config.db.USER,
    passwd=config.db.PASSWORD,
    db=config.db.DATABASE,
    charset='utf8'
)
cursor = db.cursor(pymysql.cursors.DictCursor)
json = partial(json, dumps=utils.mysql_json_dumps())


# CRUD
# READ API
@app.route('/user/<user_id>', methods=["GET"])
async def get_user_by_id(request, user_id):
    query = "SELECT * FROM user WHERE user_id = %s;"
    cursor.execute(query, user_id)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated user ID: {user_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/user/<user_id>/star', methods=["GET"])
async def get_star_by_user(request, user_id):
    if "weekday" in request.args:
        weekday = request.raw_args["weekday"]
        if not weekday in utils.WEEKDAY_REPRS:
            return text("Weekday representation should be one of: Mon, Tue, Wed, Thr, Fri, Sat, Sun", status=400)
        query = "SELECT * FROM star WHERE user_id = %s AND weekday LIKE CONCAT('%', %s, '%');"
        cursor.execute(query, user_id, weekday)
    else:
        query = "SELECT * FROM star WHERE user_id = %s;"
        cursor.execute(query, user_id)
    result = cursor.fetchall()
    return json(result)


@app.route('/user/<user_id>/toon/<toon_id>/star', methods=["GET"])
async def get_star_by_user_toon(request, user_id, toon_id):
    query = "SELECT * FROM star WHERE user_id = %s AND toon_id = %s;"
    cursor.execute(query, user_id, toon_id)
    result = cursor.fetchall()
    return json(result)


@app.route('/user/<user_id>/history', methods=["GET"])
async def get_history_by_user(request, user_id):
    query = "SELECT * FROM view_history WHERE user_id = %s;"
    cursor.execute(query, user_id)
    result = cursor.fetchall()
    return json(result)


@app.route('/toon', methods=["GET"])
async def get_toon(request):
    query = "SELECT * FROM toon;"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


@app.route('/toon/<toon_id>', methods=["GET"])
async def get_toon_by_id(request, toon_id):
    query = "SELECT * FROM toon WHERE toon_id = %s;"
    cursor.execute(query, toon_id)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated toon ID: {toon_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/toon/<toon_id>/episode', methods=["GET"])
async def get_episode_by_toon(request, toon_id):
    query = "SELECT * FROM episode WHERE toon_id = %s;"
    cursor.execute(query, toon_id)
    result = cursor.fetchall()
    return json(result)


@app.route('/star/<star_id>', methods=["GET"])
async def get_star_by_id(request, star_id):
    query = "SELECT * FROM star WHERE star_id = %s;"
    cursor.execute(query, star_id)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated star ID: {star_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/episode/<episode_id>', methods=["GET"])
async def get_episode_by_id(request, episode_id):
    query = "SELECT * FROM episode WHERE episode_id = %s;"
    cursor.execute(query, episode_id)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated episode ID: {episode_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/episode/<episode_id>/history', methods=["GET"])
async def get_history_by_episode(request, episode_id):
    query = "SELECT * FROM view_history WHERE episode_id = %s;"
    cursor.execute(query, episode_id)
    result = cursor.fetchall()
    return json(result)


# CREATE API
@app.route('/user', methods=["POST"])
async def create_user(request):
    assert "user_id" in request.json, "User ID (user_id) must be specified."
    assert "pw" in request.json, "Password (pw) must be specified."
    assert "name" in request.json, "Username (name) must be specified."
    user_id = request.json["user_id"]
    pw = utils.sha512(request.json["pw"])
    name = request.json["name"]
    query = "INSERT INTO user (user_id, pw, name) VALUES (%s, %s, %s);"
    cursor.execute(query, (user_id, pw, name))
    db.commit()
    return text(user_id)


@app.route('/toon', methods=["POST"])
async def create_toon(request):
    assert "title" in request.json, "Title (title) must be specified."
    assert "platform" in request.json, "Platform (platform) must be specified."
    assert "weekday" in request.json, "Weekday (weekday) must be specified."
    assert "url" in request.json, "URL (url) must be specified."
    assert "thumbnail_url" in request.json, "Thumbnail URL (thumbnail_url) must be specified."
    title = request.json["title"]
    synopsis = request.json.get("synopsis")
    platform = request.json["platform"]
    weekday = request.json["weekday"]
    if not utils.verify_weekday(weekday):
        return text("Weekday representation should be one of the combinations of: Mon, Tue, Wed, Thr, Fri, Sat, Sun", status=400)
    url = request.json["url"]
    thumbnail_url = request.json["thumbnail_url"]
    query = "INSERT INTO toon (title, synopsis, platform, weekday, url, thumbnail_url) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (title, synopsis, platform, weekday, url, thumbnail_url))
    db.commit()
    return text(str(cursor.lastrowid))


if __name__ == "__main__":
    app.run()
