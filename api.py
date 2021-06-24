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
    json_dict = request.json
    user_id = utils.check_and_get(json_dict, "user_id")
    pw = utils.sha512(utils.check_and_get(json_dict, "pw"))
    name = utils.check_and_get(json_dict, "name")
    query = "INSERT INTO user (user_id, pw, name) VALUES (%s, %s, %s);"
    cursor.execute(query, (user_id, pw, name))
    db.commit()
    return text(user_id)


@app.route('/toon', methods=["POST"])
async def create_toon(request):
    json_dict = request.json
    title = utils.check_and_get(json_dict, "title")
    synopsis = utils.check_and_get(json_dict, "synopsis", optional=True)
    platform = utils.check_and_get(json_dict, "platform")
    weekday = utils.check_and_get(json_dict, "weekday")
    if not utils.verify_weekday(weekday):
        return text("Weekday representation should be one of the combinations of: Mon, Tue, Wed, Thr, Fri, Sat, Sun", status=400)
    url = utils.check_and_get(json_dict, "url")
    thumbnail_url = utils.check_and_get(json_dict, "thumbnail_url")
    query = "INSERT INTO toon (title, synopsis, platform, weekday, url, thumbnail_url) VALUES (%s, %s, %s, %s, %s, %s);"
    cursor.execute(query, (title, synopsis, platform, weekday, url, thumbnail_url))
    db.commit()
    return text(str(cursor.lastrowid))


@app.route('/star', methods=["POST"])
async def create_star(request):
    json_dict = request.json
    user_id = utils.check_and_get(json_dict, "user_id")
    toon_id = utils.check_and_get(json_dict, "toon_id")
    query = "INSERT INTO star (user_id, toon_id) VALUES (%s, %s);"
    cursor.execute(query, (user_id, toon_id))
    db.commit()
    return text(str(cursor.lastrowid))


@app.route('/episode', methods=["POST"])
async def create_episode(request):
    json_dict = request.json
    toon_id = utils.check_and_get(json_dict, "toon_id")
    title = utils.check_and_get(json_dict, "title")
    url = utils.check_and_get(json_dict, "url")
    thumbnail_url = utils.check_and_get(json_dict, "thumbnail_url")
    query = "INSERT INTO episode (toon_id, title, url, thumbnail_url) VALUES (%s, %s, %s, %s);"
    cursor.execute(query, (toon_id, title, url, thumbnail_url))
    db.commit()
    return text(str(cursor.lastrowid))


@app.route('/history', methods=["POST"])
async def create_history(request):
    json_dict = request.json
    user_id = utils.check_and_get(json_dict, "user_id")
    episode_id = utils.check_and_get(json_dict, "episode_id")
    query = "INSERT INTO view_history (user_id, episode_id) VALUES (%s, %s);"
    cursor.execute(query, (user_id, episode_id))
    db.commit()
    return text(str(cursor.lastrowid))


# UPDATE API
@app.route('/user/<user_id>', methods=["PUT"])
async def update_user(request, user_id):
    json_dict = request.json
    pw = utils.sha512(utils.check_and_get(json_dict, "pw"))
    name = utils.check_and_get(json_dict, "name")
    query = "UPDATE user SET pw = %s, name = %s WHERE user_id = %s;"
    cursor.execute(query, (pw, name, user_id))
    db.commit()
    return text(user_id)


@app.route('/toon/<toon_id>', methods=["PUT"])
async def update_toon(request, toon_id):
    json_dict = request.json
    title = utils.check_and_get(json_dict, "title")
    synopsis = utils.check_and_get(json_dict, "synopsis", optional=True)
    platform = utils.check_and_get(json_dict, "platform")
    weekday = utils.check_and_get(json_dict, "weekday")
    if not utils.verify_weekday(weekday):
        return text("Weekday representation should be one of the combinations of: Mon, Tue, Wed, Thr, Fri, Sat, Sun", status=400)
    url = utils.check_and_get(json_dict, "url")
    thumbnail_url = utils.check_and_get(json_dict, "thumbnail_url")
    query = "UPDATE toon SET title = %s, synopsis = %s, platform = %s, weekday = %s, url = %s, thumbnail_url = %s WHERE toon_id = %s;"
    cursor.execute(query, (title, synopsis, platform, weekday, url, thumbnail_url, toon_id))
    db.commit()
    return text(toon_id)


@app.route('/star/<star_id>', methods=["PUT"])
async def update_star(request, star_id):
    json_dict = request.json
    user_id = utils.check_and_get(json_dict, "user_id")
    toon_id = utils.check_and_get(json_dict, "toon_id")
    query = "UPDATE star SET user_id = %s, toon_id = %s WHERE star_id = %s;"
    cursor.execute(query, (user_id, toon_id, star_id))
    db.commit()
    return text(star_id)


@app.route('/episode/<episode_id>', methods=["PUT"])
async def update_episode(request, episode_id):
    json_dict = request.json
    toon_id = utils.check_and_get(json_dict, "toon_id")
    title = utils.check_and_get(json_dict, "title")
    url = utils.check_and_get(json_dict, "url")
    thumbnail_url = utils.check_and_get(json_dict, "thumbnail_url")
    query = "UPDATE episode SET toon_id = %s, title = %s, url = %s, thumbnail_url = %s WHERE episode_id = %s;"
    cursor.execute(query, (toon_id, title, url, thumbnail_url, episode_id))
    db.commit()
    return text(episode_id)


@app.route('/history/<history_id>', methods=["PUT"])
async def update_history(request, history_id):
    json_dict = request.json
    user_id = utils.check_and_get(json_dict, "user_id")
    episode_id = utils.check_and_get(json_dict, "episode_id")
    query = "UPDATE view_history SET user_id = %s, episode_id = %s WHERE history_id = %s;"
    cursor.execute(query, (user_id, episode_id, history_id))
    db.commit()
    return text(history_id)


# DELETE API
@app.route('/user/<user_id>', methods=["DELETE"])
async def delete_user(request, user_id):
    query = "DELETE FROM user WHERE user_id = %s;"
    cursor.execute(query, user_id)
    db.commit()

    return text("True") 


@app.route('/toon/<toon_id>', methods=["DELETE"])
async def delete_toon(request, toon_id):
    query = "DELETE FROM toon WHERE toon_id = %s;"
    cursor.execute(query, toon_id)
    db.commit()
    return text("True")


@app.route('/star/<star_id>', methods=["DELETE"])
async def delete_star(request, star_id):
    # query = "DELETE FROM star WHERE star_id = %s;"    # naive DELETE
    query = "UPDATE star SET deleted_at = CURRENT_TIMESTAMP WHERE star_id = %s;"
    cursor.execute(query, star_id)
    db.commit()
    return text("True")


@app.route('/episode/<episode_id>', methods=["DELETE"])
async def delete_episode(request, episode_id):
    query = "DELETE FROM episode WHERE episode_id = %s;"
    cursor.execute(query, episode_id)
    db.commit()
    return text("True")


@app.route('/history/<history_id>', methods=["DELETE"])
async def delete_history(request, history_id):
    # query = "DELETE FROM view_history WHERE history_id = %s;" # naive DELETE
    query = "UPDATE view_history SET deleted_at = CURRENT_TIMESTAMP WHERE history_id = %s;"
    cursor.execute(query, history_id)
    db.commit()
    return text("True")


if __name__ == "__main__":
    app.run()
