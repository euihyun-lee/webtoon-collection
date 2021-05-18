import pymysql
from sanic import Sanic
from sanic.response import json

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


@app.route('/user/<user_id>', methods=["GET"])
async def get_user_by_id(request, user_id):
    query = f"SELECT * FROM user WHERE user_id = {user_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated user ID: {user_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/user/<user_id>/star', methods=["GET"])
async def get_star_by_user(request, user_id):
    if "weekday" in request.args:
        weekday = request.args["weekday"]
        query = f"SELECT * FROM star WHERE user_id = {user_id} AND weekday = {weekday};"
    else:
        query = f"SELECT * FROM star WHERE user_id = {user_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


@app.route('/user/<user_id>/toon/<toon_id>/star', methods=["GET"])
async def get_star_by_user_toon(request, user_id, toon_id):
    query = f"SELECT * FROM star WHERE user_id = {user_id} AND toon_id = {toon_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


@app.route('/user/<user_id>/history', methods=["GET"])
async def get_history_by_user(request, user_id):
    query = f"SELECT * FROM view_history WHERE user_id = {user_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


@app.route('/toon', methods=["GET"])
async def get_toon(request):
    query = f"SELECT * FROM toon;"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


@app.route('/toon/<toon_id>', methods=["GET"])
async def get_toon_by_id(request, toon_id):
    query = f"SELECT * FROM toon WHERE toon_id = {toon_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated toon ID: {toon_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/toon/<toon_id>/episode', methods=["GET"])
async def get_episode_by_toon(request, toon_id):
    query = f"SELECT * FROM episode WHERE toon_id = {toon_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


@app.route('/star/<star_id>', methods=["GET"])
async def get_star_by_id(request, star_id):
    query = f"SELECT * FROM star WHERE star_id = {star_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated star ID: {star_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/episode/<episode_id>', methods=["GET"])
async def get_episode_by_id(request, episode_id):
    query = f"SELECT * FROM episode WHERE episode_id = {episode_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    assert len(result) <= 1, f"Duplicated episode ID: {episode_id}"
    if len(result) == 0:
        return json({})
    else:
        return json(result[0])


@app.route('/episode/<episode_id>/history', methods=["GET"])
async def get_history_by_episode(request, episode_id):
    query = f"SELECT * FROM view_history WHERE episode_id = {episode_id};"
    cursor.execute(query)
    result = cursor.fetchall()
    return json(result)


if __name__ == "__main__":
    app.run()
