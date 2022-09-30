from pymysql import cursors


def get_companies_data(app):
    cursor = app.connection.cursor(cursors.DictCursor)
    query = 'select * from companies'
    cursor.execute(query)
    return cursor.fetchall()
