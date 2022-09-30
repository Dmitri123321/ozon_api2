from pymysql import cursors


def get_companies_data(app, result=None):
    try:
        cursor = app.connection.cursor(cursors.DictCursor)
        query = 'select * from companies'
        cursor.execute(query)
        result = cursor.fetchall()
        if not result:
            raise
    except:
        app.stop('no companies data')
    return result
