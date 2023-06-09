from cassandra.cluster import Cluster
from flask import Flask
from flask_restful import Resource, Api
from cassandra.cluster import Cluster
from psycopg2 import connect
from datetime import datetime, timedelta


app = Flask(__name__)
api = Api(app)


@app.before_first_request
def initialize():
    # Initialize Cassandra connection
    global cassandra_session
    cassandra_cluster = Cluster(['cassandra'])
    cassandra_session = cassandra_cluster.connect('your_cassandra_keyspace')
    
    # Initialize PostgreSQL connection
    global postgres_conn
    postgres_conn = connect("dbname=your_postgres_db user=your_postgres_username password=your_postgres_password")


class WikiDomains(Resource):
    def get(self):
        rows = cassandra_session.execute("SELECT DISTINCT domain FROM pages;")
        domains = [row.domain for row in rows]
        return {'domains': domains}, 200


class UserPages(Resource):
    def get(self, user_id):
        rows = cassandra_session.execute("SELECT page_id, title FROM pages WHERE user_id = %s;", (user_id,))
        user_pages = [{'page_id': row.page_id, 'title': row.title} for row in rows]
        return {'pages': user_pages}, 200


class DomainArticles(Resource):
    def get(self, domain):
        count = cassandra_session.execute("SELECT COUNT(*) FROM pages WHERE domain = %s;", (domain,)).one()
        return {'domain': domain, 'article_count': count[0]}, 200


class PageDetail(Resource):
    def get(self, page_id):
        row = cassandra_session.execute("SELECT * FROM pages WHERE page_id = %s;", (page_id,)).one()
        if row:
            detail = {column: getattr(row, column) for column in row._fields}
            return detail, 200
        else:
            return {'error': 'page not found'}, 404

class UserActivity(Resource):
    def get(self, start_time, end_time):
        # Convert times to datetime objects
        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')

        with postgres_conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_id, user_name, COUNT(page_id) FROM user_actions WHERE action_time BETWEEN %s AND %s GROUP BY user_id, user_name;", 
                (start_time, end_time)
            )
            results = [{'user_id': row[0], 'user_name': row[1], 'page_count': row[2]} for row in cursor.fetchall()]
        return {'users': results}, 200

class AggregatedStatistics(Resource):
    def get(self):
        # Get start and end time
        end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
        start_time = end_time - timedelta(hours=6)  # Last 6 hours

        rows = cassandra_session.execute("SELECT domain, COUNT(*) FROM pages WHERE create_time >= %s AND create_time < %s GROUP BY domain;", 
                                         (start_time, end_time))
        statistics = [{'domain': row.domain, 'page_count': row.count} for row in rows]
        return {'statistics': statistics}, 200

class BotStatistics(Resource):
    def get(self):
        # Get start and end time
        end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
        start_time = end_time - timedelta(hours=6)  # Last 6 hours

        rows = cassandra_session.execute("SELECT domain, COUNT(*) FROM pages WHERE create_time >= %s AND create_time < %s AND created_by_bot = True GROUP BY domain;", 
                                         (start_time, end_time))
        statistics = [{'domain': row.domain, 'bot_page_count': row.count} for row in rows]
        return {'statistics': statistics}, 200

class TopUsers(Resource):
    def get(self):
        # Get start and end time
        end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
        start_time = end_time - timedelta(hours=6)  # Last 6 hours

        with postgres_conn.cursor() as cursor:
            cursor.execute(
                "SELECT user_id, user_name, COUNT(page_id) FROM user_actions WHERE action_time BETWEEN %s AND %s GROUP BY user_id, user_name ORDER BY COUNT(page_id) DESC LIMIT 20;", 
                (start_time, end_time)
            )
            results = [{'user_id': row[0], 'user_name': row[1], 'page_count': row[2]} for row in cursor.fetchall()]
        return {'top_users': results}, 200


api.add_resource(WikiDomains, '/wikidomains')
api.add_resource(UserPages, '/userpages/<string:user_id>')
api.add_resource(DomainArticles, '/domainarticles/<string:domain>')
api.add_resource(PageDetail, '/pagedetail/<string:page_id>')
api.add_resource(UserActivity, '/useractivity/<string:start_time>/<string:end_time>')
api.add_resource(AggregatedStatistics, '/aggregatedstatistics')
api.add_resource(BotStatistics, '/botstatistics')
api.add_resource(TopUsers, '/topusers')

if __name__ == '__main__':
    app.run(debug=True)