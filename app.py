from fastapi import FastAPI, HTTPException, Depends
from cassandra.cluster import Cluster
from psycopg2 import connect
from datetime import datetime, timedelta
import uvicorn


app = FastAPI()


def get_cassandra_session():
    # Initialize Cassandra connection
    cassandra_cluster = Cluster(['cassandra'])
    cassandra_session = cassandra_cluster.connect('your_cassandra_keyspace')
    return cassandra_session


def get_postgres_conn():
    # Initialize PostgreSQL connection
    postgres_conn = connect("dbname=your_postgres_db user=your_postgres_username password=your_postgres_password")
    return postgres_conn


@app.get("/wikidomains")
async def wiki_domains(cassandra_session=Depends(get_cassandra_session)):
    rows = cassandra_session.execute("SELECT DISTINCT domain FROM pages;")
    domains = [row.domain for row in rows]
    return {'domains': domains}


@app.get("/userpages/{user_id}")
async def user_pages(user_id: str, cassandra_session=Depends(get_cassandra_session)):
    rows = cassandra_session.execute("SELECT page_id, title FROM pages WHERE user_id = %s;", (user_id,))
    user_pages = [{'page_id': row.page_id, 'title': row.title} for row in rows]
    return {'pages': user_pages}


@app.get("/domainarticles/{domain}")
async def domain_articles(domain: str, cassandra_session=Depends(get_cassandra_session)):
    count = cassandra_session.execute("SELECT COUNT(*) FROM pages WHERE domain = %s;", (domain,)).one()
    return {'domain': domain, 'article_count': count[0]}


@app.get("/pagedetail/{page_id}")
async def page_detail(page_id: str, cassandra_session=Depends(get_cassandra_session)):
    row = cassandra_session.execute("SELECT * FROM pages WHERE page_id = %s;", (page_id,)).one()
    if row:
        detail = {column: getattr(row, column) for column in row._fields}
        return detail
    else:
        raise HTTPException(status_code=404, detail="Page not found")


@app.get("/useractivity/{start_time}/{end_time}")
async def user_activity(start_time: str, end_time: str, postgres_conn=Depends(get_postgres_conn)):
    # Convert times to datetime objects
    start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')

    with postgres_conn.cursor() as cursor:
        cursor.execute(
            "SELECT user_id, user_name, COUNT(page_id) FROM user_actions WHERE action_time BETWEEN %s AND %s GROUP BY user_id, user_name;", 
            (start_time, end_time)
        )
        results = [{'user_id': row[0], 'user_name': row[1], 'page_count': row[2]} for row in cursor.fetchall()]
    return {'users': results}


@app.get("/aggregatedstatistics")
async def aggregated_statistics(cassandra_session=Depends(get_cassandra_session)):
    # Get start and end time
    end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
    start_time = end_time - timedelta(hours=6)  # Last 6 hours

    rows = cassandra_session.execute("SELECT domain, COUNT(*) FROM pages WHERE create_time >= %s AND create_time < %s GROUP BY domain;", 
                                     (start_time, end_time))
    statistics = [{'domain': row.domain, 'page_count': row.count} for row in rows]
    return {'statistics': statistics}

@app.get("/botstatistics")
async def bot_statistics(cassandra_session=Depends(get_cassandra_session)):
    # Get start and end time
    end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
    start_time = end_time - timedelta(hours=6)  # Last 6 hours

    rows = cassandra_session.execute("SELECT domain, COUNT(*) FROM pages WHERE create_time >= %s AND create_time < %s AND created_by_bot = True GROUP BY domain;", 
                                     (start_time, end_time))
    statistics = [{'domain': row.domain, 'bot_page_count': row.count} for row in rows]
    return {'statistics': statistics}

@app.get("/topusers")
async def top_users(postgres_conn=Depends(get_postgres_conn)):
    # Get start and end time
    end_time = datetime.now() - timedelta(hours=1)  # Excluding the last hour
    start_time = end_time - timedelta(hours=6)  # Last 6 hours

    with postgres_conn.cursor() as cursor:
        cursor.execute(
            "SELECT user_id, user_name, COUNT(page_id) FROM user_actions WHERE action_time BETWEEN %s AND %s GROUP BY user_id, user_name ORDER BY COUNT(page_id) DESC LIMIT 20;", 
            (start_time, end_time)
        )
        results = [{'user_id': row[0], 'user_name': row[1], 'page_count': row[2]} for row in cursor.fetchall()]
    return {'top_users': results}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

