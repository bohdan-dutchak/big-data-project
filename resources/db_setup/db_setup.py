import psycopg2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Connect to PostgreSQL
pg_conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="example_password",  # replace with your actual password
    host="postgres_db",  # docker-compose service name
    port="5432"
)

# Create table user_actions
with pg_conn.cursor() as cursor:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_actions (
            user_id INT,
            user_name VARCHAR(255),
            page_id INT,
            action_time TIMESTAMP,
            PRIMARY KEY (user_id, page_id)
        );
    """)
pg_conn.commit()

# Connect to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')  # replace with your actual username and password
cluster = Cluster(['cassandra'], auth_provider=auth_provider)  # docker-compose service name
cassandra_session = cluster.connect()

# Create keyspace wiki_stream
cassandra_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS wiki_stream 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
""")

# Create table pages
cassandra_session.execute("""
    CREATE TABLE IF NOT EXISTS wiki_stream.pages (
        page_id INT,
        title TEXT,
        domain TEXT,
        user_id INT,
        create_time TIMESTAMP,
        created_by_bot BOOLEAN,
        PRIMARY KEY ((domain), create_time, page_id)
    ) WITH CLUSTERING ORDER BY (create_time DESC);
""")

# Create secondary index on user_id and created_by_bot
cassandra_session.execute("CREATE INDEX IF NOT EXISTS ON wiki_stream.pages (user_id);")
cassandra_session.execute("CREATE INDEX IF NOT EXISTS ON wiki_stream.pages (created_by_bot);")

# Close connections
pg_conn.close()
cluster.shutdown()