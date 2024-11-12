import psycopg2
from pymongo import MongoClient
from datetime import datetime

conn_pg = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="",
    host="localhost",
    port="5432"
)
cursor = conn_pg.cursor()

client = MongoClient('mongodb://localhost:27017/')
db = client['bookstore']
reviews_collection = db['reviews']

cursor.execute("""
    SELECT
        r.review_id,
        r.rating,
        r.review_text,
        r.created_at,
        r.updated_at,
        r.is_deleted,
        u.user_id,
        u.username,
        u.email,
        u.is_admin,
        u.created_at AS user_created_at,
        u.updated_at AS user_updated_at,
        u.last_login,
        b.book_id,
        b.isbn,
        b.title,
        b.description,
        b.price,
        b.publication_date
    FROM reviews r
    JOIN users u ON r.user_id = u.user_id
    JOIN books b ON r.book_id = b.book_id;
""")

for row in cursor.fetchall():
    updated_review = {
        "rating": row[1],
        "review_text": row[2],
        "created_at": row[3] if isinstance(row[3], datetime) else datetime.combine(row[3], datetime.min.time()),
        "updated_at": row[4] if isinstance(row[4], datetime) else datetime.combine(row[4], datetime.min.time()),
        "is_deleted": row[5],
        "user": {
            "user_id": row[6],
            "username": row[7],
            "email": row[8],
            "is_admin": row[9],
            "created_at": row[10] if isinstance(row[10], datetime) else datetime.combine(row[10], datetime.min.time()),
            "updated_at": row[11] if isinstance(row[11], datetime) else datetime.combine(row[11], datetime.min.time()),
            "last_login": row[12] if isinstance(row[12], datetime) else datetime.combine(row[12], datetime.min.time())
        },
        "book": {
            "book_id": row[13],
            "isbn": row[14],
            "title": row[15],
            "description": row[16],
            "price": float(row[17]),
            "publication_date": row[18] if isinstance(row[18], datetime) else datetime.combine(row[18], datetime.min.time())
        }
    }

    reviews_collection.update_one(
        {"review_id": row[0]},
        {"$set": updated_review},
        upsert=True
    )

cursor.close()
conn_pg.close()
client.close()
