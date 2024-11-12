import time
import random
import psycopg2
from pymongo import MongoClient
from datetime import datetime
import json

class DatabaseTester:
    def __init__(self):
        self.pg_conn = psycopg2.connect(
              dbname="postgres",
                user="postgres",
                password="3494",
                host="localhost",
                port="5432"
        )
        
        self.mongo_client = MongoClient('mongodb://localhost:27017/')
        self.mongo_db = self.mongo_client['bookstore']
        
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            self.user_ids = [user[0] for user in cur.fetchall()]
            
            cur.execute("SELECT book_id FROM books")
            self.book_ids = [book[0] for book in cur.fetchall()]

    def generate_review_data(self, num_reviews):
        """Generate sample review data"""
        reviews = []
        for _ in range(num_reviews):
            main_review = f"Sample review text {'lorem ipsum ' * random.randint(10, 100)}"
            
            comments = [
                {
                    'user_id': random.choice(self.user_ids),
                    'comment': f"Comment {'text ' * random.randint(5, 20)}",
                    'created_at': datetime.now()
                } for _ in range(random.randint(0, 5))
            ]
            
            sql_review_text = main_review + "\n\nComments:\n" + \
                "\n".join([f"User {c['user_id']}: {c['comment']}" for c in comments])
            
            mongo_review = {
                'user_id': random.choice(self.user_ids),
                'book_id': random.choice(self.book_ids),
                'rating': random.randint(1, 5),
                'main_review': main_review,
                'comments': comments,
                'created_at': datetime.now(),
                'updated_at': datetime.now(),
                'is_deleted': False,
                'helpful_votes': random.randint(0, 100)
            }
            
            sql_review = {
                'user_id': mongo_review['user_id'],
                'book_id': mongo_review['book_id'],
                'rating': mongo_review['rating'],
                'review_text': sql_review_text,
                'created_at': mongo_review['created_at'],
                'updated_at': mongo_review['updated_at'],
                'is_deleted': mongo_review['is_deleted']
            }
            
            reviews.append((sql_review, mongo_review))
        return reviews

    def test_insertion(self, num_reviews):
        reviews = self.generate_review_data(num_reviews)
        
        start_time = time.time()
        with self.pg_conn.cursor() as cur:
            for sql_review, _ in reviews:
                cur.execute("""
                    INSERT INTO reviews (user_id, book_id, rating, review_text, 
                                       created_at, updated_at, is_deleted)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    sql_review['user_id'], sql_review['book_id'], 
                    sql_review['rating'], sql_review['review_text'],
                    sql_review['created_at'], sql_review['updated_at'], 
                    sql_review['is_deleted']
                ))
        
        self.pg_conn.commit()
        pg_time = time.time() - start_time
        
        start_time = time.time()
        mongo_reviews = [mongo_review for _, mongo_review in reviews]
        self.mongo_db.reviews.insert_many(mongo_reviews)
        mongo_time = time.time() - start_time
        
        return {'postgresql': pg_time, 'mongodb': mongo_time}
    
    def test_reading(self, num_queries):
        results = {'postgresql': [], 'mongodb': []}
        
        with self.pg_conn.cursor() as cur:
            for _ in range(num_queries):
                random_user_id = random.choice(self.user_ids)
                
                start_time = time.time()
                cur.execute("""
                    SELECT *
                    FROM reviews
                    WHERE user_id = %s
                """, (random_user_id,))
                _ = cur.fetchall()
                query_time = time.time() - start_time
                results['postgresql'].append(query_time)
        
        for _ in range(num_queries):
            random_user_id = random.choice(self.user_ids)
            
            start_time = time.time()
            _ = list(self.mongo_db.reviews.find({'user_id': random_user_id}))
            query_time = time.time() - start_time
            results['mongodb'].append(query_time)
        
        return results
    
    def test_search_in_reviews(self, num_queries):
        """Test searching within review text"""
        results = {'postgresql': [], 'mongodb': []}
        
        search_terms = ['excellent', 'good', 'bad', 'recommend', 'interesting']
        
        with self.pg_conn.cursor() as cur:
            for _ in range(num_queries):
                search_term = random.choice(search_terms)
                
                start_time = time.time()
                cur.execute("""
                    SELECT *
                    FROM reviews
                    WHERE review_text ILIKE %s
                    LIMIT 10
                """, (f'%{search_term}%',))
                _ = cur.fetchall()
                query_time = time.time() - start_time
                results['postgresql'].append(query_time)
        
        for _ in range(num_queries):
            search_term = random.choice(search_terms)
            
            start_time = time.time()
            _ = list(self.mongo_db.reviews.find({
                '$or': [
                    {'main_review': {'$regex': search_term, '$options': 'i'}},
                    {'comments.comment': {'$regex': search_term, '$options': 'i'}}
                ]
            }).limit(10))
            query_time = time.time() - start_time
            results['mongodb'].append(query_time)
        
        return results

def run_performance_test(connection_params):
    tester = DatabaseTester()
    
    print("Testing insertion performance...")
    insertion_results = tester.test_insertion(1000)
    
    print("Testing reading performance...")
    reading_results = tester.test_reading(100)
    
    print("Testing search performance...")
    search_results = tester.test_search_in_reviews(100)
    
    avg_results = {
        'insertion': insertion_results,
        'reading': {
            'postgresql': sum(reading_results['postgresql']) / len(reading_results['postgresql']),
            'mongodb': sum(reading_results['mongodb']) / len(reading_results['mongodb'])
        },
        'search': {
            'postgresql': sum(search_results['postgresql']) / len(search_results['postgresql']),
            'mongodb': sum(search_results['mongodb']) / len(search_results['mongodb'])
        }
    }
    
    return avg_results

if __name__ == "__main__":
    connection_params = {
        'postgres': {
            'dbname': 'bookstore',
            'user': 'your_username',
            'password': 'your_password',
            'host': 'localhost'
        },
        'mongodb': 'mongodb://localhost:27017/'
    }
    
    results = run_performance_test(connection_params)
    print("\nTest Results:")
    print(json.dumps(results, indent=2))
    
    print("\nSummary:")
    for test_type in results:
        if isinstance(results[test_type], dict):
            print(f"\n{test_type.title()} Performance:")
            pg_time = results[test_type]['postgresql']
            mongo_time = results[test_type]['mongodb']
            diff_percent = ((pg_time - mongo_time) / pg_time) * 100
            print(f"PostgreSQL: {pg_time:.4f} seconds")
            print(f"MongoDB: {mongo_time:.4f} seconds")
            print(f"MongoDB is {abs(diff_percent):.1f}% {'faster' if diff_percent > 0 else 'slower'}")
        else:
            print(f"\nInsertion Performance:")
            pg_time = results[test_type]['postgresql']
            mongo_time = results[test_type]['mongodb']
            diff_percent = ((pg_time - mongo_time) / pg_time) * 100
            print(f"PostgreSQL: {pg_time:.4f} seconds")
            print(f"MongoDB: {mongo_time:.4f} seconds")
            print(f"MongoDB is {abs(diff_percent):.1f}% {'faster' if diff_percent > 0 else 'slower'}")