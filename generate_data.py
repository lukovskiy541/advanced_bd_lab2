from faker import Faker
import psycopg2
import random
from datetime import datetime

fake = Faker()

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="3494",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

categories = [
    'Fiction', 'Non-fiction', 'Science', 'History', 'Philosophy', 'Biography', 'Fantasy', 'Mystery', 'Romance', 'Poetry'
]
categories_descriptions = [
    "Fiction: Stories that are invented or imagined, often involving characters, plots, and settings that are not based on real events.",
    "Non-fiction: Factual content based on real events, people, and facts, including biographies, documentaries, and true stories.",
    "Science: Books that explore scientific concepts, research, and discoveries, covering topics like physics, biology, and chemistry.",
    "History: Narratives and analysis of past events, exploring the development of human civilizations, cultures, and important historical moments.",
    "Philosophy: Books focused on the study of fundamental questions about existence, knowledge, values, reason, and reality.",
    "Biography: Detailed accounts of a person's life, highlighting their achievements, struggles, and impact on society.",
    "Fantasy: Fictional works featuring magical or supernatural elements, often set in imaginary worlds.",
    "Mystery: Books centered around solving a crime or uncovering a secret, typically involving detectives or investigators.",
    "Romance: Fictional works that focus on relationships, emotions, and love stories, often with a happy ending.",
    "Poetry: Literary works that use rhythmic and aesthetic qualities of language to evoke emotions, thoughts, or imaginations in readers."
]



publishers = [
    ('Penguin Random House', '1745 Broadway, New York, NY 10019', 'contact@penguinrandomhouse.com', '+1 212-366-2000'),
    ('HarperCollins', '195 Broadway, New York, NY 10007', 'info@harpercollins.com', '+1 212-207-7000'),
    ('Simon & Schuster', '1230 Avenue of the Americas, New York, NY 10020', 'contact@simonandschuster.com', '+1 212-698-7000'),
    ('Hachette Book Group', '1290 Avenue of the Americas, New York, NY 10104', 'contact@hachettebookgroup.com', '+1 212-364-1100')
]



for _ in range(100):  # Генеруємо 100 книжок
    isbn = fake.isbn13()[:13]  # Генеруємо ISBN та обрізаємо до 13 символів
    title = fake.sentence(nb_words=5)  # Генеруємо назву книги
    description = fake.text(max_nb_chars=300)  # Генеруємо опис
    price = round(random.uniform(10, 100), 2)  # Випадкова ціна від 10 до 100
    publication_date = fake.date_this_century()  # Випадкова дата публікації
    last_updated_by = random.randint(1, 100)  # Випадковий user_id для останнього оновлення
    book_type_id = random.randint(1, len(categories))  # Випадковий тип книжки
    publisher_id = random.randint(1, len(publishers))  # Випадковий видавець
    is_deleted = random.choice([True, False])  # Випадковий статус видалення

    # SQL запит для вставки книги
    cursor.execute("""
        INSERT INTO books (isbn, title, description, price, publication_date, created_at, updated_at, 
            last_updated_by, book_type_id, publisher_id, is_deleted)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (isbn, title, description, price, publication_date, fake.date_this_decade(), fake.date_this_decade(),
          last_updated_by, book_type_id, publisher_id, is_deleted))

conn.commit()



num_reviews = 1000

cursor.execute("SELECT user_id FROM users")
existing_user_ids = [row[0] for row in cursor.fetchall()]

for _ in range(num_reviews):
    user_id = random.randint(1, 100)
    book_id = random.randint(1, 100)
    rating = random.randint(1, 5)
    review_text = fake.text(max_nb_chars=200)
    created_at = fake.date_this_decade()
    updated_at = created_at
    is_deleted = random.choice([True, False])

    cursor.execute("""
        INSERT INTO reviews (user_id, book_id, rating, review_text, created_at, updated_at, is_deleted)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (user_id, book_id, rating, review_text, created_at, updated_at, is_deleted))


conn.commit()

cursor.close()
conn.close()

print(f"Generated {num_reviews} reviews successfully.")
