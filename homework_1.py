import sqlite3
import pandas as pd
import os

# Clear terminal 
os.system('clear')

DB_FILE = "database.sqlite"

#Connect to the database
try:
    conn = sqlite3.connect(DB_FILE)
    print("SQLite Database connection successful\n")
except Exception as e:
    print(f"Uh oh '{e}'")

# Exercise 1.1: Inspect tables, columns, row counts
print("Exercise 1.1: Inspecting tables \n")

tables = pd.read_sql_query("""
    SELECT name 
    FROM sqlite_master 
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
""", conn)

print("Tables found in database:")
print(tables, "\n")

for t in tables["name"]:
    print(f"Table: {t}")
    # columns
    cols = pd.read_sql_query(f"PRAGMA table_info({t});", conn)
    print("Columns:")
    print(cols)
    # row count
    row_count = pd.read_sql_query(f"SELECT COUNT(*) as n FROM {t};", conn)
    print("Row count:", row_count["n"][0])
    # sample rows
    sample = pd.read_sql_query(f"SELECT * FROM {t} LIMIT 5;", conn)
    print("Sample rows:")
    print(sample, "\n")

# Exercise 1.2: Lurkers
print("\n Exercise 1.2: Lurkers \n")
lurkers = pd.read_sql_query("""
    SELECT u.id, u.username
    FROM users u
    WHERE u.id NOT IN (SELECT DISTINCT user_id FROM posts WHERE user_id IS NOT NULL)
      AND u.id NOT IN (SELECT DISTINCT user_id FROM comments WHERE user_id IS NOT NULL)
      AND u.id NOT IN (SELECT DISTINCT user_id FROM reactions WHERE user_id IS NOT NULL)
    ORDER BY u.id;
""", conn)

print(f"Number of lurkers: {len(lurkers)}")
print("Sample of lurkers (first 10):")
print(lurkers.head(10))

# Exercise 1.3: Influencers 
print("\n Exercise 1.3: Influencers \n")
influencers = pd.read_sql_query("""
    SELECT
        u.id AS user_id,
        u.username,
        COUNT(DISTINCT p.id) AS posts_count,
        COALESCE(SUM(pc.comment_count), 0) AS comments_on_posts,
        COALESCE(SUM(pr.reaction_count), 0) AS reactions_on_posts,
        COALESCE(SUM(pc.comment_count), 0) + COALESCE(SUM(pr.reaction_count), 0) AS total_engagement
    FROM users u
    LEFT JOIN posts p ON p.user_id = u.id
    LEFT JOIN (
        SELECT post_id, COUNT(*) AS comment_count
        FROM comments
        GROUP BY post_id
    ) pc ON pc.post_id = p.id
    LEFT JOIN (
        SELECT post_id, COUNT(*) AS reaction_count
        FROM reactions
        GROUP BY post_id
    ) pr ON pr.post_id = p.id
    GROUP BY u.id, u.username
    ORDER BY total_engagement DESC, posts_count DESC
    LIMIT 5;
""", conn)

print("Top 5 influencers:")
print(influencers)

# Exercise 1.4: Spammers 
print("\n Exercise 1.4: Spammers \n")
spammers = pd.read_sql_query("""
    SELECT
        combined.user_id,
        u.username,
        combined.text_content,
        combined.occurrences
    FROM (
        SELECT user_id, text_content, COUNT(*) AS occurrences
        FROM (
            SELECT user_id, content AS text_content FROM posts WHERE content IS NOT NULL
            UNION ALL
            SELECT user_id, content AS text_content FROM comments WHERE content IS NOT NULL
        )
        GROUP BY user_id, text_content
        HAVING COUNT(*) >= 3
    ) combined
    LEFT JOIN users u ON u.id = combined.user_id
    ORDER BY combined.occurrences DESC;
""", conn)

print("Users with repeated text (potential spammers):")
print(spammers.head(20))

#  Close connection 
conn.close()
