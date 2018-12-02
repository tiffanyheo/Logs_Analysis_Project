# "Logs Analysis Project"

import psycopg2
from datetime import datetime
import time


DBNAME = "news"  # Define the database name

db = psycopg2.connect(database=DBNAME)  # Connect the database

c = db.cursor()

c.execute("create view articles_with_count as "
          "select articles.id as id, count(log.id) as num "
          "from articles left join log "
          "on articles.slug = replace(path, '/article/','') "
          "group by articles.id ")

# 1
print("What are the most popular three articles of all time?")
c.execute("select articles.title, articles_with_count.num "
          "from articles join articles_with_count "
          "on articles.id = articles_with_count.id "
          "order by num desc limit 3")
results = c.fetchall()
for row in results:
    print(str(row[0])+" --- "+str(row[1])+" views")

print("\n")

# 2
print("Who are the most popular article authors of all time?")
c.execute("select authors.name, sum(articles_with_count.num) as sum "
          "from articles, authors, articles_with_count "
          "where articles.author = authors.id "
          "and articles.id = articles_with_count.id "
          "group by authors.name order by sum desc limit 1")
results = c.fetchall()
for row in results:
    print(str(row[0])+" --- "+str(row[1])+" views")

print("\n")

# 3
print("On which days did more than 1% of requests lead to errors?")
c.execute("create view counting_view as "
          "select TO_CHAR(time, 'YYYY-MM-DD') as time_formatted, "
          "count(case when status !='200 OK' then 1 end) as error_count, "
          "count(id) as total_count from log group by time_formatted")
c.execute("create view counting_view_with_percentage as "
          "select time_formatted, error_count, total_count, "
          "CAST(error_count AS FLOAT)/CAST(total_count AS FLOAT) as percent "
          "from counting_view")
c.execute("select * from counting_view_with_percentage "
          "where percent > 0.01 order by time_formatted")
results = c.fetchall()
for row in results:
    print(str(row[0])+" --- "+"{:.2%}".format(row[3])+" errors")

print("\n")
db.close()
