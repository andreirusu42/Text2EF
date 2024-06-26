import sqlparse
from sql_formatter.core import format_sql

sql = "SELECT * FROM users"

formatted_sql = format_sql(sql)

formatted_sql = formatted_sql.replace('avg (', 'AVG(')

print(formatted_sql)

parsed = sqlparse.parse(formatted_sql)

print(parsed[0].tokens)
