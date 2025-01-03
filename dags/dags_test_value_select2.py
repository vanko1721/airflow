import psycopg2 # type: ignore

db = psycopg2.connect(host='172.28.0.3', dbname='vanko1721',user='vanko1721',password='vanko1721',port=5432)
cursor=db.cursor()
def execute(self,query):
    query = 'select col2 from value_test where col1 =1'
    self.cursor.execute(query)
    row = self.cursor.fetchall()
    return row
