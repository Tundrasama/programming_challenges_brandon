import sqlite3
from pandas import read_csv, DataFrame, read_sql_query

def findMatches():
	conn = create_connection(db_file)

	stmt = 'SELECT DISTINCT a.timestamp,a.close_price from ticks a INNER JOIN ticks b on a.close_price between b.lower_lim and b.upper_lim'
	groupstmt = "SELECT ROWID, timestamp,group_concat(close_price,'|') as matches from (SELECT DISTINCT a.ROWID,b.ROWID,a.timestamp,b.close_price from ticks a INNER JOIN ticks b on a.close_price between b.lower_lim and b.upper_lim) GROUP BY ROWID"

	matches = DataFrame(read_sql_query(stmt, conn))
	groupedmatches = DataFrame(read_sql_query(groupstmt, conn))
	
	matches.to_csv('matches.csv',sep=',')
	groupedmatches.to_csv('grouped_matches.csv',sep=',')

def import_csv(csv_file,db_file):
	conn = create_connection(db_file)
	data = DataFrame(read_csv(csv_file,header=0))
	data['lower_lim']=data['close_price']-0.06
	data['upper_lim']=data['close_price']+0.06
	data.to_sql('ticks',conn,if_exists='replace')

def create_connection(db_file):
	conn = None
	try:
		conn = sqlite3.connect(db_file)
	except NameError as e:
		print(e)
		
	return conn

if __name__ == "__main__":
	db_file = 'stocks.db'
	import_csv('stocks.csv','stocks.db')
	findMatches()