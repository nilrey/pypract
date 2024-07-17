import dbquery as dbq
import json
import datetime
import time

# dbq.q_user_insert('user10')
INSERT_ROWS_LIMIT = 100

def readInputFile(filename='test_10_0.json'):
	filepath = '0008-data/'+filename
	with open(filepath, "r") as file:
		obj_data = json.load(file)
	return obj_data

def dbq_tread_mark_insert(chains):
	rows_cnt = 0
	for chain in chains:
		dbq.tread_mark_insert(json.dumps(chain['chain_markups']['markup_path']))
		rows_cnt += 1
	return rows_cnt
	

if(__name__ == "__main__"):
	print('Start ' + str(datetime.datetime.now()))
	nn_output = readInputFile()
	filename = nn_output['files'][0]['file_name']
	chains = nn_output['files'][0]['file_chains']
	rows_cnt = 0
	for chain in chains:

		mp = json.dumps(chain['chain_markups']['markup_path'])
		mv = json.dumps(chain['chain_markups']['markup_vector'])
		dbq.tread_mark_insert(mp, mv)
	# 	mp = json.dumps(chain['chain_markups']['markup_path'])
	# 	dbq.q_mark_insert(mp)
	# 	if (rows_cnt < INSERT_ROWS_LIMIT):
	# 		rows_cnt += 1
	# 	else:
	# 		rows_cnt = 0
	# 		# print('Sleep for 3 sec.')
	# 		# time.sleep(3)
	# 		# print(str(datetime.datetime.now()))

		# chain_markups, nn_output['files'][0]['file_chains']['chain_markups']['markup_path'][0]

	# print( type(json.dumps(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_path'][0]) ))
	# keysdict = list(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_path'][0].keys())[0][5:]
	# print( keysdict )
	# mp = json.dumps(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_path'])
	# dbq.tread_mark_insert(mp)

	# mp = json.dumps(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_path'])
	# mv = json.dumps(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_vector'])
	# dbq.q_mark_insert(mp, mv)
	print('Done  ' + str(datetime.datetime.now()))