import dbquery as dbq
import json
import datetime
from sqlalchemy import text
# скрипт парсит json файл 0008-data/test_10_1_new.json , в виде блоков json кладет в поля типа json, в базу
def readInputFile(filename='output_markup.json'):
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
	nn_output['files'] = nn_output['files'][:3]
	count_inserts = total_inserts = 0
	stmt = f'INSERT INTO common.markups( id, previous_id, dataset_id, file_id, parent_id, mark_time, mark_path, vector, description, author_id, dt_created, is_deleted ) VALUES '
	query = ''
	for f in nn_output['files']:
		for chain in f['file_chains'] :
			for  chain_markup in chain['chain_markups']:
				s = json.dumps(chain_markup)
				query += f'(\'{dbq.getUuid()}\', null, null, null, null, 99, \'{s}\', \'{s}\', \'tread\', null, \'2024-09-16 12:00:00\', false),'
				if(count_inserts == 1000 ):
					total_inserts += count_inserts
					dbq.tread_mark_insert_batch( stmt+query[:-1])
					count_inserts = 0
					query = ''
				count_inserts += 1 
		# break
	if(query):
		total_inserts += count_inserts
		dbq.tread_mark_insert_batch( stmt+query[:-1])

	print(f"Total: {total_inserts}")
	# chains = nn_output['files'][0]['file_chains']
	# for chain in chains:
	# 	for markup in chain['chain_markups']:
	# 		print(markup['markup_path'])

			# mp = markup['markup_path']
			# mv = '' #json.dumps(chain['chain_markups']['markup_vector'])
			# dbq.tread_mark_insert(mp, mv)

	# Parse index
	# key_index = list(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_path'][0].keys())[0][5:]
	# print( key_index )
	
	# Single query
	# mp = json.dumps(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_path'])
	# mv = json.dumps(nn_output['files'][0]['file_chains'][0]['chain_markups']['markup_vector'])
	# dbq.tread_mark_insert(mp, mv) # with tread
	# dbq.q_mark_insert(mp, mv)

	print('Done  ' + str(datetime.datetime.now()))