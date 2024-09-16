import dbquery as dbq
import json
import datetime
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
	for f in nn_output['files']:
		print(f['file_name'])

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