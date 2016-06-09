import multiprocessing as mp
import os
import time, datetime
import cStringIO
import csv

def get_dict_from_file(file_name):
	file_name.seek(0)
	reader = csv.reader(file_name, delimiter=',')
	dictionary = {}
	for i in xrange(0,2001,1):
		dictionary[str(i)] = ''
	
	for row in reader:
		dictionary[row[0]] = row[1]
	return dictionary

def put_to_file(dicts, fout):
	writer = csv.writer(fout, delimiter=',')
	
	for field in dicts.keys():
		for field2 in dicts[field].keys():
			for field3 in dicts[field][field2].keys():
				writer.writerow([field, field2, field3,\
								dicts[field][field2][field3]['request'],\
								dicts[field][field2][field3]['show'],\
								dicts[field][field2][field3]['click']])

def put_to_file_banner(dicts, banner_dict, fout):
	writer = csv.writer(fout, delimiter=',')
	banner_name = ''
	for d in dicts.keys():
		for banner_id in dicts[d].keys():
			banner_name = banner_dict[banner_id]
			for field3 in dicts[d][banner_id].keys():
				writer.writerow([d, banner_name, field3,\
								dicts[d][banner_id][field3]['request'],\
								dicts[d][banner_id][field3]['show'],\
								dicts[d][banner_id][field3]['click']])

def group_by(reader, banner_dict, fout, dicts, lock, date_format, index1, index2):
	r = reader.next()
	while not r:
		r = reader.next()
	if date_format == '%Y%m%d':
		d = r[1].split()[0] # d - beginning date in format YYYY-MM-DD
		new_date = datetime.datetime.strptime(d, '%Y-%m-%d')
		date_key = datetime.datetime.strftime(new_date, '%Y%m%d')
	else: # '%Y%m%d%H'
		d = r[1][0:13] # d - beginning date in format YYYY-MM-DD HH
		new_date = datetime.datetime.strptime(d, '%Y-%m-%d %H')
		date_key = datetime.datetime.strftime(new_date, '%Y%m%d%H')
	field1 = r[index1]
	field2 = r[index2]
	action = r[2]
	dicts[date_key] = {field1: {field2: {'request': 0, 'show': 0, 'click': 0}}}
	dicts[date_key][field1][field2][action] += 1
		
	for r in reader:
		if r:
			if date_format == '%Y%m%d':
				date = r[1].split()[0]
			else:
				date = r[1][0:13]
			field1 = r[index1]
			field2 = r[index2]
			action = r[2]
			if date == d:
				if dicts[date_key].has_key(field1):
					if not dicts[date_key][field1].has_key(field2):
						dicts[date_key][field1][field2] = {'request': 0, 'show': 0, 'click': 0}
				else:
					dicts[date_key][field1] = {field2: {'request': 0, 'show': 0, 'click': 0}}
				dicts[date_key][field1][field2][action] += 1
			else:
				lock.acquire()
				if index1 == 3: # if it is banner_id field
					put_to_file_banner(dicts, banner_dict, fout)
				else:
					put_to_file(dicts, fout)
				lock.release()
				
				dicts.pop(date_key, None)
				d = date
				if date_format == '%Y%m%d':
					new_date = datetime.datetime.strptime(d, '%Y-%m-%d')
					date_key = datetime.datetime.strftime(new_date, '%Y%m%d')
				else:
					new_date = datetime.datetime.strptime(d, '%Y-%m-%d %H')
					date_key = datetime.datetime.strftime(new_date, '%Y%m%d%H')
				dicts[date_key] = {}

def do_stuff(args):
	pos1 = args[0]
	pos2 = args[1]
	choice = args[2]
	dicts = args[3]
	banner_dict = args[4]
	
	f_rawdata = open('rawdata.csv', 'r')
	counter = 0
	actions = {'request': 0, 'show': 0, 'click': 0}
	lock = mp.Lock()
	
	f_rawdata.seek(pos1)
	str = cStringIO.StringIO()
	while f_rawdata.tell() < pos2:
		str.write(f_rawdata.readline())
	reader = csv.reader(str, delimiter=',')
	f_rawdata.close()
	
	fout = open('output.csv', 'a')
	
	str.seek(0)
	if choice == 1: 
		group_by(reader, banner_dict, fout, dicts, lock, '%Y%m%d', 3, 4)
	elif choice == 2:
		group_by(reader, banner_dict, fout, dicts, lock, '%Y%m%d', 3, 5)
	elif choice == 3:
		group_by(reader, banner_dict, fout, dicts, lock, '%Y%m%d%H', 3, 5)
	else:
		group_by(reader, banner_dict, fout, dicts, lock, '%Y%m%d%H', 4, 5)
	str.close()
	fout.close()

def user_choice():
	formats = {
	1 : 'yearmonthday banner_name browser',
	2 : 'yearmonthday banner_name device',
	3 : 'yearmonthdayhour banner_name device',
	4 : 'yearmonthdayhour browser device',
	0 : 'exit'
	}
	choice = input('Choose a number of format:\n1: {}\n2:\
 {}\n3: {}\n4: {}\n0: {}\n>> '.format(formats[1],
									 formats[2],
									 formats[3],
									 formats[4],
									 formats[0]))
	
	try:
		choice = int(choice)
		print('\nYour choice is: ' + formats[choice] + '\n')
		return choice
	except KeyError as e:
		print('\nUndefined unit: {}'.format(e.args[0]))
		print('Try again!\n')
		return user_choice()

if __name__ == '__main__':
	choice = user_choice()
	if choice == 0:
		print 'Bye'
	else:
		split_size = 100*1024*1024
		tasks = []
		cursor = 0
		dicts = {}
		
		f_rawdata = open('rawdata.csv', 'r')
		#Rewrite output.csv file with empty one
		outputfile = open('output.csv', 'w')
		outputfile.close()
		# -------------------
		
		f_banner = open('lookup.csv')
		banner_dict = get_dict_from_file(f_banner)
		
		f_rawdata.seek(0, os.SEEK_END)
		size = f_rawdata.tell()
		
		
		print '......................'
		print 'Please wait a few minutes ...'
		value_part = size // split_size + 1
		for part in xrange(value_part):
			if cursor + split_size > size:
				end = size
			else:
				end = cursor + split_size
				f_rawdata.seek(end)
				f_rawdata.readline() # read tail af line
				line = f_rawdata.readline() # read whole line
				hour = datetime.datetime.strptime(line.split(',')[1], '%Y-%m-%d %H:%M:%S.%f').time().hour
				h = hour
				while h == hour:
					line = f_rawdata.readline()
					h = datetime.datetime.strptime(line.split(',')[1], '%Y-%m-%d %H:%M:%S.%f').time().hour
				end = f_rawdata.tell() - len(line) # seek begin of line
			tasks.append((cursor,end,choice,dicts,banner_dict))
			cursor = end
		
		f_rawdata.close()
		f_banner.close()
		
		for task in tasks:
			pr = mp.Process(target=do_stuff, args=(task, ))
			pr.start()
			pr.join()
		
		print 'Thank you for waiting!'
		
		print '\nYour result file is "output.csv".'
