import os

target_dir = '/home/sadmin/Work/projects/py-test/pypract'
res = []
if not os.path.isdir(target_dir):
    res = 'Ошибка: указанная директория не сущестует или не доступна'
else:
    res = 'Найдено файлов: ' + str(len( list( f for f in  os.listdir(target_dir) if os.path.isfile(f) ) ))
print((res))