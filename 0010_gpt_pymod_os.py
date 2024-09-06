import os.path

cwd = os.getcwd()
cwd = os.listdir('.')
# print(os.path.isfile('0001_math_multiplication.py'))
# print(cwd)

ev = os.environ.get('PATH')
print(ev)