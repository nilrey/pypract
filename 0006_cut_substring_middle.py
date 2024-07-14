import re

str = '"9253487c4434"   idockerapi   "uvicorn api.main:ap…"   16 minutes ago   Up 2 minutes   "0.0.0.0: 8002->80 / tcp"   cdockerapi'
substrs = re.findall('"([^"]*)"', str)
# replace
for index, txt in enumerate(substrs):
    str = str.replace(txt, '*'*(10-index) )
print(str)
# restore
for index, txt in enumerate(substrs):
    str = str.replace('*'*(10-index), txt )
print(str)



for index, txt in enumerate(substrs):
    print(txt)
    str = str.replace(txt, txt.replace(' ', '_') )
# print(str)