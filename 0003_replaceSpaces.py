import re
def replaceSpaces(str):
    # replacements = {" / ": "/", ", ": ","}
    # for replaceFrom, replaceTo in replacements.items():
    #     str = str.replace(replaceFrom, replaceTo)
    output = re.sub(r'\s\s+', '^-^', str).split('^-^')
    return output


str = '4ba3f8b5b23e   cdockerapi    0.00%     0B / 0B             0.00%     0B / 0B   0B / 0B     0'
str = '"9253487c4434"   idockerapi   "uvicorn api.main:ap…"   16 minutes ago   Up 2 minutes   "0.0.0.0: 8002->80 / tcp"   cdockerapi'
print(replaceSpaces(str))