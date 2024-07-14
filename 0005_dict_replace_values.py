
def replaceHeaderTitles(headers):
    replacements = {'IMAGE_ID':'id', 'REPOSITORY':'name', 'TAG':'tag', 'location':'location', 'CREATED':'created_at', 'SIZE':'size'}
    newheaders = []
    for header in headers:
        if header in replacements.keys():
            newheader = header.replace(header, replacements[header]) 
            newheaders.append(newheader)
        else:
            newheaders.append(header)
    return newheaders

h = ['test', 'IMAGE_ID', 'aaaaa', 'REPOSITORY', 'CREATED']
h = replaceHeaderTitles(h)
print(h)

