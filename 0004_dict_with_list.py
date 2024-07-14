page = {
    "page": 1,
    "pageSize": 15,
    "totalItems": 33,
    "totalPages": 6
}
items = [  
    {
        "REPOSITORY": "idockerapi",
        "TAG": "latest",
        "IMAGE_ID": "95c33a1e257d",
        "CREATED": "24_seconds_ago",
        "SIZE": "1.38GB"
    },
    {
        "REPOSITORY": "imockneuro",
        "TAG": "latest",
        "IMAGE_ID": "3729fa5ef76c",
        "CREATED": "17_hours_ago",
        "SIZE": "1GB"
    }
]
result = {'page':page, 'items':items}
print(result['items'][0]['TAG'])