from datetime import datetime, timedelta
import random

print(datetime(1,10,1,10,10,20,30000))
print(timedelta(seconds=20))
print(timedelta(1,2,3))
print(timedelta(1.521239,2.99,3.2))
today = datetime.now()
print(today.strftime("%Y-%m-%d"))
print(random.randint(0,1))

a = {"a":1,"b":2}
b = {"c":3}
print(a.update(b))
print(a,b)

print([1,2,])


import requests

