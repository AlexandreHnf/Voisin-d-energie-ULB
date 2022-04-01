import time 
import random 

print("before sleep..")

t = random.randint(1, 20)
print("sleep({})".format(t))

time.sleep(t)

print("after sleep..")