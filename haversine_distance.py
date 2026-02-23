import math

lat1 = 50.3835021304354*(3.14159265359/180)
long1 = 23.4406433653009*(3.14159265359/180)

lat2 = 50.041187*(3.14159265359/180)
long2 = 21.999121*(3.14159265359/180)

delta_lat = lat2-lat1
delta_long = long2-long1

hav0 = (1-math.cos(delta_lat) + math.cos(lat1)*math.cos(lat2)*(1-math.cos(delta_long)))/(2)

angle = 2*math.asin(math.sqrt(hav0)*3.14159265359/180)

angle=angle*(180/3.14159265359)
distance = (f'{angle*6371.2*1000} m or {angle*6371.2} km')

print(distance)


""" 
lat1 = 52.229675*(3.14159265359/180)
long1 = 21.012230*(3.14159265359/180)

lat2 = 52.227740*(3.14159265359/180)
long2 = 21.002040*(3.14159265359/180) """