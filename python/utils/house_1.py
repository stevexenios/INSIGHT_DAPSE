'''
Control House 1

All data streaming from 8 iot devices sent as an whole. 
6 fields store location related data.
'''
from devices import mutator

# House 1 seed
house_1_device = {
        'Tarriff':'normal',
        'Energy': 0.143,
        'Humidity': 34.30,
        'Temperature': 73.12,
        'CO':4.22,
        'SO2':64.23,
        'NO2': 36.45,
        'O3': 44.63,
        'PM_2_5': 8.77,
        'Zip' : 98155,
        'State': 'WA',
        'City': 'SEATTLE',
        'Address': '501 NE 145TH ST',
        'Y' : 47.7342302,
        'X': -122.3232189
    }

# House 1 generator called in producer.py
def generate_house_1():
    global house_1_device
    temp = mutator(house_1_device)
    return temp
