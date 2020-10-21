'''
Test House 1. This one complements data from house 2, since they are the same house.

Data streaming from 4 iot devices (Temperature(F), CO, Energy and Tarriff). 
Location is in form of longitude(x) and latitude(y).
'''
from house_2 import house_2_device
from devices import mutator

def extract_house_4_data(v):
    return {
        "Tarriff": v['Tarriff'],
        "Energy": v['Energy'],
        "Temperature":v['Temperature'],
        "CO":v['CO'],
        "Y":v['Y'],
        "X":v['X']
    }

# Mutate and extract from the same dictionary for house 2(control) and 4(test)
def generate_house_2_and_4():
    house_2 = mutator(house_2_device)
    house_4 = extract_house_4_data(house_2)
    return {"h2": house_2, "h4":house_4}

