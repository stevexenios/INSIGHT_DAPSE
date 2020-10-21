'''
Test House 2. This one complements data from house 3, since they are the same house.

Data streaming from 3 iot devices (Temperature(C), CO, Energy). 
Location is in form address only.
'''
from house_3 import house_3_device
from devices import mutator

def extract_house_5_data(v):
    location_string = v['Address']+' '+ v['City']+' '+v['State']+' '+str(v['Zip']) 
    return {
        "Energy": v['Energy'],
        "CO":v['CO'],
        "Temperature":v['Temperature'],
        "Address":location_string
    }

# Mutate and extract from the same dictionary for house 2(control) and 4(test)
def generate_house_3_and_5():
    house_3 = mutator(house_3_device)
    house_3['Tarriff'] = 'normal'
    house_5 = extract_house_5_data(house_3)
    return {"h3": house_3, "h5":house_5}