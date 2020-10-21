'''
Author: Steve Mwangi
File for mutating the the telemtry device data
'''
import random

# Function to quasi randomly mutate value given from 0 to 99 of 1% depending on start
def rand(start):
    result = round(start + start * random.random() / 100 * random.choice([-1,0,1]), 2)
    if result < 0:
        return 0
    return result

# Mutates what is in the seed dictionary by using helper function above
def mutator(seed):
    # 'PM_2_5'
    seed['PM_2_5'] = rand(seed['PM_2_5'])
    
    # 'O3'
    seed['O3'] = rand(seed['O3'])
    
    # 'NO2'
    seed['NO2'] = rand(seed['NO2'])

    # 'SO2'
    seed['SO2'] = rand(seed['SO2'])
    
    # 'CO'
    seed['CO'] = rand(seed['CO'])

    # 'Temperature'
    seed['Temperature'] = rand(seed['Temperature'])
    
    # 'Humidity'
    seed['Humidity'] = rand(seed['Humidity'])

    # 'Energy'
    seed['Energy'] = rand(seed['Energy'])
    
    # 'Tarriff'
    seed['Tarriff'] = random.choice(["normal", "low", "high"])

    return seed

