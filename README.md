# car-accidents-data-processing
The goal of this project is to infer qualitative data regarding the car accidents in New York City. The development exploit both OpenMP within a single process and MPI to distribute the processing load across multiple machines.

Each row in the data set contains the following data:
DATE, TIME, BOROUGH, ZIP CODE, LATITUDE, LONGITUDE, LOCATION, ON STREET NAME, CROSS
STREET NAME, OFF STREET NAME, NUMBER OF PERSONS INJURED, NUMBER OF PERSONS KILLED,
NUMBER OF PEDESTRIANS INJURED, NUMBER OF PEDESTRIANS KILLED, NUMBER OF CYCLIST
INJURED, NUMBER OF CYCLIST KILLED, NUMBER OF MOTORIST INJURED, NUMBER OF MOTORIST
KILLED, CONTRIBUTING FACTOR VEHICLE 1, CONTRIBUTING FACTOR VEHICLE 2, CONTRIBUTING
FACTOR VEHICLE 3, CONTRIBUTING FACTOR VEHICLE 4, CONTRIBUTING FACTOR VEHICLE 5, UNIQUE
KEY, VEHICLE TYPE CODE 1, VEHICLE TYPE CODE 2, VEHICLE TYPE CODE 3, VEHICLE TYPE CODE 4,
VEHICLE TYPE CODE 5

By contributing factor we intend the reason why the accident took place in the first place.

Main focus on extracting the following information:

  • Number of lethal accidents per week throughout the entire dataset.
  
  • Number of accidents and percentage of number of deaths per contributing factor in the
    dataset.
    
      o I.e., for each contributing factor, we want to know how many accidents were due to
      that contributing factor and what percentage of these accidents were also lethal.
      
  • Number of accidents and average number of lethal accidents per week per borough.
  
      o I.e., for each borough, we want to know how many accidents there were in that
        borough each week, as well as the average number of lethal accidents that the
        borough had per week.
