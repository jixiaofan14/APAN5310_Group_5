# ETL (Data Cleaning, Simulation and Insertion)
# 1 Data Cleaning and Simulation
# 1.1 Customer Information
# 1.1.1 Customer
# import packages
import pandas as pd
import random
first_name = pd.read_excel('names.xlsx', sheet_name='first_name', header=None)
last_name = pd.read_excel('names.xlsx', sheet_name='last_name', header=None)
# Combine first_names with last_names
customer = pd.DataFrame(columns=['first_name', 'last_name'])
while len(customer) < 1000:
    first = random.choice(first_name.values.flatten())
    last = random.choice(last_name.values.flatten())
    name = f'{first} {last}'
    if name not in customer.values:
        customer = pd.concat([customer, pd.DataFrame({'first_name': [first], 'last_name': [last]})], ignore_index=True)
# Simulate email with first_names and last_names
customer['email'] = customer.apply(lambda row: f"{row['first_name']}{row['last_name']}@sqlmail.com", axis=1)
customer['email'] = customer['email'].str.lower()
# Generate random phone numbers
def random_phone():
    return str(random.randint(1, 9)) + ''.join([str(random.randint(0, 9)) for i in range(9)])
customer['phone_num'] = customer.apply(lambda row: random_phone(), axis=1)
# Generate random driver license numbers
def generate_license():
    license_num = ''
    for i in range(8):
        license_num += str(random.randint(0, 9))
    return license_num
driver_license = [generate_license() for i in range(1000)]
customer['driver_license'] = driver_license
# Add customer_id to the dataframe
customer['customer_id'] = 'c' + customer.index.map(lambda x: str(x+1).zfill(4))
customer.insert(0, 'customer_id', customer.pop('customer_id'))
# Use mock data to add addresses to each customer and organize the dataframe
df = pd.read_csv('MOCK_DATA_customer.csv', encoding='latin1')
customer[['street_address_1','street_address_2','city','state','zipcode']] = df[['street_address_1','street_address_2','city','state','zipcode']]
customer = customer.loc[:, ['customer_id', 'first_name', 'last_name', 'phone_num', 'email', 'street_address_1', 'street_address_2', 'city', 'state', 'zipcode']]
customer.tail()
customer.to_csv('customer.csv', index=False)

# 1.1.2 Credit_card
# Import packages and create credit_card with random expiration date
import pandas as pd
import random
import numpy as np
credit_card = pd.DataFrame(columns=['expiration_month', 'expiration_year'])
for i in range(1200):
    month = random.randint(1, 12)
    year = random.randint(2024, 2030)
    credit_card = pd.concat([credit_card, pd.DataFrame({'expiration_month': [month], 'expiration_year': [year]})], ignore_index=True)
# Import card_num and card_type from mock data
df = pd.read_csv('MOCK_DATA_credit_card.csv', encoding='latin1')
df.drop_duplicates(subset=['card_num'], inplace=True)
credit_card[['card_num','card_type']] = df[['card_num','card_type']]
# Assign customer_ids to the cards and organize the dataframe
credit_card['customer_id'] = 'c' + credit_card.index.map(lambda x: str(x+1).zfill(4))
credit_card.insert(0, 'customer_id', credit_card.pop('customer_id'))
credit_card.loc[1000:1199, 'customer_id'] = np.array(['c' + str(i).zfill(4) for i in range(1, 201)])
credit_card = credit_card.loc[:, ['card_num', 'card_type', 'expiration_month', 'expiration_year', 'customer_id']]
credit_card.tail()
credit_card.to_csv('credit_card.csv', index=False)


# 1.2 Hotel Information
# 1.2.1 Hotel
# Import packages and the dataset
import pandas as pd
df = pd.read_csv('100_hotels.csv', encoding='latin1')
# Rename the columns
hotel = df[['Hotel', 'Location', 'Country', 'Region', 'Rooms']]
hotel.columns = ['name', 'city', 'country', 'region', 'num_rooms']
# Generate random phone numbers
import random
def generate_phone_number():
    first_digit = random.randint(1, 9)
    rest_digits = ''.join([str(random.randint(0, 9)) for i in range(9)])
    return str(first_digit) + rest_digits
hotel.loc[:, 'phone_num'] = [generate_phone_number() for i in range(len(hotel))]
# Simulate stars for the hotels
import numpy as np
median_score = np.percentile(df['Score'], 50)
def generate_stars(score):
    if score >= median_score:
        if np.random.random() < 0.4:
            return 5
        elif np.random.random() < 0.7:
            return 4
        else:
            return 3
    else:
        if np.random.random() < 0.3:
            return 3
        elif np.random.random() < 0.6:
            return 4
        else:
            return 5
hotel['stars'] = df['Score'].apply(generate_stars)
# Replace hotel addresses with mock data
df = pd.read_csv('MOCK_DATA_hotel.csv', encoding='latin1')
hotel[['street','city','state','zipcode']] = df[['street','city','state','zipcode']]
# Add hotel_id and organize the dataframe
hotel['hotel_id'] = 'h' + hotel.index.map(lambda x: str(x+1).zfill(3))
hotel.insert(0, 'hotel_id', hotel.pop('hotel_id'))
hotel = hotel.loc[:, ['hotel_id', 'name', 'stars', 'street', 'city', 'state', 'zipcode', 'num_rooms', 'phone_num']]
hotel.tail()
hotel.to_csv('hotel.csv', index=False)

# 1.2.2 Room_type
# Import packages and mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_room_type.csv', encoding='latin1')
# Simulate bed sizes and remove duplicates
import random
room_type = df.copy()
bed_size_options = ['queen', 'king', 'twin', 'hollywood twin', 'double-double', 'studio']
room_type['bed_size'] = [random.choice(bed_size_options) for _ in range(len(df))]
room_type.drop_duplicates(subset=['room_desc', 'bed_size'], inplace=True)
# Add type_id and organize the dataframe
room_type.reset_index(drop=True, inplace=True)
room_type['type_id'] = 't' + room_type.index.map(lambda x: str(x+1).zfill(3))
room_type.insert(0, 'type_id', room_type.pop('type_id'))
room_type = room_type.loc[:, ['type_id','room_price','room_desc','footprint','bed_size']]
room_type.tail()
room_type.to_csv('room_type.csv', index=False)

# 1.2.3 Room
# Import packages and the mock data, and rename the columns
import pandas as pd
df = pd.read_csv('MOCK_DATA_room.csv', encoding='latin1')
room = df.rename(columns={'Occupancy': 'occupancy', 'Note': 'note'}).assign(hotel_id='', type_id='')
# Randomly assign a hotel_id and a type_id to each room
import random
import string
hotel_ids = ['h{:03d}'.format(i) for i in range(1, 101)]
random.shuffle(hotel_ids)
room['hotel_id'] = [random.choice(hotel_ids) for _ in range(len(room))]
type_ids = ['t{:03d}'.format(i) for i in range(1, len(room_type))]
random.shuffle(type_ids)
room['type_id'] = [random.choice(type_ids) for _ in range(len(room))]
# Randomly assign a room number to each room
import numpy as np
room_num = np.random.randint(low=100, high=10000, size=len(room))
room['room_num'] = room_num
# Add room_id and organize the dataframe
room.reset_index(drop=True, inplace=True)
room['room_id'] = 'r' + room.index.map(lambda x: str(x+1).zfill(4))
room.insert(0, 'room_id', room.pop('room_id'))
room = room.loc[:, ['room_id','hotel_id','type_id','room_num','occupancy','note']]
room.tail()
room.to_csv('room.csv', index=False)

# 1.2.4 Hotel_reservation
# Import packages and the mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_hotel_reservation.csv', encoding='latin1')
hotel_reservation = df
# Generate check_out_date based on check_in_date and duration
hotel_reservation['check_in_date'] = pd.to_datetime(hotel_reservation['check_in_date'])
duration = pd.to_timedelta(hotel_reservation['duration'], unit='D')
hotel_reservation['check_out_date'] = hotel_reservation['check_in_date'] + duration
# Randomly assign room_ids
room_ids = ['r{:04d}'.format(i) for i in range(1, 2001)]
random.shuffle(room_ids)
hotel_reservation['room_id'] = [random.choice(room_ids) for _ in range(len(hotel_reservation))]
# Join room and room_type and calculate total_charge based on room_price and duration
room_type = pd.read_csv('room_type.csv', encoding='latin1')
room = pd.read_csv('room.csv', encoding='latin1')
room_type_price = room_type[['type_id', 'room_price']]
room_with_price = room.merge(room_type_price, on='type_id', how='left')
hotel_reservation_with_price = hotel_reservation.merge(room_with_price, on='room_id', how='left')
hotel_reservation_with_price['total_charge'] = hotel_reservation_with_price['room_price'] * hotel_reservation_with_price['duration']
hotel_reservation['total_charge'] = hotel_reservation_with_price['total_charge']
hotel_reservation.dropna(subset=['total_charge'], inplace=True)
# Randomly assign customer_ids
customer_ids = ['c{:04d}'.format(i) for i in range(1, 1001)]
random.shuffle(customer_ids)
hotel_reservation['customer_id'] = [random.choice(customer_ids) for _ in range(len(hotel_reservation))]
# Add reservation_id and organize the dataframe
hotel_reservation.reset_index(drop=True, inplace=True)
hotel_reservation['reservation_id'] = 'hr' + hotel_reservation.index.map(lambda x: str(x+1).zfill(4))
hotel_reservation.insert(0, 'reservation_id', hotel_reservation.pop('reservation_id'))
hotel_reservation = hotel_reservation.loc[:, ['reservation_id','room_id','customer_id','booking_date','check_in_date','check_out_date','estcheck_in_time','estcheck_out_time','special_req','total_charge']]
hotel_reservation.tail()
hotel_reservation.to_csv('hotel_reservation.csv', index=False)
 
# 1.2.5 Hotel_review
# Import packages and the mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_hotel_review.csv', encoding='latin1')
hotel_review = df
# Assign reservation_ids
reservation_ids = ['hr{:04d}'.format(i) for i in range(1, len(hotel_reservation))]
random.shuffle(reservation_ids)
hotel_review['reservation_id'] = [random.choice(reservation_ids) for _ in range(len(hotel_review))]
# Remove duplicates and organize the dataframe
hotel_review.drop_duplicates(subset=['reservation_id'], inplace=True)
hotel_review.reset_index(drop=True, inplace=True)
hotel_review = hotel_review.loc[:, ['reservation_id','date','rating','comments']]
hotel_review.tail()
hotel_review.to_csv('hotel_review.csv', index=False)
 
 
# 1.3 Flight Information
# 1.3.1 Airplane
# Import packages and the dataset
import pandas as pd
df = pd.read_csv('world_aircraft_accident_summary.csv', encoding='latin1')
airplane = df[['Aircraft']].dropna()
# Split aircraft into manufacturer and model
airplane[['manufacturer', 'model']] = airplane['Aircraft'].apply(lambda x: pd.Series(str(x).rstrip('\n').split(' ', 1)))
airplane.drop(['Aircraft'], axis=1, inplace=True)
airplane = airplane.drop_duplicates(subset=['manufacturer', 'model'], keep=False)
# Modify some models and manufacturers
airplane.loc[airplane['manufacturer'] == 'ATR-72-500', 'manufacturer'] = 'ATR'
airplane.loc[airplane['manufacturer'] == 'ATR-72-500', 'model'] = '72-500'
airplane.loc[airplane['model'] == 'AEROSPACE \n146-300', 'manufacturer'] = 'BRITISH AEROSPACE'
airplane.loc[airplane['model'] == 'AEROSPACE \n146-300', 'model'] = '146-300'
airplane.loc[airplane['model'] == 'AEROSPACE \n146-100', 'manufacturer'] = 'BRITISH AEROSPACE'
airplane.loc[airplane['model'] == 'AEROSPACE \n146-100', 'model'] = '146-300'
airplane.loc[airplane['manufacturer']=='ATR', 'model'] = '72-500'
airplane.loc[airplane['model'] == 'BUFFALO', 'manufacturer'] = 'DHC'
airplane.loc[airplane['model'] == 'BUFFALO', 'model'] = 'DHC-5 BUFFALO'
airplane.loc[airplane['model'] == 'TWIN OTTER 400 (VIKING)', 'manufacturer'] = 'DHC'
airplane.loc[airplane['model'] == 'TWIN OTTER 400 (VIKING)', 'model'] = 'DHC-6 TWIN OTTER 400 (VIKING)'
airplane.loc[airplane['manufacturer']=='B.AE.', 'manufacturer'] = 'BRITISH AEROSPACE'
# Assign occupancy to airplanes
airplane['occupancy'] = 0
airplane.loc[airplane['model'].str.contains('707'), 'occupancy'] = 200
airplane.loc[airplane['model'].str.contains('737'), 'occupancy'] = 180
airplane.loc[airplane['model'].str.contains('747'), 'occupancy'] = 500
airplane.loc[airplane['model'].str.contains('767'), 'occupancy'] = 200
airplane.loc[airplane['model'].str.contains('300'), 'occupancy'] = 250
airplane.loc[airplane['model'].str.contains('310'), 'occupancy'] = 210
airplane.loc[airplane['model'].str.contains('320'), 'occupancy'] = 160
airplane.loc[airplane['model'].str.contains('321'), 'occupancy'] = 200
airplane.loc[airplane['manufacturer'].str.contains('BRITISH AEROSPACE'), 'occupancy'] = 100
# Reset the index and assign occupancy to the rest airplanes
airplane.reset_index(drop=True, inplace=True)
import numpy as np
airplane.loc[airplane['occupancy'] == 0, 'occupancy'] = np.random.choice([50, 80, 100, 20], size=airplane['occupancy'].eq(0).sum())
# Add airplane_id and organize the dataframe
airplane.reset_index(drop=True, inplace=True)
airplane['airplane_id'] = 'a' + airplane.index.map(lambda x: str(x+1).zfill(3))
airplane.insert(0, 'airplane_id', airplane.pop('airplane_id'))
airplane.tail()
airplane.to_csv('airplane.csv', index=False)
 
# 1.3.2 Flight_route
# Import packages and the dataset
import pandas as pd
df = pd.read_csv('Jan_2020_ontime.csv', encoding='latin1')
# Convert carrier code to company names
flight_route = df
airline_dict = {'B6': 'JetBlue Airways',
                'DL': 'Delta Air Lines',
                'EV': 'ExpressJet Airlines',
                'F9': 'Frontier Airlines',
                'G4': 'Allegiant Air',
                'HA': 'Hawaiian Airlines',
                'MQ': 'Envoy Air',
                'NK': 'Spirit Airlines',
                'OH': 'PSA Airlines',
                'OO': 'SkyWest Airlines',
                'UA': 'United Airlines',
                'WN': 'Southwest Airlines',
                'YV': 'Mesa Airlines',
                'YX': 'Midwest Airlines',
                '9E': 'Alaska Airlines',
                'AA': 'American Airlines'}
flight_route['company'] = flight_route['OP_CARRIER'].replace(airline_dict)
# Combine carrier code and flight number to get full flight number
flight_route['flight_num'] = flight_route['OP_CARRIER'] + flight_route['OP_CARRIER_FL_NUM'].astype(str)
flight_route = flight_route.rename(columns={'ORIGIN': 'departure_airport', 'DEST': 'landing_airport'})
flight_route = flight_route.drop_duplicates(subset=['flight_num', 'departure_airport', 'landing_airport'])
flight_route.reset_index(drop=True, inplace=True)
# Remove duplicates
flight_route.drop(['OP_CARRIER', 'OP_CARRIER_FL_NUM'], axis=1, inplace=True)
cols = flight_route.columns.tolist()
cols = cols[-1:] + cols[:-1]
flight_route = flight_route[cols]
# Add route_id and organize the dataframe
flight_route.reset_index(drop=True, inplace=True)
flight_route['route_id'] = 'r' + flight_route.index.map(lambda x: str(x+1).zfill(5))
flight_route.insert(0, 'route_id', flight_route.pop('route_id'))
flight_route = flight_route[['route_id','flight_num','departure_airport','landing_airport','company']]
flight_route.tail()
flight_route.to_csv('flight_route.csv', index=False)

# 1.3.3 Flight_reservation
# Import packages and randomly assign class to each reservation
import numpy as np
import pandas as pd
np.random.seed(1234)
class_list = ['economy', 'premium economy', 'business', 'first']
class_col = np.random.choice(class_list, size=4000)
# Randomly assign ticket price based on class
ticket_price = []
for i in class_col:
    if i == 'economy':
        ticket_price.append(round(np.random.uniform(50, 1000), 2))
    elif i == 'premium economy':
        ticket_price.append(round(np.random.uniform(100, 2500), 2))
    elif i == 'business':
        ticket_price.append(round(np.random.uniform(500, 4000), 2))
    else:
        ticket_price.append(round(np.random.uniform(1000, 10000), 2))
# Assign customer_ids to each reservation
flight_reservation = pd.DataFrame({
    'ticket_price': ticket_price
})
customer_ids = ['c{:04d}'.format(i) for i in range(1, 1001)]
random.shuffle(customer_ids)
flight_reservation['customer_id'] = [random.choice(customer_ids) for _ in range(len(flight_reservation))]
# Randomly assign booking dates to each reservation
import random
booking_dates = pd.date_range(start='2018-01-01 00:00:00', end='2023-04-01 23:59:59', freq='s')
random_integers = [random.randint(0, len(booking_dates)-1) for i in range(len(flight_reservation))]
flight_reservation['booking_date'] = [booking_dates[i] for i in random_integers]
# Convert booking_date to datetime
import pandas as pd
from datetime import datetime
flight_reservation['booking_date'] = pd.to_datetime(flight_reservation['booking_date'], unit='s')
flight_reservation['booking_date'] = flight_reservation['booking_date'].apply(lambda x: datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))
# Add reservation_id and organize the dataframe
flight_reservation.reset_index(drop=True, inplace=True)
flight_reservation['reservation_id'] = 'fr' + flight_reservation.index.map(lambda x: str(x+1).zfill(4)).astype(str)
flight_reservation.insert(0, 'reservation_id', flight_reservation.pop('reservation_id'))
flight_reservation = flight_reservation[['reservation_id','customer_id','ticket_price','booking_date']]
flight_reservation.tail()
flight_reservation.to_csv('flight_reservation.csv', index=False)

# 1.3.4 Flight
# Import packages and the dataset
import pandas as pd
df = pd.read_csv('Jan_2020_ontime.csv', encoding='latin1')
# Remove NAs and convert time to twenty-four hour system
df = df.dropna(subset=['DEP_TIME', 'ARR_TIME'])
df['DEP_TIME'] = df['DEP_TIME'].astype(int)
df['ARR_TIME'] = df['ARR_TIME'].astype(int)
df.loc[df['DEP_TIME'] >= 2400, 'DEP_TIME'] -= 2400
df.loc[df['ARR_TIME'] >= 2400, 'ARR_TIME'] -= 2400
# Convert time to datetime
df['DEP_TIME'] = df['DEP_TIME'].astype(str).str.zfill(4)
df['ARR_TIME'] = df['ARR_TIME'].astype(str).str.zfill(4)
df['DEP_TIME'] = df['DEP_TIME'].str.pad(4, fillchar='0') + '00'
df['ARR_TIME'] = df['ARR_TIME'].str.pad(4, fillchar='0') + '00'
df['DEP_TIME'] = pd.to_datetime(df['DEP_TIME'], format='%H%M%S').dt.strftime('%H:%M:%S')
df['ARR_TIME'] = pd.to_datetime(df['ARR_TIME'], format='%H%M%S').dt.strftime('%H:%M:%S')
# Assign simulated date
import numpy as np
df_flight = pd.read_csv('MOCK_DATA_flight.csv', encoding='latin1')
flight = df.rename(columns={'DEP_TIME': 'departure_time', 'ARR_TIME': 'arrival_time'}).loc[:, ['departure_time', 'arrival_time']]
dates = df_flight['departure_date'].values
flight['departure_date'] = np.random.choice(dates, size=len(flight), replace=True)
flight['arrival_date'] = flight['departure_date']
# Calculate duration
flight['departure_date'] = pd.to_datetime(flight['departure_date'])
flight['arrival_date'] = pd.to_datetime(flight['arrival_date'])
flight['departure_time'] = pd.to_datetime(flight['departure_time'])
flight['arrival_time'] = pd.to_datetime(flight['arrival_time'])
flight['duration'] = flight['arrival_time'] - flight['departure_time']
negative_duration_mask = flight['duration'] < pd.Timedelta(0)
flight.loc[negative_duration_mask, 'duration'] += pd.Timedelta('1 day')
flight.loc[negative_duration_mask, 'arrival_date'] += pd.Timedelta('1 day')
flight['duration'] = flight['duration'].dt.total_seconds() / 60
flight['duration'] = flight['duration'].astype(int)
# Convert time and date to standard forms
flight['departure_time'] = flight['departure_time'].dt.strftime('%H:%M:%S')
flight['arrival_time'] = flight['arrival_time'].dt.strftime('%H:%M:%S')
flight['departure_date'] = flight['departure_date'].dt.strftime('%Y-%m-%d')
flight['arrival_date'] = flight['arrival_date'].dt.strftime('%Y-%m-%d')
# Import the dataset and combine carrier code with flight number
df = pd.read_csv('Jan_2020_ontime.csv', encoding='latin1')
flight['flight_num'] = df['OP_CARRIER'] + df['OP_CARRIER_FL_NUM'].astype(str)
flight[['ORIGIN','DEST']] = df[['ORIGIN','DEST']]
# Join flight_route to get route_id
flight_route = pd.read_csv('flight_route.csv', encoding='latin1')
flight = flight.merge(flight_route, 
                      left_on=['flight_num', 'ORIGIN', 'DEST'], 
                      right_on=['flight_num', 'departure_airport', 'landing_airport'], 
                      how='left')
flight['route_id'] = flight['route_id']
# Assign reservation_ids to each flight
reservation_ids = ['fr{:04d}'.format(i) for i in range(1, len(flight_reservation))]
random.shuffle(reservation_ids)
flight['reservation_id'] = [random.choice(reservation_ids) for _ in range(len(flight))]
# Add flight_id and organize the dataframe
flight.reset_index(drop=True, inplace=True)
flight['flight_id'] = 'f' + flight.index.map(lambda x: str(x+1).zfill(6))
flight.insert(0, 'flight_id', flight.pop('flight_id'))
import random
import string
airplane_ids = ['a{:03d}'.format(i) for i in range(1, 104)]
random.shuffle(airplane_ids)
flight['airplane_id'] = [random.choice(airplane_ids) for _ in range(len(flight))]
flight = flight.loc[:, ['flight_id','route_id','airplane_id','reservation_id','departure_date','arrival_date','departure_time','arrival_time','duration']]
flight.tail()
flight.to_csv('flight.csv', index=False)

# 1.3.5 Ticket
# Import packages and randomly assigned class to each ticket
import numpy as np
import pandas as pd
np.random.seed(1234)
class_list = ['economy', 'premium economy', 'business', 'first']
class_col = np.random.choice(class_list, size=5000)
# Generate seat numbers and passport numbers for each ticket
import random
import string
seat_num = [random.choice(string.ascii_uppercase) + str(i) for i in range(1, 66)] * 77
passport = ["".join(random.choices(string.ascii_uppercase + string.digits, k=9)) for _ in range(5000)]
# Create a dataframe with the data generated above
ticket = pd.DataFrame({
    'class': class_col,
    'seat_num': seat_num[:5000],
    'passport': passport
})
# Assign reservation_ids to each ticket
reservation_ids = ['fr{:04d}'.format(i) for i in range(1, 4001)]
random.shuffle(reservation_ids)
ticket['reservation_id'] = [random.choice(reservation_ids) for _ in range(len(ticket))]
ticket.loc[4000:4999, 'reservation_id'] = np.array(['fr' + str(i).zfill(4) for i in range(1, 1001)])
# Add ticket_id and organize the dataframe
ticket.reset_index(drop=True, inplace=True)
ticket['ticket_id'] = 't' + ticket.index.map(lambda x: str(x+1).zfill(4)).astype(str)
ticket.insert(0, 'ticket_id', ticket.pop('ticket_id'))
ticket = ticket[['ticket_id','reservation_id','class','seat_num','passport']]
ticket.tail()
ticket.to_csv('ticket.csv', index=False)
 
 
# 1.4 Car Rental Information
# 1.4.1 Branch
# Import packages and the mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_branch.csv', encoding='latin1')
branch = df
# Randomly assign business hours to each branch
import numpy as np
weekday_start_time = ['08:00:00', '09:00:00', '10:00:00']
random_weekday_start_time = np.random.choice(weekday_start_time, size=len(df))
branch['weekday_start_time'] = random_weekday_start_time
weekday_end_time = ['17:00:00', '18:00:00', '19:00:00']
random_weekday_end_time = np.random.choice(weekday_end_time, size=len(df))
branch['weekday_end_time'] = random_weekday_end_time
sat_start_time = ['10:00:00', '12:00:00', '13:00:00']
random_sat_start_time = np.random.choice(sat_start_time, size=len(df))
branch['sat_start_time'] = random_sat_start_time
sat_end_time = ['16:00:00', '17:00:00', '18:00:00']
random_sat_end_time = np.random.choice(sat_end_time, size=len(df))
branch['sat_end_time'] = random_sat_end_time
sun_start_time = ['12:00:00', '13:00:00', '14:00:00']
random_sun_start_time = np.random.choice(sun_start_time, size=len(df))
branch['sun_start_time'] = random_sun_start_time
sun_end_time = ['16:00:00', '17:00:00', '18:00:00']
random_sun_end_time = np.random.choice(sun_end_time, size=len(df))
branch['sun_end_time'] = random_sun_end_time
# Add branch_id and organize the dataframe
branch = branch.rename(columns={'carnumb': 'num_cars'})
branch.reset_index(drop=True, inplace=True)
branch['branch_id'] = 'b' + branch.index.map(lambda x: str(x+1).zfill(4))
branch.insert(0, 'branch_id', branch.pop('branch_id'))
branch = branch[['branch_id','zipcode','state','city','address','weekday_start_time','weekday_end_time','sat_start_time','sat_end_time','sun_start_time','sun_end_time','num_cars']]
branch.tail()
branch.to_csv('branch.csv', index=False)
 
# 1.4.2 Car
# Import packages and the mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_car.csv', encoding='latin1')
car = df.rename(columns={'branch_id': 'rent_price'})
# Randomly assign condition and fuel type to each car
import numpy as np
statuses = ['excellent', 'good', 'fair']
car['status'] = np.random.choice(statuses, size=len(car))
fuel_types = ['gasoline', 'diesel', 'electricity']
car['fuel_type'] = np.random.choice(fuel_types, size=len(car))
# Assign branch_ids to each car
branch_ids = ['b{:04d}'.format(i) for i in range(1, 1001)]
random.shuffle(branch_ids)
car['branch_id'] = [random.choice(branch_ids) for _ in range(len(car))]
# Add car_id and organize the dataframe
car = car.rename(columns={'status': 'condition'})
car.reset_index(drop=True, inplace=True)
car['car_id'] = 'c' + car.index.map(lambda x: str(x+1).zfill(4))
car.insert(0, 'car_id', car.pop('car_id'))
car = car[['car_id','brand','model','condition','fuel_type','size','occupancy','rent_price']]
car.tail()
car.to_csv('car.csv', index=False)
 
# 1.4.3 Car_reservation
# Import packages and the mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_car_reservation.csv', encoding='latin1')
car_reservation = df.rename(columns={'duration': 'duration'})
# Simulate dropoff time based on pickup time and duration
car_reservation['dropoff_time'] = pd.to_datetime(car_reservation['pickup_time']) + pd.to_timedelta(car_reservation['duration'], unit='d')
# Assign pickup branches and dropoff branches to each reservation
branch_ids = ['b{:04d}'.format(i) for i in range(1, 1001)]
random.shuffle(branch_ids)
car_reservation['pickup_branch'] = [random.choice(branch_ids) for _ in range(len(car_reservation))]
car_reservation['dropoff_branch'] = [random.choice(branch_ids) for _ in range(len(car_reservation))]
# Assign customer_ids and car_ids to each reservation
customer_ids = ['c{:04d}'.format(i) for i in range(1, 1001)]
random.shuffle(customer_ids)
car_reservation['customer_id'] = [random.choice(customer_ids) for _ in range(len(car_reservation))]
car_ids = ['c{:04d}'.format(i) for i in range(1, 2001)]
random.shuffle(car_ids)
car_reservation['car_id'] = [random.choice(customer_ids) for _ in range(len(car_reservation))]
# Randomly assign preferred insurances
import numpy as np
preferred_insurances = ['State Farm','GEICO','Progressive','Allstate','USAA']
car_reservation['preferred_insurance'] = np.random.choice(preferred_insurances, size=len(car_reservation))
# Simulate booking date based on pickup time
car_reservation['pickup_time'] = pd.to_datetime(car_reservation['pickup_time'])
car_reservation['booking_date'] = car_reservation['pickup_time'] - pd.to_timedelta('2 days')
# Join car to calculate total charge based on duration and rent price
car = pd.read_csv('car.csv', encoding='latin1')
merged_df = car_reservation.merge(car[['car_id', 'rent_price']], on='car_id', how='left')
merged_df['rate_per_day'] = merged_df.apply(lambda row: row['rent_price'], axis=1)
car_reservation['rate_per_day'] = merged_df['rate_per_day']
car_reservation['pickup_time'] = pd.to_datetime(car_reservation['pickup_time'])
car_reservation['dropoff_time'] = pd.to_datetime(car_reservation['dropoff_time'])
car_reservation['duration'] = (car_reservation['dropoff_time'] - car_reservation['pickup_time']).dt.days
car_reservation['total_charge'] = car_reservation['duration'] * car_reservation['rate_per_day']
# Add reservation_id and organize the dataframe
car_reservation = car_reservation.rename(columns={'pickup_branch': 'pickup_branch_id'})
car_reservation = car_reservation.rename(columns={'dropoff_branch': 'dropoff_branch_id'})
car_reservation.reset_index(drop=True, inplace=True)
car_reservation['reservation_id'] = 'cr' + car_reservation.index.map(lambda x: str(x+1).zfill(4))
car_reservation.insert(0, 'reservation_id', car_reservation.pop('reservation_id'))
car_reservation = car_reservation[['reservation_id','car_id','customer_id','pickup_branch_id','dropoff_branch_id','pickup_time','dropoff_time','preferred_insurance','booking_date','total_charge']]
car_reservation.tail()
car_reservation.to_csv('car_reservation.csv', index=False)
 
# 1.4.4 Car_review
# Import packages and the mock data
import pandas as pd
df = pd.read_csv('MOCK_DATA_car_review.csv', encoding='latin1')
car_review = df
# Assign reservation_ids to each review
reservation_ids = ['cr{:04d}'.format(i) for i in range(1, len(car_reservation))]
random.shuffle(reservation_ids)
car_review['reservation_id'] = [random.choice(reservation_ids) for _ in range(len(car_review))]
# Remove duplicates and organize the dataframe
car_review.drop_duplicates(subset=['reservation_id'], inplace=True)
car_review.reset_index(drop=True, inplace=True)
car_review = car_review.loc[:, ['reservation_id','renter_rating','car_rating', 'comments']]
car_review.tail()
car_review.to_csv('car_review.csv', index=False)
 
 
# 1.5 Booking and Payment
# 1.5.1 Booking
# Import packages and the datasets
import pandas as pd
hotel_reservation = pd.read_csv('hotel_reservation.csv')
flight_reservation = pd.read_csv('flight_reservation.csv')
car_reservation = pd.read_csv('car_reservation.csv')
# Join hotel_r_ids with car_r_ids by customer_ids
booking = pd.merge(hotel_reservation[['customer_id', 'reservation_id']],
                   car_reservation[['customer_id', 'reservation_id']],
                   on='customer_id', suffixes=('_hotel', '_car'))
booking = booking.groupby('reservation_id_hotel').first()[['customer_id', 'reservation_id_car']].reset_index()
booking.columns = ['hotel_r_id', 'customer_id', 'car_r_id']
# Add columns for leaving_flight_r_ids and returning_flight_r_ids
flight_reservation = flight_reservation.groupby('customer_id').head(2)
grouped_reservation = flight_reservation.groupby('customer_id')['reservation_id'].apply(list)
booking['leaving_flight_r_id'] = ''
booking['returning_flight_r_id'] = ''
# Join flight_r_ids by customer_ids
for index, row in booking.iterrows():
    customer_id = row['customer_id']
    if customer_id in grouped_reservation:
        reservation_ids = grouped_reservation[customer_id]
        if len(reservation_ids) == 1:
            booking.loc[index, 'leaving_flight_r_id'] = reservation_ids[0]
        elif len(reservation_ids) == 2:
            booking.loc[index, 'leaving_flight_r_id'] = reservation_ids[0]
            booking.loc[index, 'returning_flight_r_id'] = reservation_ids[1]
# Calculate total charges
hotel_reservation = pd.read_csv('hotel_reservation.csv')
flight_reservation = pd.read_csv('flight_reservation.csv')
car_reservation = pd.read_csv('car_reservation.csv')
hotel_charge = booking.merge(hotel_reservation, how='left', left_on='hotel_r_id', right_on='reservation_id')
hotel_charge = hotel_charge.rename(columns={'total_charge': 'hotel_charge'})[['hotel_charge']]
car_charge = booking.merge(car_reservation, how='left', left_on='car_r_id', right_on='reservation_id')
car_charge = car_charge.rename(columns={'total_charge': 'car_charge'})[['car_charge']]
leaving_charge = booking.merge(flight_reservation, how='left', left_on='leaving_flight_r_id', right_on='reservation_id')
leaving_charge = leaving_charge.rename(columns={'ticket_price': 'leaving_charge'})[['leaving_charge']]
returning_charge = booking.merge(flight_reservation, how='left', left_on='returning_flight_r_id', right_on='reservation_id')
returning_charge = returning_charge.rename(columns={'ticket_price': 'returning_charge'})[['returning_charge']]
booking_with_charges = pd.concat([booking, hotel_charge, car_charge, leaving_charge, returning_charge], axis=1)
booking['amount'] = booking_with_charges[['hotel_charge', 'car_charge', 'leaving_charge', 'returning_charge']].sum(axis=1)
# Add booking_id and organize the dataframe
booking_payment = booking
booking.reset_index(drop=True, inplace=True)
booking['booking_id'] = 'b' + booking.index.map(lambda x: str(x+1).zfill(4))
booking.insert(0, 'booking_id', booking.pop('booking_id'))
booking = booking[['booking_id','leaving_flight_r_id','returning_flight_r_id','hotel_r_id','car_r_id','amount']]
booking.tail()
booking.to_csv('booking.csv', index=False)
booking_payment.to_csv('booking_payment.csv', index=False)

# 1.5.2 Payment
# Import packages and the datasets
import pandas as pd
credit_card = pd.read_csv('credit_card.csv')
booking_payment = pd.read_csv('booking_payment.csv')
credit_card = credit_card.groupby('customer_id').first().reset_index()
# Join booking_payment with credit_card by customer_ids for card_nums
booking_payment = pd.merge(booking_payment, credit_card[['customer_id', 'card_num']], on='customer_id')
payment = booking_payment
# Randomly assign dates to each payment
import random
dates = pd.date_range(start='2018-01-01 00:00:00', end='2023-04-01 23:59:59', freq='s')
random_integers = [random.randint(0, len(dates)-1) for i in range(len(payment))]
payment['date'] = [dates[i] for i in random_integers]
payment = payment[['booking_id', 'card_num', 'date']]
payment.tail()
payment.to_csv('payment.csv', index=False)
 
 
# 2 Data Insertion
# 2.1 Connect With PgAdmin
import pandas as pd
from sqlalchemy import create_engine
conn_url = 'postgresql://postgres:1412@localhost/Test'
engine = create_engine(conn_url)
connection = engine.connect()
stmt = """
    CREATE TABLE customer (
        customer_id VARCHAR(10) NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL, 
        phone_num VARCHAR(15) NOT NULL,
        email VARCHAR(254) NOT NULL,
        street_address_1 VARCHAR(35) NOT NULL, 
        street_address_2 VARCHAR(35),
        city VARCHAR(25) NOT NULL,
        state VARCHAR(20), 
        zipcode VARCHAR(10) NOT NULL, 
        PRIMARY KEY (customer_id)
    );
    CREATE TABLE credit_card (
        card_num VARCHAR(20) NOT NULL,
        card_type VARCHAR(30) NOT NULL,
        expiration_month INT NOT NULL,
        expiration_year INT NOT NULL,
        customer_id VARCHAR(10) NOT NULL,
        PRIMARY KEY (card_num),
        FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
    );
    CREATE TABLE flight_route (
        route_id VARCHAR(10) NOT NULL,
        flight_num VARCHAR(10) NOT NULL,
        departure_airport VARCHAR(5) NOT NULL,
        landing_airport VARCHAR(5) NOT NULL,
        company VARCHAR(20) NOT NULL,
        PRIMARY KEY (route_id)
    );
    CREATE TABLE airplane (
        airplane_id VARCHAR(10) NOT NULL,
        manufacturer VARCHAR(20) NOT NULL,
        model VARCHAR(30) NOT NULL,
        occupancy INT NOT NULL,
        PRIMARY KEY (airplane_id)
    );
    CREATE TABLE flight (
        flight_id VARCHAR(10) NOT NULL,
        route_id VARCHAR(10) NOT NULL,
        airplane_id VARCHAR(10) NOT NULL,
        departure_date DATE NOT NULL,
        arrival_date DATE NOT NULL,
        departure_time TIME NOT NULL,
        arrival_time TIME NOT NULL,
        duration INT NOT NULL,
        PRIMARY KEY (flight_id),
        FOREIGN KEY (route_id) REFERENCES flight_route (route_id),
        FOREIGN KEY (airplane_id) REFERENCES airplane (airplane_id)
    );
        CREATE TABLE flight_reservation (
        reservation_id VARCHAR(10) NOT NULL,
        customer_id VARCHAR(10) NOT NULL,
        flight_id VARCHAR(10) NOT NULL,
        ticket_price NUMERIC(10,2) NOT NULL,
        booking_date DATE NOT NULL,
        PRIMARY KEY (reservation_id),
        FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
        FOREIGN KEY (flight_id) REFERENCES flight (flight_id)
    );
    CREATE TABLE ticket (
        ticket_id VARCHAR(10) NOT NULL,
        reservation_id VARCHAR(10) NOT NULL,
        class VARCHAR(20) NOT NULL,
        seat_num VARCHAR(5),
        passport VARCHAR(15) NOT NULL,
        PRIMARY KEY (ticket_id),
        FOREIGN KEY (reservation_id) REFERENCES flight_reservation (reservation_id)
    );
    CREATE TABLE hotel (
        hotel_id VARCHAR(10) NOT NULL,
        name VARCHAR(100) NOT NULL,
        stars INTEGER,
        street VARCHAR(35) NOT NULL,
        city VARCHAR(35) NOT NULL, 
        state VARCHAR(20), 
        zipcode VARCHAR(10) NOT NULL,
        num_rooms INT NOT NULL,
        phone_num VARCHAR(15) NOT NULL,
        PRIMARY KEY (hotel_id)
    );
    CREATE TABLE room_type (
        type_id VARCHAR(10) NOT NULL,
        room_price NUMERIC(10,2) NOT NULL,
        room_desc TEXT,
        footprint INT,
        bed_size VARCHAR(20) NOT NULL,
        PRIMARY KEY (type_id)
    );
    CREATE TABLE room (
        room_id VARCHAR(10) NOT NULL,
        hotel_id VARCHAR(10) NOT NULL,
        type_id VARCHAR(10) NOT NULL,
        room_num VARCHAR(5) NOT NULL,
        occupancy INT NOT NULL,
        note TEXT,
        PRIMARY KEY (room_id),
        FOREIGN KEY (hotel_id) REFERENCES hotel (hotel_id),
        FOREIGN KEY (type_id) REFERENCES room_type (type_id)
    );
        CREATE TABLE hotel_reservation (
        reservation_id VARCHAR(10) NOT NULL,
        room_id VARCHAR(10) NOT NULL,
        customer_id VARCHAR(10) NOT NULL,
        booking_date DATE NOT NULL,
        check_in_date DATE NOT NULL,
        check_out_date DATE NOT NULL,
        estcheck_in_time TIME NOT NULL,
        estcheck_out_time TIME NOT NULL,
        special_req TEXT,
        total_charge NUMERIC(10,2) NOT NULL,
        PRIMARY KEY (reservation_id),
        FOREIGN KEY (room_id) REFERENCES room (room_id),
        FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
    );
    CREATE TABLE hotel_review (
        reservation_id VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
        comments TEXT,
        PRIMARY KEY (reservation_id),
        FOREIGN KEY (reservation_id) REFERENCES hotel_reservation (reservation_id)
    );
    CREATE TABLE branch (
        branch_id VARCHAR(10) NOT NULL,
        zipcode VARCHAR(40) NOT NULL,
        state VARCHAR(40), 
        city VARCHAR(40) NOT NULL, 
        address VARCHAR(40) NOT NULL,
        weekday_start_time TIME NOT NULL,
        weekday_end_time TIME NOT NULL,
        sat_start_time TIME NOT NULL,
        sat_end_time TIME NOT NULL,
        sun_start_time TIME NOT NULL,
        sun_end_time TIME NOT NULL,
        num_cars INT NOT NULL,
        PRIMARY KEY (branch_id)
    );
    CREATE TABLE car (
        car_id VARCHAR(10) NOT NULL,
        brand VARCHAR(20) NOT NULL,
        model VARCHAR(40) NOT NULL,
        condition VARCHAR(10) NOT NULL,
        fuel_type VARCHAR(15) NOT NULL,
        size NUMERIC(4,2) NOT NULL,
        occupancy INT NOT NULL,
        rent_price NUMERIC(10,2) NOT NULL,
        PRIMARY KEY (car_id)
    );
        CREATE TABLE car_reservation (
        reservation_id VARCHAR(10) NOT NULL,
        car_id VARCHAR(10) NOT NULL,
        customer_id VARCHAR(10) NOT NULL,
        pickup_branch_id VARCHAR(10) NOT NULL,
        dropoff_branch_id VARCHAR(10) NOT NULL,
        pickup_time TIMESTAMP NOT NULL,
        dropoff_time TIMESTAMP NOT NULL,
        preferred_insurance VARCHAR(20) NOT NULL,
        booking_date TIMESTAMP NOT NULL,
        total_charge NUMERIC(10,2),
        PRIMARY KEY (reservation_id),
        FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
        FOREIGN KEY (car_id) REFERENCES car (car_id),
        FOREIGN KEY (pickup_branch_id) REFERENCES branch (branch_id),
        FOREIGN KEY (dropoff_branch_id) REFERENCES branch (branch_id)
    );
    CREATE TABLE car_review (
        reservation_id VARCHAR(10) NOT NULL,
        renter_rating INTEGER NOT NULL CHECK (renter_rating >= 1 AND renter_rating <= 5),
        car_rating INTEGER NOT NULL CHECK (car_rating >= 1 AND car_rating <= 5),
        comments TEXT,
        PRIMARY KEY (reservation_id),
        FOREIGN KEY (reservation_id) REFERENCES car_reservation (reservation_id)
    );
    CREATE TABLE booking (
        booking_id VARCHAR(10) NOT NULL,
        leaving_flight_r_id VARCHAR(10),
        returning_flight_r_id VARCHAR(10),
        hotel_r_id VARCHAR(10) NOT NULL,
        car_r_id VARCHAR(10) NOT NULL,
        amount NUMERIC(10,2) NOT NULL,
        PRIMARY KEY (booking_id),
        FOREIGN KEY (leaving_flight_r_id) REFERENCES flight_reservation (reservation_id),
        FOREIGN KEY (returning_flight_r_id) REFERENCES flight_reservation (reservation_id),
        FOREIGN KEY (hotel_r_id) REFERENCES hotel_reservation (reservation_id),
        FOREIGN KEY (car_r_id) REFERENCES car_reservation (reservation_id)
    );
    CREATE TABLE payment (
        booking_id VARCHAR(10) NOT NULL,
        card_num VARCHAR(20) NOT NULL,
        date TIMESTAMP NOT NULL,
        PRIMARY KEY (booking_id),
        FOREIGN KEY (booking_id) REFERENCES booking (booking_id),
        FOREIGN KEY (card_num) REFERENCES credit_card (card_num)
    );
"""
connection.execute(stmt)


# 2.3 Read The Dataframes Created Above
customer = pd.read_csv('customer.csv')
credit_card = pd.read_csv('credit_card.csv')
flight_route = pd.read_csv('flight_route.csv')
airplane = pd.read_csv('airplane.csv')
flight = pd.read_csv('flight.csv')
flight_reservation = pd.read_csv('flight_reservation.csv')
ticket = pd.read_csv('ticket.csv')
hotel = pd.read_csv('hotel.csv')
room_type = pd.read_csv('room_type.csv')
room = pd.read_csv('room.csv')
hotel_reservation = pd.read_csv('hotel_reservation.csv')
hotel_review = pd.read_csv('hotel_review.csv')
branch = pd.read_csv('branch.csv')
car = pd.read_csv('car.csv')
car_reservation = pd.read_csv('car_reservation.csv')
car_review = pd.read_csv('car_review.csv')
booking = pd.read_csv('booking.csv')
payment = pd.read_csv('payment.csv')


# 2.4 Insert The Dataframes Into PgAdmin
customer.to_sql(name='customer', con=engine, if_exists='append', index=False)
credit_card.to_sql(name='credit_card', con=engine, if_exists='append', index=False)
flight_route.to_sql(name='flight_route', con=engine, if_exists='append', index=False)
airplane.to_sql(name='airplane', con=engine, if_exists='append', index=False)
flight.to_sql(name='flight', con=engine, if_exists='append', index=False)
flight_reservation.to_sql(name='flight_reservation', con=engine, if_exists='append', index=False)
ticket.to_sql(name='ticket', con=engine, if_exists='append', index=False)
hotel.to_sql(name='hotel', con=engine, if_exists='append', index=False)
room_type.to_sql(name='room_type', con=engine, if_exists='append', index=False)
room.to_sql(name='room', con=engine, if_exists='append', index=False)
hotel_reservation.to_sql(name='hotel_reservation', con=engine, if_exists='append', index=False)
hotel_review.to_sql(name='hotel_review', con=engine, if_exists='append', index=False)
branch.to_sql(name='branch', con=engine, if_exists='append', index=False)
car.to_sql(name='car', con=engine, if_exists='append', index=False)
car_reservation.to_sql(name='car_reservation', con=engine, if_exists='append', index=False)
car_review.to_sql(name='car_review', con=engine, if_exists='append', index=False)
booking.to_sql(name='booking', con=engine, if_exists='append', index=False)
payment.to_sql(name='payment', con=engine, if_exists='append', index=False)

