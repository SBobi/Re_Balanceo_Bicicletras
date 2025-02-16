

# --------------------------------------
# Librerias
# --------------------------------------

import os
import requests
import pandas as pd
import xml.etree.ElementTree as ET

# --------------------------------------
# Main
# --------------------------------------

def main():

    print("\n----------------------------")
    print("Getting and Processing Data")
    print("----------------------------\n")

    nr_stations = 30
    station_location_pre = retrieve_station_location_data()
    classic_trips = retrieve_trip_data(station_location_pre, nr_stations)
    drop_unneeded_station_info(station_location_pre, classic_trips)

# --------------------------------------
# Estaciones
# --------------------------------------

def retrieve_station_location_data():

    url = 'https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml'
    response = requests.get(url)
    xml_data = response.content

    root = ET.fromstring(xml_data)

    stations = []
    for station in root.findall('station'):
        station_data = {
            'id': station.find('id').text,
            'name': station.find('name').text,
            'terminalName': station.find('terminalName').text,
            'lat': station.find('lat').text,
            'long': station.find('long').text,
            'installed': station.find('installed').text,
            'locked': station.find('locked').text,
            'installDate': station.find('installDate').text,
            'removalDate': station.find('removalDate').text,
            'temporary': station.find('temporary').text,
            'nbBikes': station.find('nbBikes').text,
            'nbStandardBikes': station.find('nbStandardBikes').text,
            'nbEBikes': station.find('nbEBikes').text,
            'nbEmptyDocks': station.find('nbEmptyDocks').text,
            'nbDocks': station.find('nbDocks').text
        }
        stations.append(station_data)

    # TODO
    print(f"1 -- El sistema tiene {len(stations)} estaciones de bicicletas.\n")

    station_location = pd.DataFrame(stations).sort_values("name")
    station_location = station_location[station_location['lat'] != '0.0']
    station_location = station_location[station_location['long'] != '0.0']

    numeric_columns = ['lat', 'long']
    for col in numeric_columns:
        station_location[col] = station_location[col].astype(float)

    integer_columns = ['terminalName', 'nbBikes', 'nbStandardBikes', 'nbEBikes', 'nbEmptyDocks', 'nbDocks']
    for col in integer_columns:
        station_location[col] = station_location[col].astype(int)

    station_location.drop(columns=['installed', 'locked', 'installDate', 'removalDate', 'temporary'], inplace=True)
    
    return station_location

def drop_unneeded_station_info(station_locations, stations_in_saved_trips_data):
    
    stations1 = set(list(stations_in_saved_trips_data[["Start station number", "Start station"]].drop_duplicates().itertuples(index=False, name=None)))
    stations2 = set(list(station_locations[["terminalName", "name"]].itertuples(index=False, name=None)))
    
    keys_df = pd.DataFrame(index=station_locations.index)
    keys_df['Keys'] = list(zip(station_locations["terminalName"], station_locations["name"]))
    station_locations = station_locations[~keys_df['Keys'].isin(stations2 - stations1)]

    # TODO
    print(f"3 -- Se limpia el archivo de estaciones al retirar las {len(stations2 - stations1)} estaciones sin viajes.\n")
    os.makedirs('./process_data', exist_ok=True)
    station_locations.to_csv("./process_data/stations_all_info.csv", sep=',', index=False)

# --------------------------------------
# Viajes
# --------------------------------------

def retrieve_trip_data(station_locations, nr_stations):
    
    filename = "%dJourneyDataExtract.csv"
    folder = "./data/raw_data/"

    data = []
    for i in range(391, 395):
        new_data = pd.read_csv(os.path.join(folder, filename%i), parse_dates=["Start date", "End date"]).dropna() 
        data.append(new_data)
    data = pd.concat(data)

    # TODO
    print(f"2 -- El sistema tiene {data.shape[0]} viajes.")

    # Drop times from datetimes to obtain dates
    data["Start datetime"] = data["Start date"]
    data["End datetime"] = data["End date"]
    data["Start date"] = data["Start date"].dt.date
    data["End date"] = data["End date"].dt.date

    # Filter the dataset
    classic_trips = filter_trips_data(data, station_locations, nr_stations)
    classic_trips.to_csv("./process_data/trips_all_info.csv", sep=',', index=False)

    return classic_trips

# Filter the data on certain criteria
def filter_trips_data(data, station_locations, nr_stations):

    # Drop rows where trips end on a different day than when they started
    data = data[data['Start date'] == data['End date']]

    # Drop rows where trips either start or end at stations we do not know the capacity of
    data = drop_trips_involving_incomplete_stations(data, station_locations)

    # Keep only the first nr_stations stations
    all_stations = get_all_stations_info(data)
    station_names = sorted(list(set(list(all_stations.itertuples(index=False, name=None)))))
    stations_to_drop = station_names[nr_stations:]

    # TODO
    print(f"Se retiran {len(stations_to_drop)} estaciones por decisión del usuario.")

    data = drop_all_trips_with_stations(data, stations_to_drop)
    classic_trips = data[data['Bike model'] == 'CLASSIC']

    # TODO
    print(f"El sistema tiene {classic_trips.shape[0]} viajes clásicos.\n")

    return classic_trips


# Drop trips that involve stations for which we do not have location coordinate information or capacity information
def drop_trips_involving_incomplete_stations(data, station_locations):

    station_nr_to_name_mapping = get_all_stations_info(data)
    stations_in_trips = set(list(station_nr_to_name_mapping[["Station number", "Station"]].drop_duplicates().itertuples(index=False, name=None)))
    stations_with_locations = set(list(station_locations[["terminalName", "name"]].itertuples(index=False, name=None)))
    station_names_to_drop = stations_in_trips - stations_with_locations

    # TODO
    print(f"Se retiran viajes que involucran {len(station_names_to_drop)-1} estaciones con información incompleta.")

    return drop_all_trips_with_stations(data, station_names_to_drop)


# Drop trips that either Start or End at a station in the list of stations to drop
def drop_all_trips_with_stations(data, station_names_to_drop):

    def drop_mode(mode):
        keys_df = pd.DataFrame(index=data.index)
        keys_df['Keys'] = list(zip(data["%s station number"%mode], data["%s station"%mode]))
        filtered_data = data[~keys_df['Keys'].isin(station_names_to_drop)]
        return filtered_data

    data = drop_mode("Start")
    data = drop_mode("End")
    return data


# Get the name and station number of all unique stations present in the data
def get_all_stations_info(raw_data):

    start_station_nr_to_name_mapping = raw_data.set_index('Start station number')['Start station'].to_dict()
    end_station_nr_to_name_mapping = raw_data.set_index('End station number')['End station'].to_dict()
    start_station_nr_to_name_mapping.update(end_station_nr_to_name_mapping)
    station_nr_to_name_mapping = pd.DataFrame(list(start_station_nr_to_name_mapping.items()), columns=['Station number', 'Station'])
    return station_nr_to_name_mapping

# --------------------------------------
# Run
# --------------------------------------

if __name__ == '__main__':
    main()

