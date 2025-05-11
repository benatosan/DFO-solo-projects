"""IWLS_Public_API 

DFO-MPO/CHS-SHC
Institute of Ocean Sciences / Institut des sciences de la mer

This file is NOT FUNCTIONAL due to API endpoints being hidden for security reasons.


This file can be imported as a module and contains the following
functions:

    * get_metadata - returns a dataframe containing metadata from iwls given the station_id
    * get_all_metadata - returns a dataframe containing metadata from iwls and from key value pairs given the station_id    
    * get_station_id - returns the station id given the station_code
    * get_stations_df - returns a dataframe of stations given the region
    * get_stations_list_private - gets all stations using the private api    
    * get_stations_list - returns a list of stations given the region 
    * get_station_metadata - returns a json response of metadata for a station
    * get_height_code - returns height code from height id
    * get_height_id - returns height id from height code
    * get_height_types - returns dataframe of height types
    * find_data_gaps - returns dataframe of data gaps from start to end date for a station
    * get_metadata_class - convert dictionary of metadata to a class object
    * get_metadata_rest - get metadata for a station using the public REST service
    * get_station_timeseries - returns station data from production api 
    * get_crc_hex_string - returns Modbus 16 CRC calculation
    * create_messages_for_iwls - Create list of messages to send to IWLS
    * send_messages_to_IWLS - Send list of messages to IWLS
    * get_timeseries_id - gets the timeseries id for a station id with the private api
    STILL HAVE TO ADD IN ALL THE NEW METHODS !!!

"""

# Standard imports 
import pandas as pd
from datetime import  datetime,timedelta
import requests
from adal import AuthenticationContext
# import libscrc
import xml.etree.ElementTree as ET
import socket
import telnetlib
import logging
import json

##--------------------------------------------------------------------------------
"""
API Endpoints
"""
station_url = "hidden"
heights_url = "hidden"
private_base_url = "hidden"
private_station_url = "hidden"
private_region_url = "hidden"
private_additional_configurations = "hidden"
private_chs_regions = "hidden"

s = requests.Session()
s.encoding = 'utf-8'
s.chunked = True
s.timeout = 10
s.headers['Accept'] = 'application/json,*/*'
##--------------------------------------------------------------------------------

# Placeholders for caching station metadata
list_stations = []
list_regions = []

# ---------------------------------------------------------------------------------------
# Added by Mike Sheward - 2024-10-18
# Add code to merge IWLS metadata with key value pairs 
# ---------------------------------------------------------------------------------------
class StationMetaData:
    def __init__(self):
        self.active = ''
        self.basic_file_name = ''
        self.chsRegionCode = ''
        self.chsRegionId = ''
        self.classCode = ''
        self.code = ''
        self.commissioningDate = ''
        self.contact = ''
        self.data_logger=''
        self.dateOfCommissioning = ''
        self.datums = []
        self.dcp=''
        self.description = ''
        self.disseminated = '' 
        self.established = ''
        self.establishedYear = ''
        self.expectedProductivityPerHour = ''
        self.externalId = ''
        self.externalOrganizationId = ''
        self.goes_enabled = ''
        self.goes_iwls_sensors=''
        self.goes_message_type=''
        self.goes_minutes_back = ''
        self.goes_sutron_sensors=''
        self.goes_units=''
        self.heights = []
        self.ibmCode = ''
        self.id = ''
        self.ip_address = ''
        self.ip_enabled = ''
        self.isTidal = ''
        self.isTideTableReferencePort = ''
        self.iwls_environment = ''
        self.iwls_sensors = ''
        self.lastMaintenanceDate = ''   
        self.latitude = ''
        self.log_name=''
        self.longitude = ''
        self.modem_enabled = ''
        self.name = ''
        self.officialName = ''
        self.offset = ''
        self.operating = ''
        self.organizationId = ''
        self.owner = ''
        self.phone_number = ''
        self.port = ''
        self.previousCode = ''
        self.provinceCode = ''
        self.provinceId = ''
        self.referenceDatums = []
        self.referencePort = ''
        self.region_header = ''             
        self.script_variable_iwls = ''
        self.script_variable_sensor = ''
        self.stationClassCode = ''
        self.status = ''
        self.sutron_sensors = ''
        self.tidal = ''
        self.tideTableId = ''
        self.timeSeries = []
        self.timeZoneCode = ''
        self.type = ''
        self.user_login = ''
        self.user_pass = ''
        self.version = ''
        self.voltageCritical = ''
        self.voltageWarning = ''      

        # NOTE: Special Cases - backwards compatibility 
        self.xconnectlogfile = ''
        self.ip = ''

    def toJSON(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__, 
            sort_keys=True)   

    # NOTE: indent parameter causes newlines after each element
    # def toJSON(self):
    #     return json.dumps(
    #         self,
    #         default=lambda o: o.__dict__, 
    #         sort_keys=True,
    #         indent=4)         



def get_station(station_code):
    """
    Get metadata for station code 
    return:
        Pandas dataframe containing metadata for station
    """
    params = {'code':station_code}
    r = s.get(url=station_url, params=params)
    data_json = r.json()
    df = pd.DataFrame.from_dict(data_json)
    return df
##--------------------------------------------------------------------------------
def get_station_status(station_id):
    """
    Get status of station from id  
    """  
    url = private_station_url + station_id +'/status/'
    r = s.get(url=url)
    data_json = r.json()
    return data_json
##--------------------------------------------------------------------------------

def get_station_id(station_code):
    """
    Get internal station id given the station code  
    return:
        Station ID i.e. code='07120' returns id=5cebf1df3d0f4a073c4bbd1e
    """    
    # Get the station object, and get the station id 
    station = get_station(station_code)
    if station.empty:
        return None
    else:
        station_id = station['id'].values[0]
        return station_id

##--------------------------------------------------------------------------------

def get_stations_df(chs_region_code='PAC'):
    """
    Get a dataframe of all stations for a region 
    return:
        Pandas dataframe containing stations 
        Dataframe contains:
        ['id', 'code', 'officialName', 'operating', 'latitude', 'longitude',
       'type', 'timeSeries']        
    """
    params = {'chs-region-code':chs_region_code}
    r = s.get(url=station_url, params=params)
    data_json = r.json()
    df = pd.DataFrame.from_dict(data_json)
    return df

##--------------------------------------------------------------------------------

def get_stations_list_private():
    """
    Get all stations 
    return:
        JSON object containing all stations 
    """
    r = s.get(url=private_station_url)
    data_json = r.json()
    return data_json

##--------------------------------------------------------------------------------

def get_stations_list(chs_region_code='PAC',time_series_code="wlo"):
    """
    Get a list of all stations for a region 
    return:
        list containing stations - type: PERMANENT or TEMPORARY
    """
    params = {'chs-region-code':chs_region_code,'time-series-code':time_series_code}
    r = s.get(url=station_url, params=params)
    data_json = r.json()
    list_of_classes = []
    
    for station in data_json:
        list_of_classes +=[get_metadata_class(station)]
    
    return list_of_classes

##--------------------------------------------------------------------------------

def get_station_metadata(station_code) ->dict:
    """
    Return a json response from the IWLS Private API containing metadata for a single station

    param:
        station_code = five digit station identifier (string)
    """
    
    station_id = get_station_id_private(station_code)
    if station_id is None:
        return None

    # Need to use the private url here in order to get metadata for non-disseminated stations 
    # Private does not get all properties - like officialName is actually name in private metadata call 
    # so need to be aware of this     
    url = private_station_url + station_id + '/metadata'
    params = {}
    r = s.get(url=url, params=params)
    metadata_json = r.json()   

    # Check for 'status': 'NOT_FOUND'
    # BUT status is not always in the metadata !!!
    if 'status' in metadata_json:
        if metadata_json['status'] == 'NOT_FOUND':
            return None
    
    return metadata_json

##--------------------------------------------------------------------------------

def get_height_code(heightTypeId ):    
    """
    Return  Code from height type id
    params:
        heightTypeId = unique IWLS database height type id (string)
    return:
        Height Code
    """
    height_types = get_height_types()    
    code = height_types.loc[height_types.id==heightTypeId].code.values[0]


    # params = {}
    # r = s.get(url=url, params=params)
    # data_dict = json.loads(r.text)
    # code = data_dict['code']   
    return code

##--------------------------------------------------------------------------------

def get_height_id(code):    
    """
    Return  height type id
    params:
        code = acronym for height type i.e. HRWL, LRWL
    return:
        unique IWLS database height type id
    """
    height_types = get_height_types()    
    id = height_types.loc[height_types.code==code].id.values[0]
    return id        

##--------------------------------------------------------------------------------

def get_height_types():    
    """
    Return  Dataframe of height types

    return:
            Dataframe of height types
    """
    
    params = {}
    r = s.get(url=heights_url, params=params)
    data_json = r.json()
    df = pd.DataFrame.from_dict(data_json)
    return df

##--------------------------------------------------------------------------------

def find_data_gaps(station_id,time_series_id,start_time,end_time):
    """
    Return a dataframe of gaps for a specified period
    params:
        station_id: ex: id=5cebf1df3d0f4a073c4bbd1e
        time_seris_id: ex: 
        start_time = Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
        end_time = End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
    return:
        Pandas dataframe containing gap data
    """
    url = private_station_url + station_id + '/time-series/' + time_series_id + '/find-data-gaps/'
    params ={
            'from':start_time,
            'to': end_time
            }
    r = s.get(url=url, params=params)
    data_json = r.json()
    df = pd.DataFrame.from_dict(data_json)
    if not df.empty:
    #Convert start an end strings to datetimes
        df['start'] = df['start'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ'))
        df['end'] = df['end'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ'))
    
    return df

def get_data_gaps(station_id,time_series_id,start_time,end_time):
    """
    Return a dataframe of gaps for a specified period
    params:
        station_id: ex: id=5cebf1df3d0f4a073c4bbd1e
        time_series: ex: id=5cebf1de3d0f4a073c4bb96a
        start_time = Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
        end_time = End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
    return:
        Pandas dataframe containing gap data
    """
    url = private_station_url + station_id + '/time-series/' + time_series_id + '/find-data-gaps/'
    params ={
            'from':start_time,
            'to': end_time
            }
    r = s.get(url=url, params=params)
    data_json = r.json()

    # Check the type of json reply - If valid data should be a list of 
    # dict objects, if an error then it returns a dict (not enclosed in a list)
    # Error example: 
    # <class 'dict'> {'message': 'frequency per hour is null', 'code': 'EMPTY_STATION_EXPECTED_PRODUCTIVITY'}
    
    # Check if it returns a list 
    if type(data_json) == list:
        # Good we have a list - is it empty ?
        if not data_json:
            return None 
    else:
        # Not so good - we have a dict - probably 
        return None 

    # Looks to be ok - convert to dataframe 
    df = pd.DataFrame.from_dict(data_json)

    # Additional test for columns in dataframe - just in case !!
    if not all([item in df.columns for item in ['start','end','numberOfMissingData']]):
        return None 
    
    if not df.empty:
        #Convert start an end strings to datetimes
        df['start'] = df['start'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ'))
        df['end'] = df['end'].apply(lambda x: datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ'))
    
    return df

##--------------------------------------------------------------------------------

def get_station_timeseries(station_code,time_series_code,start_time,end_time):
    """
    Get timeseries for a single station
    params:
        station_code = five digits station identifier (string)
        time_series_code = Code of the timeseries (wlo,wlp, wlf,wlp-hilo)
        start_time = Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
        end_time = End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
    return:
        Pandas dataframe containing time series data 
    """

    # For stats - check the time to run 
    # start_time_stats = time.time()

    # Get the station id 
    station_id = get_station_id(station_code)
    if station_id is None:
        return None

    # Have to split the calls to the API for certain time series types 
    up_to_a_year = ['wlp-bores','wcsp-extrema','wcdp-extrema','wcp-slack','wlp-hilo']

    if time_series_code in up_to_a_year:
        days_per_request = 366
    else:
        days_per_request = 6        

    # Can only get 7 days of data per request, split data in multiple requests
    # Use 6 days for each request 
    start_time_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
    end_time_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")

    if start_time_dt > end_time_dt:
        print('ERROR: Start datetime is greater than End datetime')
        return None

    # Create the list dates splitting every N days 
    date_list = [start_time_dt]
    if (end_time_dt - start_time_dt) > timedelta(days=days_per_request):
        date_list.append(start_time_dt + timedelta(days=days_per_request))
        while (date_list[-1]  + timedelta(days=days_per_request)) < end_time_dt:
            date_list.append(date_list[-1] + timedelta(days=days_per_request))
    date_list.append(end_time_dt)

    # Create list of start time and end time pairs
    time_ranges = []
    for idx, i in enumerate(date_list[:-1]):
        time_ranges.append([i,date_list[idx+1] - timedelta(seconds=1)])
    time_ranges[-1][-1] = time_ranges[-1][-1] + timedelta(seconds=1)

    #  Convert datetime object back to ISO 8601 format strings
    time_ranges_strings = [[datetime.strftime(i,'%Y-%m-%dT%H:%M:%SZ',) for i in x] for x in time_ranges]  

    # Initialize a dataframe - note we had to explicitly declare the 'reviewed' column as a boolean 
    # or we get into concat warnings below
    # FutureWarning: In a future version, object-dtype columns with all-bool values will not be included in 
    # reductions with bool_only=True. Explicitly cast to bool dtype instead.
    data_df = pd.DataFrame(columns=['eventDate','qcFlagCode','value','timeSeriesId','reviewed'])
    data_df['reviewed'] = data_df['reviewed'].astype('bool')

    # Execute all get requests
    url = station_url + station_id + '/data'
    for i in time_ranges_strings:
        params = {
            'time-series-code':time_series_code,
            'from':i[0],
            'to': i[1]
            }
        r = s.get(url=url, params=params)
        df = pd.DataFrame.from_dict(r.json())
        # print(df.head(5))

        # Dataframe append is deprecated so use concat 
        # FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.
        # In a future version, this will no longer exclude empty or all-NA columns when determining the result 
        # dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
        # data_df = pd.concat([data_df, df])
        data_df = pd.concat([data_df, df])

    # For stats - check the time to run 
    # print(f'Time  = {round((time.time() - start_time_stats),1)} ')
    # Check if we have any data and if so, convert the eventDate column to a 
    # datetime index. This makes it easier in subsequent calculations  
    if data_df is not None:
        if 'eventDate' in data_df.columns:
            data_df = data_df.loc[data_df.index.notnull()]
            data_df = data_df.dropna(subset = ['eventDate'])
            data_df['eventDate'] = pd.to_datetime(data_df['eventDate'])
            data_df = data_df.set_index('eventDate')

    # Create IWLS_timeseries object
    if not data_df.empty:
        # Format it accordingly 
        data_df = data_df[['value','qcFlagCode']]
        return data_df
    else:
        return data_df

##--------------------------------------------------------------------------------   

def get_station_timeseries_private(station_code,time_series_code,start_time,end_time):
    """
    Get timeseries for a single station using the private api 
    params:
        station_code = five digits station identifier (string)
        time_series_code = Code of the timeseries (wlo,wlp, wlf,wlp-hilo)
        start_time = Start time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
        end_time = End time, ISO 8601 format UTC (e.g.: 2019-11-13T19:18:00Z)
    return:
        Pandas dataframe containing time series data 
    """

    # For stats - check the time to run 
    # start_time_stats = time.time()

    # Get the station id 
    station_id = get_station_id_private(station_code)
    if station_id is None:
        return None

    # Have to split the calls to the API for certain time series types 
    up_to_a_year = ['wlp-bores','wcsp-extrema','wcdp-extrema','wcp-slack','wlp-hilo']

    if time_series_code in up_to_a_year:
        days_per_request = 366
    else:
        days_per_request = 6        

    # Can only get 7 days of data per request, split data in multiple requests
    # Use 6 days for each request 
    start_time_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
    end_time_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%SZ")

    if start_time_dt > end_time_dt:
        print('ERROR: Start datetime is greater than End datetime')
        return None

    # Create the list dates splitting every N days 
    date_list = [start_time_dt]
    if (end_time_dt - start_time_dt) > timedelta(days=days_per_request):
        date_list.append(start_time_dt + timedelta(days=days_per_request))
        while (date_list[-1]  + timedelta(days=days_per_request)) < end_time_dt:
            date_list.append(date_list[-1] + timedelta(days=days_per_request))
    date_list.append(end_time_dt)

    # Create list of start time and end time pairs
    time_ranges = []
    for idx, i in enumerate(date_list[:-1]):
        time_ranges.append([i,date_list[idx+1] - timedelta(seconds=1)])
    time_ranges[-1][-1] = time_ranges[-1][-1] + timedelta(seconds=1)

    #  Convert datetime object back to ISO 8601 format strings
    time_ranges_strings = [[datetime.strftime(i,'%Y-%m-%dT%H:%M:%SZ',) for i in x] for x in time_ranges]  

    # Initialize a dataframe - note we had to explicitly declare the 'reviewed' column as a boolean 
    # or we get into concat warnings below
    # FutureWarning: In a future version, object-dtype columns with all-bool values will not be included in 
    # reductions with bool_only=True. Explicitly cast to bool dtype instead.
    # data_df = pd.DataFrame(columns=['eventDate','qcFlagCode','value','timeSeriesId','reviewed'])
    data_df = pd.DataFrame(columns=['timeSeriesId','value','qcFlag','eventDateEpoch'])    
    # data_df['reviewed'] = data_df['reviewed'].astype('bool')

    # Execute all get requests
    # Use the private url 
    # /time-series/5d9dd7c233a9f593161c3f13/data?from=2025-01-24T09%3A00%3A00Z&
    # to=2025-01-26T09%3A00%3A00Z

    # Need to get the timeseries id from the private api 
    time_series_id = get_timeseries_id(station_id, time_series_code)   

    # Construct the private url 
    url = f'{private_station_url}{station_id}/time-series/{time_series_id}/data'
    for i in time_ranges_strings:
        params = {
            'from':i[0],
            'to': i[1]
            }
        r = s.get(url=url, params=params)
        df = pd.DataFrame.from_dict(r.json())

        # Dataframe append is deprecated so use concat 
        # FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.
        # In a future version, this will no longer exclude empty or all-NA columns when determining the result 
        # dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
        # data_df = pd.concat([data_df, df])
        data_df = pd.concat([data_df, df])

    # For stats - check the time to run 
    # print(f'Time  = {round((time.time() - start_time_stats),1)} ')
    # Check if we have any data and if so, convert the eventDate column to a 
    # datetime index. This makes it easier in subsequent calculations  
    # Look at how dates come back 
    # Convert from epoch time - our epoch time is in milliseconds 
    # i.e. df['date'] = pd.to_datetime(df['date'],unit='ms')
    # This is what the dataframe looks like 
    #     timeSeriesId  value    qcFlag  eventDateEpoch
    # 0  5d9dd7c233a9f593161c3f13  0.796  NOT_EVAL   1734254460000
    # 1  5d9dd7c233a9f593161c3f13  0.569  NOT_EVAL   1734273180000

    data_df['eventDate'] = pd.to_datetime(data_df['eventDateEpoch'],unit='ms')

    # Rename qcflag to qcFlagCode
    data_df = data_df.rename(columns={'qcFlag': 'qcFlagCode'})

    # AA - be sure the data coming back from this is the same as calling the public method 
    # i.e. identical to returns from get_station_timeseries

    if data_df is not None:
        if 'eventDate' in data_df.columns:
            data_df = data_df.loc[data_df.index.notnull()]
            data_df = data_df.dropna(subset = ['eventDate'])
            data_df['eventDate'] = pd.to_datetime(data_df['eventDate'])
            data_df = data_df.set_index('eventDate')

    # Create IWLS_timeseries object
    if not data_df.empty:
        # Format it accordingly 
        data_df = data_df[['value','qcFlagCode']]
        return data_df
    else:
        return data_df

##--------------------------------------------------------------------------------   

def get_metadata_class(station):
    #Converts a dictionary of station data to a station class object
    
    class station_class:
        def __init__(self, station=None):
            if station is not None:
                for key, value in station.items():
                    setattr(self, key, value)
    
    return station_class(station)
##--------------------------------------------------------------------------------

def get_metadata_rest(station_id):
    """
    Get metadata for station code using the public REST service 
    return:
        Pandas dataframe containing metadata for station
    """
    url = private_station_url + station_id + '/metadata'
    r = s.get(url=url)
    data_json = r.json()
    return data_json

##--------------------------------------------------------------------------------

def get_crc_hex_string(dataToSend):
    """
    returns Modbus 16 CRC calculation 
    helper function to create_messages_for_iwls
    """
    crc16_bytes = crc16(dataToSend)
    int_crc16 = int.from_bytes(crc16_bytes,byteorder='little')
    hex_crc16 = hex(int_crc16)
    return hex_crc16[2:]

##--------------------------------------------------------------------------------

# def get_crc_hex_string_old(dataToSend):
#     """
#     returns Modbus 16 CRC calculation 
#     helper function to create_messages_for_iwls
#     """
#     crc16 = libscrc.modbus(dataToSend)
#     return hex(crc16)[2:]

##--------------------------------------------------------------------------------

def create_messages_for_iwls(data,listsensors,iwlssensors,regionheader,station_code):
    """
    Get a list of messages to send to iwls for a specific station
    inputs: 
    data: dataframe formatted like the example below
    listsensors: list of sensor names in the correct order with the proper name ex: ["VR","SU","ENC","PRESA","FTS64-1"]
    iwlssensors: list of IWLS sensor names in the correct order ex:["WL1","WL2","WL3","WL4","WL5"]
    regionheader: name of the region ex: "CTRAR","ATLAN","PACIF","QUE"
    station_code: five digit station code
    returns:
    list of messages 
    """
    # Dataframe looks like this:
    # Sensor       WaterLevelDate       VEGA
    # 0       12/22/2021 16:01:00  5.017,m,G
    # 1       12/22/2021 16:02:00  5.024,m,G
    # 2       12/22/2021 16:03:00  5.031,m,G
    # 3       12/22/2021 16:04:00  5.036,m,G

    # Filter iwlssensors that are in df
    list_columns = []
    list_iwls_columns = []
    for col in data[0].columns:
            # Find the column in the xml string that matches the sensor name for IWLS
        if col in listsensors:
            list_columns.append(col)
            sensor_idx = listsensors.index(col)
            iwls_sensor = iwlssensors[sensor_idx]
            list_iwls_columns.append(iwls_sensor)
    
    # calculate message header
    # i.e. $PACIF,07120,WL1,WL2,WL3;
    line1sensors = ''

    # Create the sensor portion of the message header 
    # Only include IWLS sensors, where we have a "matching"
    # sensor name in the columns of the dataframe 
    # Then, if no values (like AC, BAT), they will be replaced with 
    # ",,,"
    for sensor in list_iwls_columns:
        line1sensors += ',' + sensor

    header = regionheader +  ',' + station_code + line1sensors + ';'
    datalines = ''
    
    # Get each gap line from the dataframe and format for IWLS  
    # NOTE: No linefeeds until final line
    # NOTE: Checksum calculated on data between the $ and the *    
    # $PACIF,07120,WL1,WL2,WL3;
    # 181212,170300,0.565,m,G,0.567,m,G,0.565,m,B;
    # 181212,170000,0.562,m,G,0.564,m,G,0.561,m,G;
    # 181212,165700,0.56,m,G,0.562,m,G,0.559,m,G;
    # 181212,165400,0.553,m,G,0.554,m,G,0.552,m,G;
    # *8F56
    messages = []
    for shorterdata in data:
        for index, row in shorterdata.iterrows():
            # Convert the date to iwls format 
            try:
                water_level_date = datetime.strptime(row['WaterLevelDate'], "%Y-%m-%d %H:%M:%S").strftime("%y%m%d,%H%M%S")
            except:
                water_level_date = datetime.strptime(row['WaterLevelDate'], "%m/%d/%Y %H:%M:%S").strftime("%y%m%d,%H%M%S")
            # Loop thru the columns and match the sensors 
            msg = water_level_date + ','
            for sensor in list_columns:
                # Get the row and column
                v = row[sensor]

                # Check for nulls ! 
                if pd.isnull(v):
                    msg += ',,,'
                else:
                    # Check we have the 3 values - Value, Units, Quality
                    a = v.split(',')
                    if len(a)==3:
                        # Probably have good values 
                        msg += v + ','
                    else:
                        msg += ',,,'

            # Test the last characters as it will be a comma
            # We want to replace it with a semi colon 
            # Check we have the proper number of commas in the string 
            # Should be 2 for the date, and 3 for each reading
            if (msg[-1]==','):
                # Remove it and replace with ;
                msg = msg[:-1] + ';'
            datalines += msg

        # Data ends with a ';'
        # Need to remove the last ';'
        if (datalines[-1]==';'):
            datalines = datalines[:-1]

        # Checksum : the asterisk ending the data fields is followed by a 
        # 4 digit checksum CRC-16 MODBUS. This checksum can be used to ensure 
        # the integrity of the contained message. The CRC is calculated on 
        # all elements of the string between the $ and the asterisk.
        message = header + datalines
        checkSum = get_crc_hex_string(message.encode('utf-8')).upper()

            # Create the message to send 
        message = '["$' + message + '*' + checkSum + '"]\r\n'
        messages.append(message)
    return messages

##--------------------------------------------------------------------------------

def send_messages_to_IWLS(messages,iwls_environment):
    """
    Send a list of messages to IWLS for importing
    inputs:
    iwls_environment should be one of "dev","test","prod"
    messages should be a list of messages formatted as such
    """
    # $PACIF,07120,WL1,WL2,WL3;
    # 181212,170300,0.565,m,G,0.567,m,G,0.565,m,B;
    # 181212,170000,0.562,m,G,0.564,m,G,0.561,m,G;
    # 181212,165700,0.56,m,G,0.562,m,G,0.559,m,G;
    # 181212,165400,0.553,m,G,0.554,m,G,0.552,m,G;


    messages = messages
    iwls_environment = iwls_environment
    # Application ID - Based on your App Registry - To be provided by IT Maintenance team
    # Note: Kevin calls this the CHS Observation loader  App ID
    CLIENT_ID = "bd756f93-cf17-4e83-bf84-a1910d1c8db9"

    # Application password/secret. This string will be provided by the IT Maintenance team
    # Note: Kevin calls this the CHS Observation loader Secret
    CLIENT_SECRET = "IPAMtpC-YNZ.DsRf1dNHj4RT.m.g0YYf9m"

    # Updated 2022-02-03
    PRIVATEAPI_URLS = {
        "dev": "hidden",
        "test": "hidden",
        "prod": "hidden"
    }

    # PRIVATEAPI_RESOURCE_ID = "557c3d1a-801f-4474-a8c5-06b1e26a5ce4"
    # Keven calls this Private API App ID
    RESOURCE = "fb2137b7-ad9a-4730-88df-781e85402628"

    # Authentication stuff
    auth_context = AuthenticationContext(
        "hidden")
    SESSION = requests.Session()


    # SPINE app
    tokens = auth_context.acquire_token_with_client_credentials(
        RESOURCE, CLIENT_ID, CLIENT_SECRET)

    SESSION.headers.update(
        {'Authorization': "Bearer " + tokens['accessToken']})
    
    if iwls_environment in PRIVATEAPI_URLS:
        for message in messages:

            # Make the post request 
            response = SESSION.post(f"{PRIVATEAPI_URLS[iwls_environment]}/rest/stations/integrateRawObservations",
                                    data=message, headers={'Content-Type':'application/json'})
            status_code = str(response.status_code)
            response_content = str(response.content)    
            # Get response
    return status_code

##--------------------------------------------------------------------------------

def get_timeseries_id(station_id, timeseries_code = "wlo"):

    url = private_station_url + station_id + '/time-series/'
    r = s.get(url=url)
    data_json = r.json()
    for timeseries in data_json:
        if timeseries['code'] == timeseries_code:
            return timeseries['id']
    return None

##--------------------------------------------------------------------------------
def send_to_iwls(message,environment):
    # Sends the data to the private API and returns status code

    # Application ID - Based on your App Registry - To be provided by IT Maintenance team
    # Note: Kevin calls this the CHS Observation loader  App ID
    CLIENT_ID = "bd756f93-cf17-4e83-bf84-a1910d1c8db9"

    # Application password/secret. This string will be provided by the IT Maintenance team
    # Note: Kevin calls this the CHS Observation loader Secret
    CLIENT_SECRET = "IPAMtpC-YNZ.DsRf1dNHj4RT.m.g0YYf9m"

    # Updated 2022-02-03
    PRIVATEAPI_URLS = {
        "dev": "hidden",
        "test": "hidden",
        "prod": "hidden"
    }

    # PRIVATEAPI_RESOURCE_ID = "557c3d1a-801f-4474-a8c5-06b1e26a5ce4"
    # Keven calls this Private API App ID
    RESOURCE = "fb2137b7-ad9a-4730-88df-781e85402628"


    # Authentication stuff
    auth_context = AuthenticationContext(
        "hidden")
    SESSION = requests.Session()

    # What return codes indicate success ?
    codes_success = ['200','201','202']

    # SPINE app
    tokens = auth_context.acquire_token_with_client_credentials(
        RESOURCE, CLIENT_ID, CLIENT_SECRET)

    SESSION.headers.update(
        {'Authorization': "Bearer " + tokens['accessToken']})
    
    for iwlsEnv in [*PRIVATEAPI_URLS]:
        if iwlsEnv == environment:
            
            # Make the post request 
            response = SESSION.post(f"{PRIVATEAPI_URLS[iwlsEnv]}/rest/stations/integrateRawObservations",
                                    data=message, headers={'Content-Type':'application/json'})

            # Get response
            status_code = str(response.status_code)
            response_content = str(response.content)
            logging.info('Return Code:' + status_code)
            logging.info('Return Content:' + response_content)               

    return status_code, response_content
##--------------------------------------------------------------------------------
def get_list_of_queries(df_station_gaps, log_name=None):
    # Returns a list of queries to be sent to the datalogger

    # Dataframe looks like this 
    #    start                                     end
    # 0 2022-01-16 18:10:00+00:00 2022-01-16 18:10:00+00:00
    # 0 2022-01-16 22:32:00+00:00 2022-01-16 22:32:00+00:00

    # Format df time columns
    df = df_station_gaps
    df['start'] = df['start'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['end'] = df['end'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Now convert to the list 
    # This will create a list of queries for the sutron data logger
    # Convert to a list for getting the list of queries for the particular station          
    # List should look like this
    # [['2021-10-28 12:46:00', '2021-10-28 12:46:00'], ['2021-10-28 18:09:00', '2021-10-28 18:16:00']]
    list_original_gaps = df.values.tolist()

    # Now send this list to our list expander 
    # Note: we could just loop though the list, and if a date time delta is greater 
    # than a certain freqency then just send the single start and end to the 
    # function, or we can send the whole list and get back a new list with 
    # any date ranges expanded - this is debatable 
    # I would say just send the whole list - when timing the call it is only about 2 to 
    # 5 extra milliseconds to send the whole list 
    # We could also only do this when we are on a 'long' timespan 
    # again open for debate 
    # Frequency:
    # 1D, 6H, 1T (each minute)
    list_gaps = expand_list_date_range(list_original_gaps,'1D')

    # List to hold the queries 
    list_queries = []

    # Loop thru gaps and create the list 
    for g in list_gaps:
        # Convert each gap into date objects - then into string objects formatted for sutron data logger 
        start = datetime.strftime(datetime.strptime(g[0], "%Y-%m-%d %H:%M:%S"),"%m-%d-%Y %H:%M")  
        end = datetime.strftime(datetime.strptime(g[1], "%Y-%m-%d %H:%M:%S"),"%m-%d-%Y %H:%M")          

        # Create the query to send
        send_message = b''
        if (log_name is not None) and (log_name != '') :  
            send_message = b'get /F ' + log_name.encode("utf-8") + b' /S ' + start.encode("utf-8") + b' /E ' + end.encode("utf-8") + b' /ny /c /csv\r\n'
        else:
            send_message = b'get /S ' + start.encode("utf-8") + b' /E ' + end.encode("utf-8") + b' /ny /c /csv\r\n'

        list_queries.append(send_message)
   
    return list_queries
#----------------------------------------------------------------------
def expand_list_date_range(list_dates,frequency):
    # For ease of use - convert to dataframe first 
    # Show Else
    df = pd.DataFrame(list_dates)
    df.columns = ['start', 'end']

    list_dates_expanded = []
    for index, row in df.iterrows():
        # Expand the date range 
        date_expanded = pd.date_range(start=row['start'],end=row['end'],freq=frequency)

        # Expanded dates look like this 
        # range: 2021-10-01 09:16:00 2021-10-11 13:25:00
        # Expanded out to a Datetimeindex 
        # DatetimeIndex(['2021-10-01 09:16:00', '2021-10-03 09:16:00',
        #             '2021-10-05 09:16:00', '2021-10-07 09:16:00',
        #             '2021-10-09 09:16:00', '2021-10-11 09:16:00'],
        #             dtype='datetime64[ns]', freq='2D')

        # Test - is there only one entry in the date_range 
        # If so, then there is no need to expand 
        if date_expanded.size == 1:
            l = []        
            l.append(row['start'])
            l.append(row['end'])
            list_dates_expanded.append(l)
            continue
        
        previous_date = None
        i=0
        for current_date in date_expanded:
            # We are on at least the second element in the Datetimeindex 
            if previous_date is not None:
                l=[]
                l.append(previous_date.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'))
                l.append(current_date.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'))
                list_dates_expanded.append(l)

            # Set the current date to previous so we can use it later on 
            previous_date=current_date
            i +=1

            # If we are on the last element in the Datetimeindex then 
            # we need to create the proper ending date time to the list 
            if i==date_expanded.size:
                l = []
                l.append(current_date.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S'))
                l.append(row['end'])
                list_dates_expanded.append(l)           

    # Dr. Phil wanted the list sorted with the most recent gap first 
    list_dates_expanded.sort(key = lambda row: row[0],reverse=True)
    return list_dates_expanded
##--------------------------------------------------------------------------------
def get_metadata_from_xml(station_id, xml_file):

    # Get as much metadata as we can get for now 
    station = get_station_class(station_id)

    # Open the xml file and add more properties 
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Get the particular station
    is_found = False 
    for el in root.findall("station/[stationid='%s']" % station_id):

        # Get the elements 
        is_found = True 
        stationid = el.find('stationid').text
        stationname = el.find('stationname').text
        ipaddress = el.find('ipaddress').text
        port = el.find('port').text
        sensors = el.find('sensors').text.split(',')
        iwlssensors = el.find('iwlssensors').text.split(',')
        regionheader = el.find('regionheader').text
        enabled = el.find('enabled').text
        sensorlog = el.find('logname')
        if sensorlog != None:
            sensorlog = sensorlog.text
        username = el.find('user_login')
        if username != None:
            username = username.text
        userpass = el.find('user_pass')
        if userpass != None:
            userpass = userpass.text
        logger_file_name = el.find('logger_file_name')
        if logger_file_name != None:
            logger_file_name = logger_file_name.text
        script_variable_sensor = el.find('script_variable_sensor')
        if script_variable_sensor != None:
            script_variable_sensor = script_variable_sensor.text
        script_variable_iwls = el.find('script_variable_iwls')
        if script_variable_iwls != None:
            script_variable_iwls = script_variable_iwls.text        

        # Add specific properties to the metadata object 
        station.ip = ipaddress
        station.port = port 
        station.sutron_sensors = sensors 
        station.iwls_sensors = iwlssensors
        station.region_header = regionheader
        station.sensor_log = sensorlog
        station.user_name = username
        station.user_pass = userpass
        station.enabled = True if enabled.lower() == 'true' else False
        station.logger_file_name = logger_file_name
        station.script_variable_sensor = script_variable_sensor
        station.script_variable_iwls = script_variable_iwls
        station.xconnectlogfile = None


        # Temporary for testing - Mike 2021-11-24
        if station.enabled == False:
            logging.info(f'Not enabled for {stationid} {stationname}')
            return None
        else:
            logging.info(f'Enabled  {stationid} {stationname} {enabled}')
        # Check for a different logfile (default=SSP.log)
        xconnectlogfile = el.find('logname')
        if xconnectlogfile is not None: 
            xconnectlogfile = el.find('logname').text
            station.xconnectlogfile = xconnectlogfile

        # Break out of loop - we only want the first station match 
        break        

    if (station):
        if (is_found):
            return station
        else:
            return None
    else:
        return None
##--------------------------------------------------------------------------------    
def get_station_class(station_code):
    """
    Return a json response from the IWLS API containing metadata for a single station
    as a class object 

    param:
        station_code = five digit station identifier (string)
    """
    
    # Get the metadata 
    metadata = get_station_metadata(station_code)

    # Get a station class 
    station = get_metadata_class(metadata)

    return station
##--------------------------------------------------------------------------------
def get_gap_data_from_station(metadata_dict, list_queries,strike):
    # 1. connect to station (authenticate if necessary)
    # 2. update sensor map if necessary
    # 3. get gap data
    # 4. format data and return as df

    # Set function variables
    sutron_sensors = metadata_dict.sutron_sensors
    iwls_sensors = metadata_dict.iwls_sensors
    ipaddress = metadata_dict.ip_address
    port = metadata_dict.port
    username = metadata_dict.user_login
    password = metadata_dict.user_pass
    basic_file_name = metadata_dict.basic_file_name
    script_variable_sensor = metadata_dict.script_variable_sensor
    script_variable_iwls = metadata_dict.script_variable_iwls
    station_code = metadata_dict.code   
    official_name = metadata_dict.officialName

    # Create a TCP/IP socket via Telnet
    # format credentials to be accepted by Sutron (8310)
    if (username is not None):
        username = username+"\r"
        username = bytes(username, 'utf-8') 
    
    if (password is not None):
        password = password+"\r"
        password = bytes(password, 'utf-8')

    # Connect to station (authenticate if necessary)
    try:
        with telnetlib.Telnet(ipaddress, port) as tn:  

            # read_until can have a timeout parameter. Timeout is in seconds 
            timeout = 10 

            if username is not None:
                try:
                    tn.read_until(b'Login user: ',timeout)

                except socket.timeout:
                    # Raise exception for connection failure due to timeout 
                    return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Login User'

                # Send user / password if required 
                tn.write(username)
                tn.write(password)

            # Look for flash disk prompt
            try:
                reply = tn.read_until(b'Flash Disk>',timeout)

            except socket.timeout:
                # Raise exception for connection failure due to timeout 
                return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Waiting for Initial Flash Disk prompt'

            # Connected to station, now get sensor map for station (only on first connect)
            # 2024-12-05 No need to do this anymore 
            # Rely on key value pairs 
            # if strike == 0:
            #     push_message = f"TYPE {basic_file_name} /C\r\n"
            #     get_push = bytes(push_message, 'utf-8')
            #     tn.write(get_push)

            #     try:
            #         reply = tn.read_until(b'Flash Disk>',timeout)

            #     except socket.timeout:
            #         # Raise exception for connection failure due to timeout 
            #         return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Getting basic file from logger'

            #     if 'File not found.' in str(reply):
            #         # Push file does not exist or name changed
            #         return None, metadata_dict, f'{official_name} {station_code}: Push file (.bas) not found in logger'

            #     sutron_sensors, iwls_sensors = get_stations_as_lists(
            #                 reply, script_variable_sensor,  script_variable_iwls)

            #     # Update metadata object
            #     metadata_dict.sutron_sensors = sutron_sensors
            #     metadata_dict.iwls_sensors = iwls_sensors

            # Send queries to station to collect data
            logger_list = []
            timeout_query = 25

            x = 0 
            for query in list_queries:
                logging.info(f'{official_name} {station_code} {metadata_dict.ip}: {query}' )

                # When a large number of queries are run for a station, on some stations,
                # the communications start to produce errors, usually after around 15 queries 
                # thus we will sleep a little bit between queries after 10 queries are run 
                # and we may have to adjust this in the future 
                # x += 1
                # if x > 9:
                #     logging.info(f'{official_name} {station_code}: Sleeping for 5 seconds. Query Number:{x}')
                #     time.sleep(5)

                # Send query to logger
                tn.write(query)

                try:
                    reply = tn.read_until(b'Flash Disk>',timeout_query)
                    msg = reply.decode('utf-8').split('\r\n')
                    logging.debug(f'msg: {msg}')
                
                except socket.timeout:
                    # Raise exception for connection failure due to timeout 
                    return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Sending query to station'
                
                # Obtained a response from the station
                if msg:
                    # Validate through the enforcer function
                    enforced_msg = station_response_enforcer(query, msg, station_code, official_name)

                    # If response if None, message did not pass tests
                    if not enforced_msg:
                        # Go to next query
                        continue
                else:
                    logging.info(f'{official_name} {station_code}: Query={query} msg returned was empty ')

                    # Get the next query 
                    continue

                # Add list elements into logger_list
                logger_list.extend(enforced_msg) 

            # Obtained gap data from station, now format
            if not logger_list:
                # List is empty - no data returned from sutron
                return None, metadata_dict,f'{official_name} {station_code}: Queries were run, but, no data received'

            formatted_gap_data = gap_data_formatter(logger_list, sutron_sensors)

    except Exception as e:
        # Error opening the telnet connection 
        return None, metadata_dict,'Could not create the Telnet connection'

    # Everything is super great - it all worked ! 
    return formatted_gap_data, metadata_dict,'Good'

##--------------------------------------------------------------------------------
def get_gap_data_from_station_OLD_20241205(metadata_dict, list_queries,strike):
    # 1. connect to station (authenticate if necessary)
    # 2. update sensor map if necessary
    # 3. get gap data
    # 4. format data and return as df

    # Set function variables
    sutron_sensors = metadata_dict.sutron_sensors
    iwls_sensors = metadata_dict.iwls_sensors
    ipaddress = metadata_dict.ip_address
    port = metadata_dict.port
    username = metadata_dict.user_login
    password = metadata_dict.user_pass
    basic_file_name = metadata_dict.basic_file_name
    script_variable_sensor = metadata_dict.script_variable_sensor
    script_variable_iwls = metadata_dict.script_variable_iwls
    station_code = metadata_dict.code   
    official_name = metadata_dict.officialName

    # Create a TCP/IP socket via Telnet
    # format credentials to be accepted by Sutron (8310)
    if (username is not None):
        username = username+"\r"
        username = bytes(username, 'utf-8') 
    
    if (password is not None):
        password = password+"\r"
        password = bytes(password, 'utf-8')

    # Connect to station (authenticate if necessary)
    try:
        with telnetlib.Telnet(ipaddress, port) as tn:  

            # read_until can have a timeout parameter. Timeout is in seconds 
            timeout = 10 

            if username is not None:
                try:
                    tn.read_until(b'Login user: ',timeout)

                except socket.timeout:
                    # Raise exception for connection failure due to timeout 
                    return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Login User'

                # Send user / password if required 
                tn.write(username)
                tn.write(password)

            # Look for flash disk prompt
            try:
                reply = tn.read_until(b'Flash Disk>',timeout)

            except socket.timeout:
                # Raise exception for connection failure due to timeout 
                return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Waiting for Initial Flash Disk prompt'

            # connected to station, now get sensor map for station (only on first connect)
            if strike == 0:
                push_message = f"TYPE {basic_file_name} /C\r\n"
                get_push = bytes(push_message, 'utf-8')
                tn.write(get_push)

                try:
                    reply = tn.read_until(b'Flash Disk>',timeout)

                except socket.timeout:
                    # Raise exception for connection failure due to timeout 
                    return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Getting basic file from logger'

                if 'File not found.' in str(reply):
                    # Push file does not exist or name changed
                    return None, metadata_dict, f'{official_name} {station_code}: Push file (.bas) not found in logger'

                sutron_sensors, iwls_sensors = get_stations_as_lists(
                            reply, script_variable_sensor,  script_variable_iwls)

                # Update metadata object
                metadata_dict.sutron_sensors = sutron_sensors
                metadata_dict.iwls_sensors = iwls_sensors

            # Send queries to station to collect data
            logger_list = []
            timeout_query = 25

            x = 0 
            for query in list_queries:
                logging.info(f'{official_name} {station_code} {metadata_dict.ip}: {query}' )

                # When a large number of queries are run for a station, on some stations,
                # the communications start to produce errors, usually after around 15 queries 
                # thus we will sleep a little bit between queries after 10 queries are run 
                # and we may have to adjust this in the future 
                # x += 1
                # if x > 9:
                #     logging.info(f'{official_name} {station_code}: Sleeping for 5 seconds. Query Number:{x}')
                #     time.sleep(5)

                # Send query to logger
                tn.write(query)

                try:
                    reply = tn.read_until(b'Flash Disk>',timeout_query)
                    msg = reply.decode('utf-8').split('\r\n')
                    logging.debug(f'msg: {msg}')
                
                except socket.timeout:
                    # Raise exception for connection failure due to timeout 
                    return None, metadata_dict,f'{official_name} {station_code}: Socket Timeout - Sending query to station'
                
                # Obtained a response from the station
                if msg:
                    # Validate through the enforcer function
                    enforced_msg = station_response_enforcer(query, msg, station_code, official_name)

                    # If response if None, message did not pass tests
                    if not enforced_msg:
                        # Go to next query
                        continue
                else:
                    logging.info(f'{official_name} {station_code}: Query={query} msg returned was empty ')

                    # Get the next query 
                    continue

                # Add list elements into logger_list
                logger_list.extend(enforced_msg) 

            # Obtained gap data from station, now format
            if not logger_list:
                # List is empty - no data returned from sutron
                return None, metadata_dict,f'{official_name} {station_code}: Queries were run, but, no data received'

            formatted_gap_data = gap_data_formatter(logger_list, sutron_sensors)

    except Exception as e:
        # Error opening the telnet connection 
        return None, metadata_dict,'Could not create the Telnet connection'

    # Everything is super great - it all worked ! 
    return formatted_gap_data, metadata_dict,'Good'


##--------------------------------------------------------------------------------
def get_stations_as_lists(push_file, script_variable_sensor, script_variable_iwls):
    # helper function to get_gap_data_from_station
    # after queries push file and read to memory
    # this function will parse and output 2 lists:
    # [sutron sensors] & [iwls sensors]

    l = push_file.decode('utf-8').split('\r\n')
    
    #isolate sensors from script_variable label
    sutron = [x for x in l if x.startswith(script_variable_sensor)]
    iwls = [x for x in l if x.startswith(script_variable_iwls)]

    sutron_sensors = []
    iwls_sensors = []

    for sutron_sensor, iwls_sensor in zip(sutron, iwls):

        # isolate sensor names from string
        sutron_sensor = sutron_sensor.split('"')[1]
        iwls_sensor = iwls_sensor.split('"')[1]
        
        #test strings are not empty and add to list
        if sutron_sensor and iwls_sensor:
            sutron_sensors.append(sutron_sensor)
            iwls_sensors.append(iwls_sensor)

    return sutron_sensors, iwls_sensors
##--------------------------------------------------------------------------------
def station_response_enforcer(query, msg, station_code, official_name=''):
    # helper function to get_gap_data_from_station
    # helps to ensure validity of the station response
    # wrt to the query sent

    # make sure we have some elements in the reply 
    if len(msg) >= 1:
        
        # Check 1: get the first item - it should match the query we asked for 
        query_sent = query.decode().strip()
        query_from_reply = msg[0].strip()
        
        if (query_sent != query_from_reply):
            logging.info(f'{official_name} {station_code}: The query we sent DID NOT return in the first element of the list in the reply ')
            logging.info(f'{official_name} {station_code}: Query sent to logger: {query_sent}')

            # The return, even though erroneous, can be quite large 
            # Thus log a substring   
            if len(query_from_reply) >= 30:
                logging.info(f'{official_name} {station_code}: Query in msg[0]: {query_from_reply[:30]}')
            else:
                logging.info(f'{official_name} {station_code}: Query in msg[0]: {query_from_reply}')

            # Check 1 failed, so return none
            return None

        else:
            # Check 1 Passed, remove first element - s/b "get /hour /ny /c /csv \r\n"
            msg.pop(0)

        # Check 2: get the last item - should be 'Flash Disk'
        if (msg[-1] == '\\Flash Disk>'):
            msg.pop()
        else:
            logging.info(f'{official_name} {station_code}: Last element in msg IS NOT Flash Disk')
            # The return, even though erroneous, can be quite large 
            # Thus log a substring 
            if len(query_from_reply) >= 30:
                logging.info(f'{official_name} {station_code}: msg[-1]: {msg[-1][:30]}')
            else:
                logging.info(f'{official_name} {station_code}: msg[-1]: {msg[-1]}')       

            # Check 2 failed, so return none
            return None
        
        # all tests pass, return back msg
        return msg
##--------------------------------------------------------------------------------    
def gap_data_formatter(gap_data, list_sensors):
    # helper function to get_gap_data_from_station
    # Reformat the dates and times
    # also add to a list for adding to the database 
    # Log looks like this 
    # Date,Time,Sensor,Value,Units,Quality Flag (G=Good, B=Bad)
    # 05/14/2020,16:29:00,SPS,8.828,,G
    # 05/14/2020,16:29:00,OTTPLSCtemp3,10.780,C,G
    # 05/14/2020,16:29:00,OTTPLSCsal3,30.560,psu,G
    # 05/14/2020,16:29:00,OTTPLSCcnd3,46.780,mS/cm,G

    sensor_data = []
    for line in gap_data:

        # Split the line into a list 
        line = line.split(',')
        
        # Get the sensor and check if in our list
        sensor = line[2]
        
        #convert list of tuples to 1d list
        if sensor in list_sensors:
            sensor_data.append(line)
    
    # Convert list to dataframe - concatenate some columns, 
    # drop some columns, to make it easier to pivot the data 
    df = pd.DataFrame(sensor_data,columns=['WaterLevelDate','WaterLevelTime',\
                                'Sensor','WaterLevelValue','Units','Quality'])
    df['Measurement'] = df['WaterLevelValue'] + ',' + df['Units'] + ',' + df['Quality']
    df['WaterLevelDate'] = df['WaterLevelDate'] + ' ' + df['WaterLevelTime']
    df = df.drop(['WaterLevelTime','WaterLevelValue', 'Units','Quality'], axis=1)
    
    # Pivot the table to we have sensor names as column names 
    df1 =df.pivot_table(index=["WaterLevelDate"],columns='Sensor',\
                            values='Measurement',aggfunc=lambda x: ' '.join(x))
    
    # Set the index WaterLevelDate back to a column 
    df1 = df1.reset_index(level=0)

    # Return the dataframe
    return df1  
##--------------------------------------------------------------------------------
def create_message(stationid, listsensors, iwlssensors, gap_data, regionheader):

    # 2022-01-14 Khaleel - lets split up messages if the dataframe is greater than X in size 

    # Dataframe looks like this:
    # Sensor       WaterLevelDate       VEGA
    # 0       12/22/2021 16:01:00  5.017,m,G
    # 1       12/22/2021 16:02:00  5.024,m,G
    # 2       12/22/2021 16:03:00  5.031,m,G
    # 3       12/22/2021 16:04:00  5.036,m,G

    # Filter iwlssensors that are in df
    list_columns = []
    list_iwls_columns = []
    for col in gap_data.columns:
            # Find the column in the xml string that matches the sensor name for IWLS
        if col in listsensors:
            list_columns.append(col)
            sensor_idx = listsensors.index(col)
            iwls_sensor = iwlssensors[sensor_idx]
            list_iwls_columns.append(iwls_sensor)
    
    # calculate message header
    # i.e. $PACIF,07120,WL1,WL2,WL3;
    line1sensors = ''

    # Create the sensor portion of the message header 
    # Only include IWLS sensors, where we have a "matching"
    # sensor name in the columns of the dataframe 
    # Then, if no values (like AC, BAT), they will be replaced with 
    # ",,,"
    for sensor in list_iwls_columns:
        line1sensors += ',' + sensor

    header = regionheader +  ',' + stationid + line1sensors + ';'
    datalines = ''
    
    # Get each gap line from the dataframe and format for IWLS  
    # NOTE: No linefeeds until final line
    # NOTE: Checksum calculated on data between the $ and the *    
    # $PACIF,07120,WL1,WL2,WL3;
    # 181212,170300,0.565,m,G,0.567,m,G,0.565,m,G;
    # 181212,170000,0.562,m,G,0.564,m,G,0.561,m,G;
    # 181212,165700,0.56,m,G,0.562,m,G,0.559,m,G;
    # 181212,165400,0.553,m,G,0.554,m,G,0.552,m,G;
    # *8F56
    for index, row in gap_data.iterrows():
        # Convert the date to iwls format 
        water_level_date = datetime.strptime(row['WaterLevelDate'], "%m/%d/%Y %H:%M:%S").strftime("%y%m%d,%H%M%S")

        # Loop thru the columns and match the sensors 
        msg = water_level_date + ','
        for sensor in list_columns:
            # Get the row and column
            v = row[sensor]

            # Check for nulls ! 
            if pd.isnull(v):
                msg += ',,,'
            else:
                # Check we have the 3 values - Value, Units, Quality
                a = v.split(',')
                if len(a)==3:
                    # Probably have good values 
                    msg += v + ','
                else:
                    msg += ',,,'

        # Test the last characters as it will be a comma
        # We want to replace it with a semi colon 
        # Check we have the proper number of commas in the string 
        # Should be 2 for the date, and 3 for each reading
        if (msg[-1]==','):
            # Remove it and replace with ;
            msg = msg[:-1] + ';'
        datalines += msg

    # Data ends with a ';'
    # Need to remove the last ';'
    if (datalines[-1]==';'):
        datalines = datalines[:-1]

    # Checksum : the asterisk ending the data fields is followed by a 
    # 4 digit checksum CRC-16 MODBUS. This checksum can be used to ensure 
    # the integrity of the contained message. The CRC is calculated on 
    # all elements of the string between the $ and the asterisk.
    message = header + datalines
    checkSum = get_crc_hex_string(message.encode('utf-8')).upper()

    # Create the message to send 
    message = '["$' + message + '*' + checkSum + '"]\r\n'
    return message 

# ---------------------------------------------------------------------------------------
# Added by Mike - 2023-11-22 
# ---------------------------------------------------------------------------------------
def get_timeseries_codes_for_station(station_code):
    """
    Return a list of time series codes IWLS API  for a single station

    param:
        station_code = five digit station identifier (string)
    """    
    metadata = get_station_metadata(station_code)
    timeseries_codes = [t['code'] for t in metadata['timeSeries']]
    return timeseries_codes

# ---------------------------------------------------------------------------------------
# Added by Mike - 2024-04-12 
# Code to test the api endpoints for key values pairs 
# ---------------------------------------------------------------------------------------
def get_all_key_values(iwls_environment: str) -> list:
    """
    Get all key value pairs available 

    param:
        iwls_environment: one of "dev","test","prod"     

    return:
        List containing id and key
        [
            {
                "id": "66196b2eb06c63ec97beedcf",
                "key": "goes_message_type"
            },
            {
                "id": "66196b2fb06c63ec97beedd0",
                "key": "dcp"
            },
            {
                "id": "66196b2fb06c63ec97beedd1",
                "key": "goes_sutron_sensors"
            }
            ... etc ...
        ]
        
    """    
    # Sample URL     
    session, base_url = get_session_auth(iwls_environment)
    url = f'{base_url}/rest/additionalConfigurationKeys/'
    r = session.get(url=url)
    list_keys = []
    data_json = r.json()    
    for dict in data_json:
        for key, value in dict.items():
            if key == 'key':
                list_keys.append(value)
            
    return list_keys    

def get_station_keys(station_code: str, iwls_environment: str) ->dict:
    """
    Get station key value pairs for station code 

    param:
        station_code = five digit station identifier (string)
        iwls_environment: one of "dev","test","prod"    

    return:
        Dict containing keys and values for the station 
        {
        "port": "8081",
        "sutron_sensors": "QWE1,QWE2,FTS81-1",
        "iwls_sensors": "WL1,WL2,WL3",
        "goes_minutes_back": "45",
        "ip_enabled": "true",
        "basic_file_name": "07120_MEAS_IPtoIWLS.bas",
        "script_variable_sensor": "CONST LoggerSensor",
        "script_variable_iwls": "CONST IWLS_TimeSeries",
        "goes_enabled": "False"
        }     
    """
    # Sample URL 
    station_id = get_station_id_private(station_code)
    session, base_url = get_session_auth(iwls_environment)
    url = f'{base_url}/rest/stations/{station_id}/additional-configurations/'
    r = session.get(url=url)
    data_json = r.json()
    return data_json    

def delete_station_keys(station_code: str, keys: list, iwls_environment: str):
    """
    Delete key(s) for a station 

    param:
        station_code = five digit station identifier (string)
        keys = list of keys i.e. ['user_login','user_pass']
        iwls_environment: one of "dev","test","prod"        

    return:
        Status Code, Response Content 
    """   
    # Sample url:
    station_id = get_station_id(station_code)
    session, base_url = get_session_auth(iwls_environment)
    url = f'{base_url}/rest/stations/{station_id}/additional-configurations/'
    r = session.delete(url=url,data=json.dumps(keys),headers={'Content-Type':'application/json'})
    status_code = str(r.status_code)
    response_content = str(r.content)               
    return status_code, response_content

def update_station_keys(station_code: str, key_values: dict, iwls_environment: str):
    """
    Send a dict of key value pairs to IWLS via a PATCH command 

    params: 
        station_code = five digit station identifier (string), code of the station i.e. 07120

        key_values = a dict object of key value pairs 
        {'goes_enabled': 'False', 
        'modem_enabled': 'False', 
        'phone_number': '12503633910,,*3', 
        'sutron_sensors': 'QWE1,QWE2,FTS81-1', 
        'iwls_sensors': 'WL1,WL2,WL3', 
        'goes_minutes_back': '20', 
        'iwls_environment': 'prod', 
        'basic_file_name': '07120_MEAS_IPtoIWLS.bas',
        ....
        }
        
        iwls_environment: one of "dev","test","prod"        
    return:
        Status Code, Response Content             
    """    
    # Sample url:
    
    # 2025-01-31 Changed this to use the private api as the station id is not available from 
    # the public api if the station's disseminated flag is False 
    #station_id = get_station_id(station_code)
    station_id = get_station_id_private(station_code)

    session, base_url = get_session_auth(iwls_environment)
    url = f'{base_url}/rest/stations/{station_id}/additional-configurations/'
    r = session.patch(url=url,data=json.dumps(key_values),headers={'Content-Type':'application/json'})
    status_code = str(r.status_code)
    response_content = str(r.content)               
    return status_code, response_content

def get_session_auth(iwls_environment: str):
    """
    Get a session object that is authenticated and the start of the url 

    params:
        iwls_environment: one of "dev","test","prod"

    return:
        authenticated session object 
        start of the url (dev, test, or prod)

    """
    # Application ID - Based on your App Registry - To be provided by IT Maintenance team
    # Note: Kevin calls this the CHS Observation loader  App ID
    CLIENT_ID = "bd756f93-cf17-4e83-bf84-a1910d1c8db9"

    # Application password/secret. This string will be provided by the IT Maintenance team
    # Note: Kevin calls this the CHS Observation loader Secret
    CLIENT_SECRET = "IPAMtpC-YNZ.DsRf1dNHj4RT.m.g0YYf9m"

    # Updated 2022-02-03
    PRIVATEAPI_URLS = {
        "dev": "hidden",
        "test": "hidden",
        "prod": "hidden"
    }

    # PRIVATEAPI_RESOURCE_ID = "557c3d1a-801f-4474-a8c5-06b1e26a5ce4"
    # Keven calls this Private API App ID
    RESOURCE = "fb2137b7-ad9a-4730-88df-781e85402628"

    # Authentication stuff
    auth_context = AuthenticationContext(
       "hidden")
    SESSION = requests.Session()

    # SPINE app
    tokens = auth_context.acquire_token_with_client_credentials(
        RESOURCE, CLIENT_ID, CLIENT_SECRET)

    SESSION.headers.update(
        {'Authorization': "Bearer " + tokens['accessToken']})

    url = f'{PRIVATEAPI_URLS[iwls_environment]}'
    
    # Return authenticated session and the start of the url 
    return SESSION, url

# ---------------------------------------------------------------------------------------
# Added by Mike Sheward - 2024-06-27
# Add code to get the IP address for the station from the modem definition 
# ---------------------------------------------------------------------------------------
def get_modem(station_code: str, iwls_environment: str) ->dict:
    """
    Get modem object(s) for station code 

    param:
        station_code = five digit station identifier (string)
        iwls_environment: one of "dev","test","prod"    

    return:
        Dict containing keys and values for the station 
        [
            {
                "id": "string",
                "chsRegionId": "string",
                "stationId": "string",
                "serialNumber": "string",
                "purchaseDate": "2024-06-27",
                "installationDate": "2024-06-27",
                "comments": "string",
                "ipAddress": "string",
                "modemModelId": "string",
                "expectedTransmissionPerHour": 0,
                "supplierId": "string",
                "barcode": "string",
                "warranty": 0,
                "phoneNumber": "string",
                "voicePhoneNumber": "string",
                "serviceProvider": "string",
                "specificInformation": "string",
                "antennaTypeModel": "string",
                "softwareFirmwareVersion": "string"
            }
        ]
    """
    # Sample URL 
    station_id = get_station_id(station_code)
    session, base_url = get_session_auth(iwls_environment)
    url = f'{base_url}/rest/modems/?station-ids={station_id}&getAvailableOnly=false'
    r = session.get(url=url)
    data_json = r.json()
    return data_json    

# ---------------------------------------------------------------------------------------
# Added by Mike - 2024-05-15
# Imbedded code for crc 16 calculation
# ---------------------------------------------------------------------------------------
"""
CRC-16 calculation for Modbus protocol
Copyright (c) 2023 webtoucher
Distributed under the BSD 3-Clause license. See LICENSE for more info.
"""

LOW_BYTES = b'\
\x00\xC0\xC1\x01\xC3\x03\x02\xC2\xC6\x06\x07\xC7\x05\xC5\xC4\x04\
\xCC\x0C\x0D\xCD\x0F\xCF\xCE\x0E\x0A\xCA\xCB\x0B\xC9\x09\x08\xC8\
\xD8\x18\x19\xD9\x1B\xDB\xDA\x1A\x1E\xDE\xDF\x1F\xDD\x1D\x1C\xDC\
\x14\xD4\xD5\x15\xD7\x17\x16\xD6\xD2\x12\x13\xD3\x11\xD1\xD0\x10\
\xF0\x30\x31\xF1\x33\xF3\xF2\x32\x36\xF6\xF7\x37\xF5\x35\x34\xF4\
\x3C\xFC\xFD\x3D\xFF\x3F\x3E\xFE\xFA\x3A\x3B\xFB\x39\xF9\xF8\x38\
\x28\xE8\xE9\x29\xEB\x2B\x2A\xEA\xEE\x2E\x2F\xEF\x2D\xED\xEC\x2C\
\xE4\x24\x25\xE5\x27\xE7\xE6\x26\x22\xE2\xE3\x23\xE1\x21\x20\xE0\
\xA0\x60\x61\xA1\x63\xA3\xA2\x62\x66\xA6\xA7\x67\xA5\x65\x64\xA4\
\x6C\xAC\xAD\x6D\xAF\x6F\x6E\xAE\xAA\x6A\x6B\xAB\x69\xA9\xA8\x68\
\x78\xB8\xB9\x79\xBB\x7B\x7A\xBA\xBE\x7E\x7F\xBF\x7D\xBD\xBC\x7C\
\xB4\x74\x75\xB5\x77\xB7\xB6\x76\x72\xB2\xB3\x73\xB1\x71\x70\xB0\
\x50\x90\x91\x51\x93\x53\x52\x92\x96\x56\x57\x97\x55\x95\x94\x54\
\x9C\x5C\x5D\x9D\x5F\x9F\x9E\x5E\x5A\x9A\x9B\x5B\x99\x59\x58\x98\
\x88\x48\x49\x89\x4B\x8B\x8A\x4A\x4E\x8E\x8F\x4F\x8D\x4D\x4C\x8C\
\x44\x84\x85\x45\x87\x47\x46\x86\x82\x42\x43\x83\x41\x81\x80\x40'

HIGH_BYTES = b'\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x01\xC0\x80\x41\x00\xC1\x81\x40\x00\xC1\x81\x40\x01\xC0\x80\x41\
\x00\xC1\x81\x40\x01\xC0\x80\x41\x01\xC0\x80\x41\x00\xC1\x81\x40'

def crc16(data: bytes) -> bytes:
    """Calculate CRC-16 for Modbus."""
    crc_high = 0xFF
    crc_low = 0xFF

    for byte in data:
        index = crc_high ^ int(byte)
        crc_high = crc_low ^ HIGH_BYTES[index]
        crc_low = LOW_BYTES[index]

    return bytes([crc_high, crc_low])

def add_crc(package: bytes) -> bytes:
    """Add CRC-16 to bytes package."""
    return package + crc16(package)

def check_crc(package: bytes) -> bool:
    """Validate signed bytes package."""
    return crc16(package) == b'\x00\x00'

def get_all_metadata(station_code: str) ->StationMetaData:
    """
    Gets all metadata for the station. 
    1. Get metadata object from IWLS 
    2. Get all key value pairs for station from IWLS 
    3. Merge them together 

    params: 
        station_code = five digit station identifier (string), code of the station i.e. 07120

    return:
        StationMetaData object
        Example:
        {'id': '5cebf1df3d0f4a073c4bbd1e'
        'code': '07120'
        'officialName': 'Victoria Harbour'
        'operating': True
        ....
        'goes_enabled': 'True'
        'modem_enabled': 'False'
        'phone_number': '12503633910,,*3'
        'user_login': None
        'user_pass': None
        'sutron_sensors': ['QWE1','QWE2','FTS81-1']
        'iwls_sensors': ['WL1','WL2','WL3']
        'goes_minutes_back': '45'
        'data_logger': '8310'
        'iwls_environment': 'prod'
        'basic_file_name': '07120_DL1_MEAS_IPtoIWLS.bas;07120_DL2_MEAS_IPtoIWLS.bas'
        'script_variable_sensor': 'CONST LoggerSensor'
        'script_variable_iwls': 'CONST IWLS_TimeSeries'
        'ip_address': '184.151.32.86'
        'port': '8081'
        'ip_enabled': 'True'
        'region_header': 'PACIF'}

    """       
    # Get the data from the private api 
    metadata = get_station_metadata(station_code)
    if metadata is None:
        return None
    
    # Presume we have a good metadata object - now get all keys 
    key_value = get_station_keys(station_code, iwls_environment='prod')

    # Append the dictionaries 
    metadata.update(key_value)

    # Create our complete metadata object which includes 
    # the IWLS metadata and all of our custom keys
    station_metadata = StationMetaData()
    for key,value in metadata.items(): 
        setattr(station_metadata, key, value)   

    # 2025-02-06
    # Some keys are different between public and private so lets add 
    # what keys that we can     
    station_metadata.officialName = station_metadata.name
    station_metadata.operating = station_metadata.active

    # Using the private api's generate no region code thus we need to look it up 
    chsRegionCode = get_region_private(station_code)
    station_metadata.chsRegionCode = chsRegionCode

    # Change multivalue strings into a list 
    s = station_metadata.iwls_sensors.split(',')
    setattr(station_metadata,'iwls_sensors',s)
    s = station_metadata.sutron_sensors.split(',')
    setattr(station_metadata,'sutron_sensors',s)   
    s = station_metadata.goes_iwls_sensors.split(',')
    setattr(station_metadata,'goes_iwls_sensors',s)
    s = station_metadata.goes_sutron_sensors.split(',')
    setattr(station_metadata,'goes_sutron_sensors',s)       

    # Change True / False entries in boolean
    b = string_to_bool(station_metadata.ip_enabled)
    setattr(station_metadata,'ip_enabled',b)
    b = string_to_bool(station_metadata.modem_enabled)
    setattr(station_metadata,'modem_enabled',b)       
    b = string_to_bool(station_metadata.goes_enabled)
    setattr(station_metadata,'goes_enabled',b)   

    # Now add any calculated keys 
    region_header = get_region_header(station_metadata.chsRegionCode)
    setattr(station_metadata, 'region_header', region_header)   

    # NOTE: special case - backwards compatibility  
    setattr(station_metadata, 'xconnectlogfile', station_metadata.log_name)       
    setattr(station_metadata, 'ip', station_metadata.ip_address)   

    return station_metadata

def get_all_metadata_for_key_value(key: str, value: str, iwls_environment: str, region=None) ->list:
    """
    Gets all metadata for stations matching a key value. 
    1. Get the list of station dicts for key values queries 
    2. Get the list of all stations 
    3. Get the small list of region ids and code 
    4. Merge them all together and send back a list of station metadata dicts

    params: 
        key
        value
        iwls_environment (dev,test,prod)
        region (optional ATL,CNA,PAC,QUE)

    return:
    List of Station metadata dicts
        Example:
        [
        {
            'basic_file_name': '08615_SCHED_IPtoIWLS.bas',
            'chsRegionCode': 'PAC',
            'chsRegionId': '5ce598e3487b844868928221',
            'code': '08615',
            'data_logger': '8310',
            'dcp': '15C3A33C',
            'goes_enabled': True,
            'goes_iwls_sensors': ['WL1','WL2'],
            'goes_message_type': '6bit',
            'goes_minutes_back': '75',
            'goes_sutron_sensors': ['QWE1','QWE2'],
            'goes_units': 'm,m',
            'id': '5cebf1e23d0f4a073c4bc07c',
            'ip_address': '184.151.32.229',
            'ip_enabled': True,
            'iwls_environment': 'prod',
            'iwls_sensors': ['WL1','WL2','WL3'],
            'modem_enabled': True,
            'name': 'Tofino',
            'officialName': 'Tofino',
            'phone_number': '12507252641',
            'port': '8081',
            'region_header': 'PACIF',
            'script_variable_iwls': 'CONST IWLS_TimeSeries',
            'script_variable_sensor': 'CONST LoggerSensor',
            'sutron_sensors': ['QWE1','QWE2','PAROS']
            }  
        ...
        ]
    """ 
    global list_stations
    global list_regions

    # NOTE: ERROR CHECK ALL OF THIS !!!
    # Get the list of station dicts for key values queries 
    list_kv = get_additional_configurations(key, value, iwls_environment)
    # Get the list of stations - check if it already exists 
    if not list_stations:
        # Get the list here 
        list_stations = get_stations_list_private()

    if not list_regions:
        # Get the list here 
        list_regions = get_region_list()

    '''
    list_regions: 
    [
        {
            "id": "5ce598e3487b844868928224",
            "code": "ATL",
            "version": 1,
            "nameEn": "Atlantic",
            "nameFr": "Atlantique"
        },
        {
            "id": "5ce598e3487b844868928223",
            "code": "QUE",
            "version": 1,
            "nameEn": "Quebec",
            "nameFr": "Québec"
        },

        list_stations:
        {
            "id": "5cebf1dd3d0f4a073c4bb8f3",
            "code": "04780",
            "name": "Sand Head",
            "chsRegionId": "5ce598e3487b844868928222",
            "latitude": 51.416667,
            "longitude": -80.35,
            "type": "UNKNOWN"
        },
        {
    '''

    # Loop thru the list of key values 
    list_metadata = []
    for d in list_kv:
        station_code = d['stationCode']        
        kv = d['additionalConfigurations']

        # Get the station info 
        station = next((item for item in list_stations if item["code"] == station_code), None)
        if station is None:
            continue 

        # Get the region code         
        chsRegionId = station['chsRegionId']
        dict_region = next((item for item in list_regions if item["id"] == chsRegionId), None)
        if dict_region is None:
            continue

        chsRegionCode = dict_region['code']

        # Test if we filter on region 
        if region is not None:
            if chsRegionCode != region:
                continue

        # Create new dict - delete blank keys afterwards 
        m = {}
        m['id'] = station['id']
        m['chsRegionId'] = chsRegionId
        m['chsRegionCode'] = chsRegionCode
        m['code'] = station_code  
        m['officialName'] = station['name']
        m['name'] = station['name']  
        for key,value in kv.items(): 
            m[key] = value 

        # Change multivalue strings into a list 
        if 'iwls_sensors' in m: m['iwls_sensors'] = m['iwls_sensors'].split(',')
        if 'sutron_sensors' in m: m['sutron_sensors'] = m['sutron_sensors'].split(',')
        if 'goes_iwls_sensors' in m: m['goes_iwls_sensors'] = m['goes_iwls_sensors'].split(',')
        if 'goes_sutron_sensors' in m: m['goes_sutron_sensors'] = m['goes_sutron_sensors'].split(',')
        if 'goes_units' in m: m['goes_units'] = m['goes_units'].split(',')

        # Change True / False entries in boolean
        if 'ip_enabled' in m: m['ip_enabled'] = string_to_bool(m['ip_enabled'])
        if 'modem_enabled' in m: m['modem_enabled'] = string_to_bool(m['modem_enabled'])    
        if 'goes_enabled' in m: m['goes_enabled'] = string_to_bool(m['goes_enabled'])
        
        if 'operating' in m: m['operating'] = string_to_bool(m['operating'])

        # Now add any calculated keys 
        region_header = get_region_header(m['chsRegionCode'])
        m['region_header'] = region_header

        # NOTE: special case - backwards compatibility  
        if 'log_name' in m: m['xconnectlogfile'] = m['log_name']
        if 'ip_address' in m: m['ip'] = m['ip_address']    

        m = dict(sorted(m.items()))
        list_metadata.append(m)

    # Sort the list - just for fun 
    sorted_list = sorted(list_metadata, key=lambda x: x['name'])
    return sorted_list

def get_all_metadata_station(station_code: str) ->dict:
    """
    Gets all metadata for the station. 
    1. Get metadata object from IWLS 
    2. Get all key value pairs for station from IWLS 
    3. Merge them together into a dict 

    params: 
        station_code = five digit station identifier (string), code of the station i.e. 07120

    return:
        Metadata dict 
        Example:
        {'id': '5cebf1df3d0f4a073c4bbd1e'
        'code': '07120'
        'officialName': 'Victoria Harbour'
        'operating': True
        ....
        'goes_enabled': 'True'
        'modem_enabled': 'False'
        'phone_number': '12503633910,,*3'
        'user_login': None
        'user_pass': None
        'sutron_sensors': ['QWE1','QWE2','FTS81-1']
        'iwls_sensors': ['WL1','WL2','WL3']
        'goes_minutes_back': '45'
        'data_logger': '8310'
        'iwls_environment': 'prod'
        'basic_file_name': '07120_DL1_MEAS_IPtoIWLS.bas;07120_DL2_MEAS_IPtoIWLS.bas'
        'script_variable_sensor': 'CONST LoggerSensor'
        'script_variable_iwls': 'CONST IWLS_TimeSeries'
        'ip_address': '184.151.32.86'
        'port': '8081'
        'ip_enabled': 'True'
        'region_header': 'PACIF'}

    """       
    # Get the data from the private api 
    m = get_station_metadata(station_code)
    if m is None:
        return None
    
    # Presume we have a good metadata object - now get all keys 
    key_value = get_station_keys(station_code, iwls_environment='prod')

    # Append the dictionaries 
    m.update(key_value)

    # Create our complete metadata object which includes 
    # the IWLS metadata and all of our custom keys
    # station_metadata = StationMetaData()
    # for key,value in metadata.items(): 
    #     setattr(station_metadata, key, value)   

    # 2025-02-06
    # Some keys are different between public and private so lets add 
    # what keys that we can     
    m['officialName'] = m['name']
    m['operating'] = m['active']

    # Using the private api's generate no region code thus we need to look it up 
    chsRegionCode = get_region_private(station_code)
    m['chsRegionCode'] = chsRegionCode

    # Change multivalue strings into a list 
    if 'iwls_sensors' in m: m['iwls_sensors'] = m['iwls_sensors'].split(',')
    if 'sutron_sensors' in m: m['sutron_sensors'] = m['sutron_sensors'].split(',')
    if 'goes_iwls_sensors' in m: m['goes_iwls_sensors'] = m['goes_iwls_sensors'].split(',')
    if 'goes_sutron_sensors' in m: m['goes_sutron_sensors'] = m['goes_sutron_sensors'].split(',')
    if 'goes_units' in m: m['goes_units'] = m['goes_units'].split(',')    

    # Change True / False entries in boolean
    if 'ip_enabled' in m: m['ip_enabled'] = string_to_bool(m['ip_enabled'])
    if 'modem_enabled' in m: m['modem_enabled'] = string_to_bool(m['modem_enabled'])    
    if 'goes_enabled' in m: m['goes_enabled'] = string_to_bool(m['goes_enabled'])

    # Now add any calculated keys 
    region_header = get_region_header(m['chsRegionCode'])
    m['region_header'] = region_header

    # NOTE: special case - backwards compatibility  
    if 'log_name' in m: m['xconnectlogfile'] = m['log_name']
    if 'ip_address' in m: m['ip'] = m['ip_address']    
    m = dict(sorted(m.items()))

    return m

def get_region_list():
    '''
    Gets the list of regions dict 
    i.e.
    [
        {
            "id": "5ce598e3487b844868928224",
            "code": "ATL",
            "version": 1,
            "nameEn": "Atlantic",
            "nameFr": "Atlantique"
        },
        {
            "id": "5ce598e3487b844868928223",
            "code": "QUE",
            "version": 1,
            "nameEn": "Quebec",
            "nameFr": "Québec"
        },
        ...
    '''
    r = s.get(url=private_chs_regions)
    data_json = r.json()
    return data_json

def get_region_header(chsRegionCode: str) ->str:
    region_map =  {'PAC':'PACIF','CNA':'CTRAR','QUE':'QUE','ATL':'ATLAN'}
    try:
        region_header = region_map[chsRegionCode]
    except KeyError:
        region_header = ''
    return region_header

def string_to_bool(s: str) -> bool:
    s = s.lower()
    if s == 'true':
        return True
    elif s == 'false':
        return False
    else:
        return False
    
def put_metadata_to_xml():
    pass

# ---------------------------------------------------------------------------------------
# The 3 methods below added by Mike 2025-01-24
# Mainly used to get station id from station code when data 
# is not disseminated via the public api 
# We need to use the private api 
# ---------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------
# Added by Mike - 2023-11-23
# On behalf of DaveR - in order getDatumTargets.py uses this api 
# ---------------------------------------------------------------------------------------
def get_station_heights(station_code):
    """
    Return a json response from the IWLS API containing the heights for a single station

    param:
        station_code = five digit station identifier (string)
    """
    
    station_id = get_station_id_private(station_code)
    if station_id is None:
        return None

    url = private_station_url + station_id + '/heights'

    params = {}
    r = s.get(url=url, params=params)
    metadata_json = r.json()
    return metadata_json

def get_station_private(station_code):
    """
    Get metadata for station code via private api 
    return:
        Pandas dataframe containing metadata for station
    """
    # Private API has no way to get the station id from the station code 
    # thus, very inefficiently, we have to get all stations and then filter to get the 
    # station id 
    url = private_station_url
    r = s.get(url=url)
    data_json = r.json()
    df = pd.DataFrame.from_dict(data_json)

    #The above code seems to ignore the 'code' parameter. Check the size of the returned dataframe
    #take additional steps if needed.
    if df.size > 8:
        Station_of_interest = df[df["code"] == station_code]
        return Station_of_interest
    else:
        return df
    
def get_station_id_private(station_code):
    """
    Get internal station id using the private api given the station code  
    return:
        Station ID i.e. code='07120' returns id=5cebf1df3d0f4a073c4bbd1e
    """    
    # Get the station object, and get the station id 
    station = get_station_private(station_code)
    if station.empty:
        return None
    else:
        station_id = station['id'].values[0]
        return station_id      

def get_region_private(station_code):
    station_id = get_station_id_private(station_code)
    url = f'{private_region_url}{station_id}'
    r = s.get(url=url)
    # Note: Response body not proper json format 
    # can't parse JSON.  Raw result: CNA
    # Thus we use the text property of the response object     
    return(r.text)

def get_additional_configurations(key, value, iwls_environment):
    """
    Get a list dict containing json data for stations with key value

    return:
        list containing json data 
    [
        {
            "stationId": "5cebf1de3d0f4a073c4bb94a",
            "stationCode": "07755",
            "additionalConfigurations": 
                {
                "modem_enabled": "False",
                "phone_number": null,
                "sutron_sensors": "QWE1,QWE2",
                "iwls_sensors": "WL1,WL2",
                "data_logger": "8310",
                "iwls_environment": "prod",
                "basic_file_name": "07755-DL1_MEAS_IPtoIWLS.bas",
                "script_variable_sensor": "CONST LoggerSensor",
                "script_variable_iwls": "CONST IWLS_TimeSeries",
                "ip_address": "174.90.250.126",
                "port": "8081",
                "ip_enabled": "True"
                }
        },
        {
            "stationId": "5cebf1de3d0f4a073c4bb94c",
            "stationCode": "07795",
            "additionalConfigurations": 
                {
                "modem_enabled": "False",
                "phone_number": "16049133426,,*3",
                ...
                }
        }
        ]        
    """
    session, base_url = get_session_auth(iwls_environment)
    base_url = base_url + '/rest/stations/additional-configurations/'
    params = {'key': key, 'value': value}
    r = session.get(url=base_url, params=params)
    data_json = r.json()
    return data_json

def get_logbook_categories():
    """
    return: a json response from the IWLS API containing the logbook categories
    [{'id': '62e186cf63e108d83a280a9e', 
        'nameEn': 'Daily Quality Control',
        'nameFr': 'Contrôle de qualité quotidien'},
      {'id': '62e186cf63e108d83a280a9f',
        'nameEn': 'Field Visit',
        'nameFr': 'Visite de terrain'},
      {'id': '62e186cf63e108d83a280aa0',
        'nameEn': 'Station status',
        'nameFr': 'État de station'},
      {'id': '62e186cf63e108d83a280aa1',
        'nameEn': 'Infrastructure',
        'nameFr': 'Infrastructure'},
      {'id': '66169d9f05ab16ccb71cf810',
        'nameEn': 'Data upload',
        'nameFr': 'Téléversement de données'},
      {'id': '6686f1f1ae71ece4cda9d4ab',
        'nameEn': 'Historic Event History from MEDS',
        'nameFr': 'Historique des Événements Historique de MEDS'}]
    
    """
    url = f'{private_base_url}logbook-categories'
    r = s.get(url=url)
    json = r.json()
    return json

def get_logbook_entries(station_code, iwls_environment):
    """
    Get a list dict containing json data for stations with key value

    return:
        list containing json data 

        DOCUMENT RETURN    
    """

    # TEMPORARY SOLUTION - USE BEARER TOKEN AS SWAGGER API 
    token = "hidden"
    station_id = get_station_id_private(station_code)
    session, base_url = get_session_auth(iwls_environment)
    session.headers.update({'Authorization': "Bearer " + token})
    url = f'{private_base_url}stations/{station_id}/logbook'
    r = session.get(url=url)
    data_json = r.json()
    return data_json