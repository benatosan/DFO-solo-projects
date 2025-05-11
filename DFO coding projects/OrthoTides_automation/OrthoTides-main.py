'''
Description:
Automation of Tide Levels on Orthos for pactides

File Version:
Version 1 - preliminary assessments and tests

Authors:
Evan James (evan.james@dfo-mpo.gc.ca)

additional contacts:
Mike Sheward (mike.sheward@dfo-mpo.gc.ca), Paul Scott (paul.scott@dfo-mpo.gc.ca), Khaleel Arfeen (khaleel.arfeen@dfo-mpo.gc.ca)
'''

#imports
from math import radians, sin, cos, sqrt, atan2
import utilities.IWLS_API_Tools as api
from datetime import datetime
import pandas as pd
import sys
import tkinter as tk
from tkinter import filedialog, messagebox
from threading import Thread

#For future iterations
import re

from warnings import simplefilter
simplefilter(action='ignore', category=FutureWarning)

#-------------------------------------------------------------------------------------------------------------------

#Global vars
stations = api.get_stations_list()

#-------------------------------------------------------------------------------------------------------------------

class TideCheck():
    '''
    Main class housing back end code for checking the water level of an orthophoto.
    '''
    def __init__(self):
        #Number of stations closest stations will provide
        self.num = 5
        self.source = ""

    #---------------------------------------------------------------------------------------------------------------

    def convert_to_dd(self, coord) -> float:
        '''
        Converts degree-minute-second to decimal degree coordinates or cardinal direction to numerical coordinates
        '''

        try:
            #if read in as a number type already
            if isinstance (coord, float) or isinstance(coord, int):
                return float(coord)
            
            #If inputted as deg-min-sec coords
            if '-' in coord[1:]:    #[1:] skips case where input coord has negative sign out front
                if any(direction in coord for direction in ['S', 'W']):
                    coord = coord[:-1]
                    degs, mins, secs = map(float, coord.split('-'))
                    return -float(degs) - float(mins)/60 - float(secs)/3600
                else:
                    coord = coord[:-1]
                    degs, mins, secs = map(float, coord.split('-'))
                    return float(degs) + float(mins)/60 + float(secs)/3600
            else:
                #If cardinal direction is S or W
                if any(direction in coord for direction in ['S', 'W']):
                    #pop the letter
                    coord = coord[:-1]
                    #turn negative for numerical coord
                    return -float(coord)
                #If cardinal direction N or E or
                elif any(direction in coord for direction in ['N', 'E']):
                    #pop letter
                    coord = coord[:-1]
                    #leave unchanged
                    return float(coord)
                #Already in numerical format
                else:
                    return float(coord)
        #Don't accept any other formats i.e: more than one letter, wrong letter, etc.
        except ValueError:
            print("Ensure coordinates are valid. Consult documentation for formatting guidelines")
            return None
            
    #---------------------------------------------------------------------------------------------------------------

    def convert_to_timestamp(self, date: str, time: str) -> str:
        '''
        Formats date and time provided from csv
        '''
        try:
            #Formatting to ISO 8601
            date = date.replace('/', '-')
            time = f"{time}:00Z"

            dateString = date + 'T' + time
            #correct date format
            dateFormat = "%Y-%m-%dT%H:%M:%SZ"

            #if the date matches proper format already
            if  self.is_valid_timestamp(dateString, dateFormat):
                dateDatetime = datetime.strptime(dateString, dateFormat)
            else:
                # Parse the date string into a datetime object
                dateDatetime = datetime.strptime(dateString, "%m-%d-%YT%H:%M:%SZ")
        except Exception as e:
            print(f"\nError: {e}\nLikely due to the CSV file being improperly formatted. Check the CSV file with the documentation.")    
        # Format the datetime object into a string
        return dateDatetime.strftime(dateFormat)
    #---------------------------------------------------------------------------------------------------------------

    def is_valid_timestamp(self, date: str, format: str) -> bool:
        '''
        Helper function to convert_to_timestamp()
        '''
        try:
            datetime.strptime(date, format)
            return True
        except ValueError:
            return False
    #---------------------------------------------------------------------------------------------------------------

    def get_closest_stations(self, lat: str, lon: str) -> list:
        '''
        Works with get_dist, returns the code of the closest station(s). Also validates coords given.
        '''
        #initialize a list to hold a few closeby stations
        closest = []   #list of all stations

        #Loop over all stations
        for station in stations:
            #distance to station
            dist = self.get_dist(lat, lon, station.latitude, station.longitude)
            #tuple containing station object and the distance to the station
            ele = (station, dist)
            #Case where list doesn't have 5 items yet
            if len(closest) < self.num:
                closest.append(ele)
            #If current station closer than furthest station in list replace it
            elif closest[-1][1] > dist:
                closest.pop()
                closest.append(ele)
            #Sort list by distance ascending to make sure list in correct order
            closest.sort(key=lambda x: x[1])
        return closest

    #---------------------------------------------------------------------------------------------------------------

    def get_data(self, stationCode: str, time: str) -> object:
        '''
        Primary communicator with IWLS API. Given station code, will grab water level at the given time.
        '''
        #Get wlo data according to parameters
        data = api.get_station_timeseries(stationCode, time_series_code= 'wlo', start_time=time, end_time=time)
        self.source = 'wlo'
        originalTime = time     #Save user inputted time
        #If no data immediately check nearby timestamp
        if data.empty:
            count = 0
            #Timestamps aren't more than 15 mins apart in IWLS
            while count <= 8:
                time = self.increment_time(time)
                data = api.get_station_timeseries(stationCode, time_series_code= 'wlo', start_time=time, end_time=time)
                self.source = 'wlo'
                print(f"time: {time}, data = {data}, source = {self.source}")
                if data.empty == False:
                    break
                count += 1
        #If no data in wlo, resort to wlp
        if data.empty:
            count = 0
            time = originalTime
            #Timestamps aren't more than 15 mins apart in IWLS
            while count <= 8:
                time = self.increment_time(time)
                data = api.get_station_timeseries(stationCode, time_series_code= 'wlp', start_time=time, end_time=time)
                self.source = 'wlp'
                print(f"time: {time}, data = {data}, source = {self.source}")
                if data.empty == False:
                    break
                count += 1
        #If still no data return "No data" as the value
        if data.empty:
            self.source = ""
            return "No data"
        #return just the numerical water level
        return data['value'].iloc[0]
    #---------------------------------------------------------------------------------------------------------------

    def increment_time(self, time) -> str:
        '''
        Loops until a timestamp with data is found or timeout occurs
        Needed since ortho timestamps can fall between IWLS data entries
        Note: Lowest data resolution is every 15 mins so will just look for closest 15 min interval

        example of time passed in:  2013-05-09T19:27:00Z
        '''
        #Splits the date and time portions of the timestamp
        time = time.split('T')
        date = time[0]
        editTime = time[1][:-4]
        #Change by 1 min
        hours, minutes = map(int, editTime.split(':'))
        #At multiple of 15 but no data
        if minutes % 15 == 0:
            pass
        #Decides whether closest multiple of 15 is up or down
        else:
            up = 15 - (minutes % 15)
            down = minutes % 15
            if up < down:
                minutes += 1
            else:
                minutes -= 1
        if minutes == 60:
            minutes = 0
            hours += 1
        if hours == 24:
            #instead of changing the date, just go backwards instead of incrementing
            hours = 23
            minutes = 45
        newTime = f"{hours:02d}:{minutes:02d}"
        time = f"{date}T{newTime}:00Z"

        return time
    #---------------------------------------------------------------------------------------------------------------

    def get_dist(self, lat1: float, long1: float, lat2: float, long2: float) -> float:
        '''
        Calculates distance in km between 2 decimal degree coordinates using the Haversine formula
            Error margine depicted in documentation, fine for regional distances.
        '''
        #Radius of earth in km
        R = 6371.0

        lat1 = self.convert_to_dd(lat1)  #convert lat to decimal degree
        long1 = self.convert_to_dd(long1)  #convert lon to decimal degree

        lat2 = self.convert_to_dd(lat2)  #convert lat to decimal degree
        long2 = self.convert_to_dd(long2)  #convert lon to decimal degree

        #Convert deg to rad
        lat1 = radians(lat1)
        lat2 = radians(lat2)
        long1 = radians(long1)
        long2 = radians(long2)

        #Diffs
        dlong = long2 - long1
        dlat = lat2 - lat1

        #Haversine formula
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlong/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))

        return R * c
    #---------------------------------------------------------------------------------------------------------------

    def write_to_csv(self, file) -> None:
        '''
        Writes water level, distance, station name, and a modified file name to csv in correct location
        '''

        #Open csv file
        try:
            f = pd.read_csv(file)
        except FileNotFoundError:
            print("CSV file not found")

        #Initialize iterator
        rowidx = 0
        #Catches if trying to access data that doesn't exist
        try:
            print("Querying data... please wait...")   #User info
            while len(f) > rowidx:  #Iterrate through csv file
                #Read in all necessary data for use
                ortho = f.loc[rowidx, 'Ortho file name']
                date = f.loc[rowidx, 'Date']
                time = f.loc[rowidx, 'hh:mm UTC']
                lat = f.loc[rowidx, 'Lat']
                lon = f.loc[rowidx, 'Long']
                stationCode = f.loc[rowidx, 'Station Code']

                #For efficiency create a dictionary that will be turned into a dataframe
                #Otherwise pandas copies and replaces existing dataframe each time
                tempDict = {
                    'Station Name' : [],
                    'Water Level' : [],
                    'Data Source' : [],
                    'Station Distance' : [],
                    'New Ortho Name' : []
                }
            
                #Case where user has put a station code in the csv, skip finding closest station
                if not pd.isna(stationCode):
                    #if stationCode is read in as str, convert it to float for rounding
                    if isinstance(stationCode, str):
                        try:
                            stationCode = float(stationCode)
                        except ValueError:
                            print("Ensure the station code is a 5-digit code")
                    #give station code proper formatting
                    stationCode = str(round(stationCode))
                    #if leading 0 is needed
                    if len(stationCode) < 5:
                        while len(stationCode) < 5:
                            stationCode = "0" + stationCode

                    print(f"Station ID provided: {stationCode} for ortho {ortho}")    #For user info

                    station = api.get_station(stationCode)  #load station, note this is a dataframe not a class object like you get with finding closest station
                    if station.empty:   #If the station code provided does not match a station in PAC Region
                        print(f"Station ID {stationCode} is invalid")
                        return
                    
                    #Load dictionary
                    tempDict['Station Name'].append(f"{station['officialName'][0]}, {station['code'][0]}")
                    wLevel = self.get_data(stationCode, self.convert_to_timestamp(date, time))
                    tempDict['Water Level'].append(f"{wLevel} m")
                    tempDict['Data Source'].append(self.source)
                    #Case if station code is provided but no coordinates
                    if not pd.isnull(lat) and not pd.isnull(lon):
                        tempDict['Station Distance'].append(f"{round(self.get_dist(lat, lon, station['latitude'][0], station['longitude'][0]), 3)} km")
                    else:
                        tempDict['Station Distance'].append("")
                    tempDict['New Ortho Name'].append(str(ortho) + str(wLevel))

                    #dataframe created with the dictionary we just made
                    df = pd.DataFrame(tempDict)
                    #put all the info next to and underneath the orthophoto it's related to
                    #Ignore index = True is important otherwise multiple rows have same index
                    f = pd.concat([f.iloc[:rowidx+1], df, f.iloc[rowidx+1:]], ignore_index=True)
                    #iterate row index to look at the next line of provided data after file has been altered
                    rowidx += 2

                #Case where no station code has been inputted
                else:
                    #Get a list of closest stations (list of tuples: (station: class, dist: float))
                    closestList = self.get_closest_stations(lat, lon)
                    
                    #Loop through tuples
                    for element in closestList:
                        #Same process as if there was a stationCode except element[0] is a station class obj
                        tempDict['Station Name'].append(f"{element[0].officialName}, {element[0].code}")
                        wLevel = self.get_data(element[0].code, self.convert_to_timestamp(date, time))
                        tempDict['Water Level'].append(f"{wLevel} m")
                        tempDict['Data Source'].append(self.source)
                        tempDict['Station Distance'].append(f"{round(element[1], 3)} km")
                        tempDict['New Ortho Name'].append(str(ortho) + str(wLevel))

                    #dataframe created with the dictionary we just made
                    df = pd.DataFrame(tempDict)
                    #put all the info next to and underneath the orthophoto it's related to
                    f = pd.concat([f.iloc[:rowidx+1], df, f.iloc[rowidx+1:]], ignore_index=True)
                    #Iterate to next line of provided data based on how many stations have been provided
                    rowidx += self.num+1

            #Write the modified dataframe to the csv, not including index
            print("Writing to csv...")
            f.to_csv(file, index = False)
          
        except KeyError:
            print("Could not find data header, reference documentation and ensure correct column header names")
        except PermissionError:
            print("Permission to edit CSV denied, make sure your CSV file is not open.")

        return
    #---------------------------------------------------------------------------------------------------------------
    def output_data(self, lat: float, lon: float, time: str, stationCode: str) -> None:
        '''
        Outputs data directly to the screen if user uses the single input option
        '''

        print(f"Coordinates enterred: {lat}, {lon}")
        #if the timestamp is not provided in correct format
        if not self.is_valid_timestamp(time, "%Y-%m-%dT%H:%M:%SZ"):
            print("Ensure time is correctly formatted")
            return
        #If stationCode is provided, skip finding closest
        if stationCode is not None:
            #if stationCode is read in as str
            if isinstance(stationCode, str):
                try:
                    stationCode = float(stationCode)
                except ValueError:
                    print("Ensure the station code is a 5-digit code")
            #give station code proper formatting
            stationCode = str(round(stationCode))
            #if leading 0 is needed
            if len(stationCode) < 5:
                while len(stationCode) < 5:
                    stationCode = "0" + stationCode

            print(f"Station ID provided: {stationCode}")    #For user info

            station = api.get_station(stationCode)  #load station, note this is a dataframe not a class object like you get with finding closest station

            if station.empty:   #If the station code provided does not match a station in PAC Region
                print(f"Station code {stationCode} is not the code for an existing station")
                return
            
            print(f"Station name: {station['officialName'][0]}")
            wLevel = self.get_data(stationCode, time)
            print(f"Station water level ({self.source}): {wLevel} m")
            if lat != "" and lon != "":
                print(f"Distance to station: {round(self.get_dist(lat, lon, station['latitude'][0], station['longitude'][0]), 3)} km")
            return
        else: #IDE says code won't reach this, it is incorrect
            tempDict = {
                'Station Name' : [],
                'Water Level' : [],
                'Data Source' : [],
                'Station Distance' : []
            }
            #Get a list of closest stations (list of tuples: (station: class, dist: int))
            closestList = self.get_closest_stations(lat, lon)
            
            #Loop through tuples
            for element in closestList:
                #Same process as if there was a stationCode except element[0] is a station class obj
                tempDict['Station Name'].append(f"{element[0].officialName}, {element[0].code}")
                wLevel = self.get_data(element[0].code, time)
                tempDict['Water Level'].append(f"{wLevel} m")
                tempDict['Data Source'].append(self.source)
                tempDict['Station Distance'].append(f"{round(element[1], 3)} km")

            #dataframe created with the dictionary we just made
            df = pd.DataFrame(tempDict)
            print(df)   #Print dataframe for readability
            return

    #---------------------------------------------------------------------------------------------------------------
    #Run functions
    #----------------------------------------------------------------------------------------------------------------

    def run(self, csvFlag: bool, singleFlag: bool, csv, lat: str, lon: str, time: str, stationCode: str):
        '''
        Given a flag indicating whether a csv or single coord has been inputted, run the program accordingly
        '''
        if csvFlag:
            self.write_to_csv(csv)
        elif singleFlag:
            self.output_data(lat, lon, time, stationCode)
        #Case where no data has been given and therefore no flag raised
        else:
            print("Please attach a csv file or enter coordinates and a timestamp")
            return


    def threaded_run(self, csvFlag: bool, singleFlag: bool, csv, lat: str, lon: str, time: str, stationCode: str) -> None:
        '''
        run() is too long for tkinter, start run on diff thread so root.mainloop() can go
        '''
        def run_in_thread():
            '''
            Runs a run() thread and prints final msg in mainloop thread since tkinter not thread safe.
            '''
            self.run(csvFlag, singleFlag, csv, lat, lon, time, stationCode)
            #final msg to tkinter in mainloop thread to avoid runtime error
            if csvFlag:
                self.root.after(0, lambda: print("CSV file updated"))
            else:
                self.root.after(0, lambda: print("complete"))
        Thread(target=run_in_thread).start()

#---------------------------------------------------------------------------------------------------------------
    '''
    CODE FOR NEXT ITERATION, not implemented in code yet
    '''
    def get_timestamp(s: str) -> str:
        '''
        Find and format a timestamp from the ortho filename
        '''
        dateFormat = "%Y-%m-%dT%H:%M:%SZ"
        #pattern for: YYYY-MM-DD_hh-mm (found in most ortho files)
        pattern = r'\b(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})'
        matches = re.findall(pattern, s)

        #Should only find one
        if len(matches) > 1:
            print("Error reading in timestamp from ortho file name")
            return None
        else:
            time = f"{matches[0]}"
        
        timestamp = datetime.strptime(time, '%Y-%m-%d_%H-%M')
        return timestamp

        


#---------------------------------------------------------------------------------------------------------------
        
    

#---------------------------------------------------------------------------------------------------------------

class tkint(TideCheck):
    '''
    Child class of TideCheck holds all tkinter UI code.
    '''
    class IO_redirector(object):
        #Sets its own textArea
        def __init__(self, textArea):
            self.textArea = textArea
    
    class stdout_redirect(IO_redirector):
        #Inits IO_redirector and writes to textArea
        def write(self, str):
            self.textArea.after(0, self.textArea.insert, tk.END, str)

    def __init__(self):
        super().__init__()
        self.root = tk.Tk()
        self.csvFlag = False
        self.singleFlag = False
        self.csv = None
        self.lat_val = ""
        self.lon_val = ""
        self.time_val = ""
        self.code_val = ""
        self.interface()
    #--------------------------------------------------------------------------------------------------

    def on_close(self):
        '''
        Resets stdout and closes tkinter window
        '''
        sys.stdout = sys.__stdout__
        self.root.destroy()
        sys.exit()
    #--------------------------------------------------------------------------------------------------

    def retrieve_entry(self):
        '''
        Grabs and assigns vars from entry fields
        '''
        self.lat_val = self.lat_input.get()
        self.lon_val = self.lon_input.get()
        self.time_val = self.time_input.get()
        self.code_val = self.code_input.get()
        if self.code_val == "":
            self.code_val = None

    #--------------------------------------------------------------------------------------------------

    def get_option(self, *args) -> None:
        '''
        Gets choice from dropdown and greys out corresponding buttons/fields
        '''
        self.op = self.tkvar.get()
        #if the single input option is chosen
        if self.op == 'Single Input':
            self.csv_select_button.config(state='disabled')
            self.lat_input.config(state='normal')
            self.lon_input.config(state='normal')
            self.time_input.config(state='normal')
            self.code_input.config(state='normal')
        #if csv option is chosen
        elif self.op == 'CSV':
            self.lat_input.config(state='disabled')
            self.lon_input.config(state='disabled')
            self.time_input.config(state='disabled')
            self.code_input.config(state='disabled')
            self.csv_select_button.config(state='normal')
        #Because default will pass, decide_flag() catches if tries to run program w this option
        elif self.op == '-':
            self.csv_select_button.config(state='disabled')
            self.lat_input.config(state='disabled')
            self.lon_input.config(state='disabled')
            self.time_input.config(state='disabled')
            self.code_input.config(state='disabled')
        else:
            print("Please select a method from the dropdown menu")

    #--------------------------------------------------------------------------------------------------

    def decide_flag(self) -> None:
        '''
        Returns whether a csv or single input has been enterred
        '''
        #Follows logic of priority

        #First checks if a method has been chosen:
        if self.op == '-':
            print("Please select a method from the dropdown menu")
        #Checks if station code has been enterred
        if self.code_val is not None and self.op == 'Single Input':
            self.singleFlag = True
            self.csvFlag = False
            return
        #Then checks for a csv file
        elif self.csv is not None and self.op == 'CSV':
            self.csvFlag = True
            self.singleFlag = False
            return
        #Then makes sure that coords and timestamp have been input
        elif not any(value == "" for value in [self.lat_val, self.lon_val, self.time_val]) and self.op == 'Single Input':
            self.singleFlag = True
            self.csvFlag = False 
            return 
        #If no input has been provided, neither flag raised
        else:
            self.singleFlag = False
            self.csvFlag = False
            return
        
    #--------------------------------------------------------------------------------------------------
    #--------------------------------------------------------------------------------------------------

    def browseFiles(self):
        #Function to browse files in the file explorer, used to select xml file
        filename = filedialog.askopenfilename(initialdir = "/", title = "Select a File")

        if filename.endswith('.csv'):
            self.csv = filename
            self.csv_selection_label.configure(text=filename,font = "verdana 6",wraplength=110)
        else:
            messagebox.showerror("Error", "Selected file is not a csv")
            self.csv = None
            self.csv_selection_label.configure(text="")

    def interface(self):
        '''
        Runs interface for tkinter GUI
        '''
        #Title label
        label = tk.Label(self.root, text = "Select a method from the dropdown menu below", anchor = 'center')
        label.grid(row=0, column=0, columnspan = 2)

        #Create csv file input, place it in a frame for formatting
        csvFrame = tk.Frame(self.root)
        csvFrame.grid(row = 3, column = 1, rowspan = 2)
        #Label for csv button
        csv_label = tk.Label(csvFrame, text = 'Select csv file')
        #Displays file path once selected
        self.csv_selection_label = tk.Label(csvFrame, text = self.csv if self.csv else "")
        self.csv_select_button = tk.Button(csvFrame,text="Select File", command = self.browseFiles)

        #Placing csv labels and buttons
        csv_label.grid(row=2,column=1)
        self.csv_select_button.grid(row = 3, column = 1)
        self.csv_selection_label.grid(row = 4, column = 1)

        #Formatting for latitude button
        lat_label = tk.Label(self.root, text="Latitude")
        lat_label.grid(row = 2, column = 0)
        self.lat_input = tk.Entry(self.root)
        self.lat_input.grid(row = 3, column = 0)

        #Formatting for longitude button
        lon_label = tk.Label(self.root, text="Longitude")
        lon_label.grid(row = 4, column = 0)
        self.lon_input = tk.Entry(self.root)
        self.lon_input.grid(row = 5, column = 0)

        #Formatting for timestamp button
        time_label = tk.Label(self.root, text="Time, ISO 8601 format (yyyy-mm-ddThh:mm:ssZ)")
        time_label.grid(row = 6, column = 0)
        self.time_input = tk.Entry(self.root)
        self.time_input.grid(row = 7, column = 0)

        #Formatting for Station code button
        code_label = tk.Label(self.root, text="5 digit Station Code (optional)")
        code_label.grid(row = 8, column = 0)
        self.code_input = tk.Entry(self.root)
        self.code_input.grid(row = 9, column = 0)

        #Formatting for method dropdown menu
        #Var to hold selection
        self.tkvar = tk.StringVar(self.root)
        #Trace to check for any changes to the dropdown menu
        self.tkvar.trace_add(mode= 'write', callback=self.get_option)
        #List of options:
        choices = {'-', 'Single Input', 'CSV'}
        self.tkvar.set('-') #Set default
        #Create dropdown
        self.dropdown = tk.OptionMenu(self.root, self.tkvar, *choices)
        self.dropdown.grid(row = 1, column=0, columnspan=2)

        #RUN button runs the program
        #Lambda here: retrieves entries in fields, decides whether csv or single input is used, calls threaded_run from TideCheck class
        runButton = tk.Button(self.root, text="RUN", command=lambda: [self.retrieve_entry(), self.decide_flag(), super(tkint, self).threaded_run(self.csvFlag, self.singleFlag, self.csv, self.lat_val, self.lon_val, self.time_val, self.code_val)])
        runButton.grid(row = 10, column = 0, columnspan=2)
        
        #defining textbox
        outputBox = tk.Text(self.root, height = 10, width = 80)
        outputBox.grid(row = 11, column=0, columnspan = 2)
        
        #outputBox is IO_redirector, stdout becomes the textArea so print() -> outputBox
        sys.stdout = tkint.stdout_redirect(outputBox)
        # Set the window close protocol
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)   
        #Mainloop
        self.root.mainloop()

#Driver code
tkint()