'''
Description:
One time script for daily and monthly means and extremes.
Using https://tides.gc.ca/en/daily-means-monthly-means-and-extremes-tides to format
Some info on site is outdated so going off of a 2022 file.

File Version:
Version 1

Authors:
Evan James (evan.james@dfo-mpo.gc.ca)

additional contacts:
Khaleel Arfeen (khaleel.arfeen@dfo-mpo.gc.ca)
'''

import requests
import time

header_dict = {
    '10050': '      THUNDER BAY, ONTARIO             18320024825 8913      EST1',
    '10220': '      ROSSPORT, ONTARIO                18320024850 8731      EST1',
    '10750': '      MICHIPICOTEN, ONTARIO            18320024758 8454      EST1',
    '10920': '      GROS CAP, ONTARIO                18320024850 8731      EST1',
    '10980': '      SAULT STE MARIE, ONTARIO         18300024631 8422      EST1',
    '11010': '      SAULT STE MARIE, ONTARIO         17638024631 8421      EST1',
    '11070': '      THESSALON, ONTARIO               17600024615 8333      EST1',
    '11195': '      LITTLE CURRENT, ONTARIO          17600024559 8156      EST1',
    '11375': '      PARRY SOUND, ONTARIO             17600024520 8002      EST1',
    '11445': '      MIDLAND, ONTARIO                 17600024445 7953      EST1',
    '11500': '      COLLINGWOOD, ONTARIO             17600024431 8013      EST1',
    '11690': '      TOBERMORY, ONTARIO               17600024515 8140      EST1',
    '11860': '      GODERICH, ONTARIO                17600024345 8144      EST1',
    '11940': '      POINT EDWARD, ONTARIO            17565024259 8225      EST1',
    '11950': '      PORT LAMBTON, ONTARIO            17465024239 8230      EST1',
    '11965': '      BELLE RIVER, ONTARIO             17440024218 8243      EST1',
    '11995': '      AMHERSTBURG, ONTARIO             17387024209 8307      EST1',
    '12005': '      BAR POINT, ONTARIO               17350024204 8307      EST1',
    '12065': '      KINGSVILLE, ONTARIO              17350024202 8244      EST1',
    '12250': '      ERIEAU, ONTARIO                  17350024216 8155      EST1',
    '12400': '      PORT STANLEY, ONTARIO            17350024239 8113      EST1',
    '12710': '      PORT DOVER, ONTARIO              17350024247 8012      EST1',
    '12865': '      PORT COLBORNE, ONTARIO           17350024252 7915      EST1',
    '13030': '      PORT WELLER, ONTARIO              7420024314 7913      EST1',
    '13150': '      BURLINGTON, ONTARIO               7420024318 7948      EST1',
    '13320': '      TORONTO, ONTARIO                  7420024338 7923      EST1',
    '13590': '      COBOURG, ONTARIO                  7420024357 7810      EST1',
    '13988': '      KINGSTON, ONTARIO                 7420024413 7631      EST1',
    '14400': '      BROCKVILLE, ONTARIO               7395024435 7541      EST1',
    '14600': '      UPPER IROQUOIS, ONTARIO           7324024449 7519      EST1',
    '14602': '      LOWER IROQUOIS, ONTARIO           7318024450 7519      EST1',
    '14660': '      MORRISBURG, ONTARIO               7286424454 7512      EST1',
    '14805': '      LONG SAULT DAM                    7250024500 7452      EST1',
    '14870': '      CORNWALL, ONTARIO                 4640024501 7443      EST1',
    '14940': '      SUMMERSTOWN, ONTARIO              4624024504 7433      EST1',
    '15520': '      Montreal Jetee #1                  556024530 7333      EST1',
    '15540': '      Montreal rue Frontenac             534824532 7333      EST1',
    '15560': '      Varennes                           483624541 7327      EST1',
    '15780': '      Contrecoeur IOC                    436024550 7317      EST1',
    '15930': '      Sorel                              377524603 7307      EST1',
    '15975': '      Lac Saint-Pierre                   339024612 7254      EST1',
    '03360': '      Trois-Rivieres                     292624620 7232      EST1',
    '03365': '      Port-Saint-Francois                298724616 7237      EST1' 
}

# Year of data collected
year = 2024
# List of month numbers with leading zeroes
months = [i for i in range(1, 13)]
# List of days in each month for 2024
days_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


class Daily_Means_File:
    def __init__(self) -> None:
        self.daily_means_for_month = []
        self.station_url = 'https://api-iwls.dfo-mpo.gc.ca/api/v1/stations'
        self.make_file()
        
    #------------------------------------------------------------------------------------------------------------------------------------------

    def make_file(self) -> None:
        '''
        Writes information into the file correctly.
        '''
        #Create a file in the directory to write to
        with open('CORNWALL_DMF_2024.dat', 'w') as f:
            
            #Loop through every station
            for station_code in header_dict:
                
                #For every station, go through every month
                for month in months:
                    
                    #Header
                    f.write(f"- {station_code} {year}{month:02d}{header_dict[str(station_code)]}\n")
                    
                    #For every month, go through each day
                    for day in range(1,days_in_month[month-1]+1, 10): # num of days is dictated by the days_in_month list, we get index from month-1
                        
                        if day == 1:
                            #Get the daily mean for current 10-day period and station
                            station_id = self.get_station_id(station_code) # station id is a hex identifier used by the API
                            dailyMeans = self.get_daily_means(station_id, month, range(1, 11))
                            #Write the line into the file
                            f.write(f"5 {station_code} {year}{month:02d}0110      {dailyMeans}\n")
                        
                        elif day == 11:
                            #Get the daily mean for current 10-day period and station
                            dailyMeans = self.get_daily_means(station_id, month, range(11, 21))
                            #Write the line into the file
                            f.write(f"5 {station_code} {year}{month:02d}1120      {dailyMeans}\n")
                        
                        else:
                            #Get the daily mean for current 10-day period and station
                            dailyMeans = self.get_daily_means(station_id, month, range(21, days_in_month[month-1]+1))
                            #Write the line into the file
                            f.write(f"5 {station_code} {year}{month:02d}21{days_in_month[month-1]}{self.get_monthly_mean()}{dailyMeans}\n")
                            #Write the line with code 6 to the file
                            f.write(f"6 {station_code} {year}{month:02d}     9999999999999999999999 999 99\n")
                            break
        return
                        
    #------------------------------------------------------------------------------------------------------------------------------------------
    def get_station_id(self, station_code):
        """
        Get internal station id given the station code  
        return:
            Station ID i.e. code='07120' returns id=5cebf1df3d0f4a073c4bbd1e
        """    

        params = {'code':station_code}

        try:
            # get station ID from endpoint
            response = requests.get(url=self.station_url, params=params)
            
            if response.status_code == 200:
                #load data as json
                data = response.json()
                station_id = data[0].get('id')
                station_code = data[0].get('code')
                print(station_code)
                return station_id
            
            else: 
                print(f"Bad response getting station code, error code: {response.status_code}")
                return

        # problem, no data able to be gathered
        except Exception as e:
            print(f"Error: {e}")

    #------------------------------------------------------------------------------------------------------------------------------------------
    def get_daily_means(self, stationId: object, month: int, days: list) -> str:
        '''
        Gets the daily mean for the set of parameters passed in.
        '''
        final_string = ""
        
        for day in days:
            #To not hit endpoint limit
            time.sleep(2)
            #Params for calculate-daily-means-igld85 
            params = {
                    'stationId' : stationId, 
                    'from' : str(year) + str(f"-{month:02d}") + str(f"-{day:02d}"),
                    'to' :  str(year) + str(f"-{month:02d}") + str(f"-{day:02d}")
                    }
            
            try:
                #Get data from endpoint
                url = f'{self.station_url}/{params["stationId"]}/stats/calculate-daily-means-igld85'
                response = requests.get(url=url, params=params)

                if response.status_code == 200:
                    #load data as json
                    data = response.json()
                    print(data)
                
                else: 
                    print(f"Bad response getting daily mean, error code: {response.status_code}")
                    final_string += "99999"
                    continue

            #Problem, no data able to be gathered
            except Exception as e:
                print(f"Error: {e}")
                #Enter data as 9s per instructions
                final_string += "99999"
                continue    #Go to next iteration
            
            
            try:
                #Log daily mean for the monthly mean calculation
                self.daily_means_for_month.append(data[0]['dailyMean_IGLD85'])
                #Ensure daily mean is 5 chars long
                dm = "{:.2f}".format(data[0]['dailyMean_IGLD85']).replace('.', '')
                while len(dm) < 5:
                    dm = ' ' + dm
                #Concat to string that will be put in the file    
                final_string += dm
            
            #Problem with this data location specifically
            except Exception as e:
                print(f"Error: {e}. Data not included in monthly mean calculation")
                final_string += "99999"
            
        return final_string
    
  
    #------------------------------------------------------------------------------------------------------------------------------------------
    def get_monthly_mean(self) -> float:
        '''
        Calculates and returns the monthly mean based on daily means
        '''

        monthly_mean = 0

        #Monthly mean may only be calculated if there are 20 or more daily means
        if len(self.daily_means_for_month) < 20:
            print("Not enough daily means to calculate monthly mean")
            return 999999
        
        else:
            
            #Loop through the daily means
            for mean in self.daily_means_for_month:
                #Round to 2 decimal places
                mean = round(mean, 2)   #round by default is round to even
                #Add to total
                monthly_mean += mean
            
            #Store monthly mean in a var
            r = "{:.3f}".format(monthly_mean/len(self.daily_means_for_month)).replace('.', '')
            while len(r) < 6:
                r = ' ' + r
            #Reset daily means list for next month
            self.daily_means_for_month = []

            return r
    #------------------------------------------------------------------------------------------------------------------------------------------


#----------------------------------------------------------------------
if __name__=='__main__':
    instance = Daily_Means_File()

#----------------------------------------------------------------------