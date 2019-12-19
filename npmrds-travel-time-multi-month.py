# This file creates Time of Day Speed information from the NPMRDS INRIX Datasets
# It combines all the monthly datasets into one for analysis by year
# Created by Puget Sound Regional Council Staff
# July 2019

import pandas as pd
import time
import os
import shutil
import zipfile
import getpass
import sys
import ast

# Get the inputs passed from the system argument
analysis_months = ast.literal_eval(sys.argv[1])
analysis_year = ast.literal_eval(sys.argv[2])
vehicles = sys.argv[3]
input_percentile = float(sys.argv[4])

#define working folders
working_path = os.getcwd()
data_directory = 'Y:\\System_Performance\\travel-time\\downloads'
temp_path = os.path.join('c:\\Users',getpass.getuser(),'Downloads')

#create output folder on server
output_directory = os.path.join(working_path, 'output',analysis_months[0]+'-'+analysis_months[len(analysis_months)-1])
        
# Create the output directory for the speed results if it does not already exist
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

#Reference Files for use in Analysis
tmc_posted_speed_file = os.path.join(working_path,'tmc_posted_speed.csv')
tmc_exclusion_file = os.path.join(working_path,'tmc_exclusions.csv')

# Shapefile of TMC's for use in shapefile joining
tmc_shapefile = os.path.join(data_directory,'Washington','Washington.shp')
tmc_projection = os.path.join(data_directory,'Washington','Washington.prj')

# Percentile to be used for the Average Speed Calculation
low_spd = 5
high_spd = 90

# Flag to determine if csv file is output or not (yes or no)
output_csv = 'yes'

# Dictionary Defining the Start and End Times for the Periods of Analysis
time_of_day = {"TOD_Name":['5am','6am','7am','8am','9am','3pm','4pm','5pm','6pm','7pm'],
               "start_time":[5,6,7,8,9,15,16,17,18,19],
               "end_time":[5,6,7,8,9,15,16,17,18,19]
           }
                                                              
# Function to Return the Timeperiod Travel Time
def travel_time(tod, df_timeperiod, average_per):

    # Calculate average observed and reference speeds
    df_avg = df_timeperiod.groupby('Tmc').quantile(average_per)    
    df_avg = df_avg.reset_index()
  
    # Calculate the ratio of observed to reference speed 
    df_avg[tod+'_ratio'] = df_avg['speed'] / df_avg['posted']

    # Rename columns for cleaner output
    df_avg  = df_avg.rename(columns={'speed':tod+'_speed'})
 
    return df_avg

# The next section is the main body for script execution
start_of_production = time.time()

# Convert the Travel Time percentile to the appropiate value for a pandas calculation
speed_percentile = 1 - input_percentile

print ('Loading the Posted Speed Limit file into a Pandas Dataframe')
df_posted_spd = pd.read_csv(tmc_posted_speed_file)
keep_columns = ['Tmc','PostedSpeed']
df_posted_spd = df_posted_spd.loc[:,keep_columns]
df_posted_spd['PostedSpeed']=  df_posted_spd['PostedSpeed'].astype(float)

# loop over the year to be analyzed
for year in analysis_year:
    
    # Due to memory use, loop over time periods
    for x in range(0,len(time_of_day['TOD_Name'])):
        
        for months in analysis_months:     
            print ('Working on '+str(time_of_day['TOD_Name'][x])+' Travel Speed calculation for ' + months + ' ' + year + ' ' + vehicles + ' tmc records')

            # Copy the compressed files to the users download directory for quicker processing    
            if os.path.exists(temp_path + '\\' + months + year + vehicles+'.zip'):
                os.remove(temp_path + '\\' + months + year + vehicles+'.zip')
            
            shutil.copyfile(data_directory + '\\' + months + year + vehicles +'.zip', temp_path + '\\' + months + year + vehicles +'.zip')
            npmrds_archive = zipfile.ZipFile(temp_path + '\\' + months + year + vehicles +'.zip', 'r')
            npmrds_archive.extractall(temp_path)
            npmrds_archive.close()
            os.remove(temp_path + '\\' + months + year + vehicles +'.zip')

            #Reading in the unzipped data file
            data_file = os.path.join(temp_path + '\\' + months + year + vehicles +".csv") 
            tmc_id_file = os.path.join(temp_path + '\\' + 'TMC_Identification.csv')
            contents_file = os.path.join(temp_path + '\\' + 'Contents.txt')

            # Open the vehicle specific TMC Identification file and store in dataframe
            print ('Loading the ' + months + ' ' + year + ' ' + vehicles + ' TMC file into a Pandas Dataframe')
            df_working_tmc = pd.read_csv(tmc_id_file)
            df_working_tmc  = df_working_tmc.rename(columns={'tmc':'Tmc'}) 

            print ('Removing Excluded TMCs from the ' + months + ' ' + year + ' ' + vehicles + ' TMC file')
            df_exclude = pd.read_csv(tmc_exclusion_file)
            df_exclude_list = df_exclude['Tmc'].tolist()
            for removal_tmc in df_exclude_list:
                df_working_tmc = df_working_tmc[df_working_tmc.Tmc != removal_tmc]

            print ('Loading the ' + months + ' ' + year + ' ' + vehicles + ' Speed file into a Pandas Dataframe')
            df_current_spd = pd.read_csv(data_file)
            df_current_spd  = df_current_spd.rename(columns={'tmc_code':'Tmc'})
            df_current_spd['measurement_tstamp'] = pd.to_datetime(df_current_spd['measurement_tstamp'])

            print ('Trimming the ' + months + ' ' + year + ' ' + vehicles + ' Speed file to only inlcude '+str(time_of_day['TOD_Name'][x]))
            df_current_spd = df_current_spd[df_current_spd['measurement_tstamp'].dt.hour == time_of_day['start_time'][x]]
                       
            print ('Removing outliers from the ' + months + ' ' + year + ' ' + vehicles + ' Speed file')
            df_current_spd = df_current_spd[df_current_spd.speed > low_spd]
            df_current_spd = df_current_spd[df_current_spd.speed < high_spd]
            
            print ('Adding the posted speed limit to the ' + months + ' ' + year + ' ' + vehicles + ' TMC file')
            df_current_spd = pd.merge(df_current_spd, df_posted_spd, on='Tmc', suffixes=('_x','_y'), how='left')
            df_current_spd  = df_current_spd.rename(columns={'PostedSpeed':'posted'})

            print ('Removing unneccesary columns from the ' + months + ' ' + year + ' ' + vehicles + ' TMC file')
            keep_columns = ['Tmc','posted','speed']
            df_current_spd = df_current_spd.loc[:,keep_columns]

            print ('Appending the ' + months + ' ' + year + ' ' + vehicles + ' speed file to the previous months')
            if months == analysis_months[0]:
                df_working_spd = df_current_spd
                
            else: 
                df_working_spd = df_working_spd.append(df_current_spd,ignore_index=True)
                
            # Delete the uncompressed files
            print ('Deleting the temporary ' + months + ' ' + vehicles + ' working files')
            os.remove(data_file)
            os.remove(tmc_id_file)
            os.remove(contents_file)

        print ('Calculating Speed Ratios for ' + str(time_of_day['TOD_Name'][x])+ ' ' + year + ' ' + vehicles + ' tmcs')
        df_spd = travel_time(time_of_day['TOD_Name'][x], df_working_spd , speed_percentile)
        
        if x == 0:
            df_output = df_working_tmc
            df_output = pd.merge(df_output, df_spd, on='Tmc', suffixes=('_x','_y'), how='left')
                
        else: 
            df_output = pd.merge(df_output, df_spd, on='Tmc', suffixes=('_x','_y'), how='left')
    
    # Write out the vehicle specific dataframe to csv if desired
    if output_csv == 'yes':
        df_output.to_csv(os.path.join(output_directory, year + '_' + vehicles +'_tmc_'+str(int(input_percentile*100))+'th_percentile_speed.csv'),index=False)
    
end_of_production = time.time()
print ('The Total Time for all processes took', (end_of_production-start_of_production)/60, 'minutes to execute.')

exit()