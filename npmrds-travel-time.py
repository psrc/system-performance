# This file creates Time of Day Speed information from the NPMRDS INRIX Datasets
# Created by Puget Sound Regional Council Staff

import pandas as pd
import time
import geopandas as gp
import os
import shutil
import zipfile
import getpass
import datetime as dt

# Year to Analyze in a 4 digit format
analysis_year = '2018'

# List of Months to analyze using the two digit integer code
analysis_months = ['jan','feb','mar']

# Path to the System Performance Data on the server
working_path = 'c:/System_Performance/travel-time/'
#working_path = '//file2/Projects/System_Performance/travel-time/'
data_directory = working_path + 'downloads/'
temp_path = os.path.join('c:\\Users',getpass.getuser(),'Downloads')
                       
# Shapefile of TMC's for use in shapefile joining
tmc_shapefile = working_path + 'reference/shapefiles/Washington.shp' 
tmc_projection = working_path + 'reference/shapefiles/Washington.prj' 

# Reference Files for use in Analysis
tmc_posted_speed_file = working_path + 'reference/tmc_posted_speed.csv' 
tmc_exclusion_file = working_path + 'reference/tmc_exclusions.csv' 

# Percentile to be used for the Average Speed Calculation
speed_percentile = 0.50
low_spd = 10
high_spd = 75

# Congestion Thresholds
moderate_congestion = 0.75
heavy_congestion = 0.50
severe_congestion = 0.25

# Flag to determine if csv file is output or not (yes or no)
output_csv = 'yes'

# Flag to determine if a csv file for a time series animation in ArcMap is desired yes or no
output_timeseries = 'yes'

# Hours to summarize in the output file with time increments of 15, 30 or 60 minutes
first_hour = 5
last_hour = 22
time_increment = 30

# Create time of day list to iterate over
analysis_time = []

for x in range(first_hour,last_hour+1):
   
    analysis_time.append(str(x)+':00')
    
    for increments in range(1, 60/time_increment):
        analysis_time.append(str(x) + ':' + str(increments * time_increment))

analysis_time = [dt.datetime.strptime(x, '%H:%M') for x in analysis_time]
           
# List of Vehicle Type to analyze
vehicle_types = ['cars']

# Create the output directory
if len(analysis_months) == 1:
    period = analysis_months[0]

else: period = analysis_months[0] + '-' + analysis_months[-1]

output_directory = working_path + '/output/' + period + '-' + analysis_year

# Create the output directory for the speed results if it does not already exist
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Function to trim the dataframe to the time period of interest
def time_period(df_region, current_period, next_period):
    
    start_hour = current_period.hour
    end_hour = next_period.hour
        
    start_minute = current_period.minute
    end_minute = next_period.minute
           
    # Trim the dataframe to inlcude the entire hour for the current period
    df_region = df_region[df_region['measurement_tstamp'].dt.hour == start_hour]
    
    # Trim down the hourly frame for the minutes of interest 
    if end_hour == start_hour:
        df_region = df_region[df_region['measurement_tstamp'].dt.minute < end_minute]
        df_region = df_region[df_region['measurement_tstamp'].dt.minute >= start_minute]
    
    else:
        df_region = df_region[df_region['measurement_tstamp'].dt.minute >= start_minute]
    
    df_region['time']= str(start_hour) + ':' + str(start_minute)
            
    return df_region
                                                                   
# Function to Return the Timeperiod Travel Time
def calculate_speed(df_timeperiod, average_per):

    # Calculate average observed and reference speeds
    df_avg = df_timeperiod.groupby('Tmc').quantile(average_per)    
    df_avg = df_avg.reset_index()
    
    # Calculate the ratio of observed to reference speed 
    df_avg['ratio'] = df_avg['speed'] / df_avg['posted']
    df_avg  = df_avg.drop(columns=['posted'])
        
    return df_avg

def gp_table_join(update_table,join_shapefile):
    
    # open join shapefile as a geodataframe
    join_layer = gp.GeoDataFrame.from_file(join_shapefile)

    # table join
    merged = join_layer.merge(update_table, on='Tmc')
    
    return merged

# Function to create summary file
def results_output_summary(working_df, working_file, vehicle_type, results_directory, time_period, moderate, heavy, severe, total_tmc):
      
    all_tmc = float(total_tmc)
    total_tmc_data = float(len(working_df))
    percent_tmc_data = (round((total_tmc_data / all_tmc),2))*100
                       
    segments_moderately_congested = float(len(working_df[(working_df[time_period+'_ratio']<=moderate)]))
    percent_moderately_congested = (round((segments_moderately_congested / total_tmc_data),2))*100

    segments_heavily_congested = float(len(working_df[(working_df[time_period+'_ratio']<=heavy)]))
    percent_heavily_congested = (round((segments_heavily_congested / total_tmc_data),2))*100
    
    segments_severely_congested = float(len(working_df[(working_df[time_period+'_ratio']<=severe)]))
    percent_severely_congested = (round((segments_severely_congested / total_tmc_data),2))*100
    
    working_file.write(' ' + '\n')
    working_file.write('Summary of Data for: ' + time_period + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments with data: ' + str(total_tmc_data) + '\n')  
    working_file.write('  --- % of Total TMC segments with data: ' + str(percent_tmc_data) + '%' + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments under 75% of the posted speed: ' + str(segments_moderately_congested) + '\n')
    working_file.write('  --- % of TMC segments under 75% of the posted speed: ' + str(percent_moderately_congested) + '%' + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments under 50% of the posted speed: ' + str(segments_heavily_congested) + '\n')
    working_file.write('  --- % of TMC segments under 50% of the posted speed: ' + str(percent_heavily_congested) + '%' + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments under 25% of the posted speed: ' + str(segments_severely_congested) + '\n')
    working_file.write('  --- % of TMC segments under 25% of the posted speed: ' + str(percent_severely_congested) + '%' + '\n')
    working_file.write(' ' + '\n')

    return working_file

start_of_production = time.time()

# Open a dataframe with posted speed limits in it by TMC
posted_df = pd.read_csv(tmc_posted_speed_file)
keep_columns = ['Tmc','PostedSpeed']
posted_df = posted_df.loc[:,keep_columns]
posted_df['PostedSpeed']=  posted_df['PostedSpeed'].astype(float)

# Create a list of tmc's to be excluded 
exclude_df = pd.read_csv(tmc_exclusion_file)
exclude_list = exclude_df['Tmc'].tolist()

# Loop over the vehicle types (cars and trucks) for analysis
for vehicles in vehicle_types:

    # Create a dataframe from the monthly speed files
    for months in analysis_months:
        print 'Creating the dataframes of speed files'
        print 'Currently working on the '+ months +' data files.'
   
        # Copy the compressed files to the users download directory for quicker processing
        print 'Copying the temporary ' + months + ' ' + vehicles + ' zip file to the downloads directory for faster analysis.'
        shutil.copyfile(data_directory + months + analysis_year+vehicles+'.zip', temp_path + '\\' + months + analysis_year+vehicles+'.zip')
        
        # Uncompress files for use in analysis and then remove the temporary archive file
        print 'Uncompressing the temporary ' + months + ' ' + vehicles + ' working files for analysis.'
        npmrds_archive = zipfile.ZipFile(temp_path + '\\' + months + analysis_year+vehicles+'.zip', 'r')
        npmrds_archive.extractall(temp_path)
        npmrds_archive.close()
        os.remove(temp_path + '\\' + months + analysis_year+vehicles+'.zip')

        data_file = temp_path + '\\' + months + analysis_year + vehicles +'.csv'      
        tmc_file = temp_path + '\\' + 'TMC_Identification.csv'
        contents_file = temp_path + '\\' + 'Contents.txt'

        # Open the vehicle specific TMC speed file and store in dataframe
        print 'Loading the ' + months + ' ' + vehicles + ' Speed file into a Pandas Dataframe'
        df_working_spd = pd.read_csv(data_file)
        keep_columns = ['tmc_code','measurement_tstamp','speed','travel_time_seconds']
        df_working_spd = df_working_spd.loc[:,keep_columns]
        df_working_spd  = df_working_spd.rename(columns={'tmc_code':'Tmc','travel_time_seconds':'travel_time'}) 

        # Add the posted speed limit to the TMC Speed file
        print 'Adding the posted speed limit to the  ' + months + ' ' + vehicles + ' Speed file'
        df_working_spd = pd.merge(df_working_spd, posted_df, on='Tmc', suffixes=('_x','_y'), how='left')
        df_working_spd  = df_working_spd.rename(columns={'PostedSpeed':'posted'})

        # Remove tmc's with outliers in the observed speed 
        df_working_spd = df_working_spd[df_working_spd.speed > low_spd]
        df_working_spd = df_working_spd[df_working_spd.speed < high_spd]

        # Convert the Time Stamp column to a pandas datetime format 
        df_working_spd.measurement_tstamp = pd.to_datetime(df_working_spd.measurement_tstamp)
    
        # Append the Currently monthly speed file to the previous
        if months == analysis_months[0]:
            df_analysis = df_working_spd
                
        else: 
            df_analysis = df_analysis.append(df_working_spd,ignore_index=True) 

        os.remove(data_file)
        os.remove(contents_file)    

    # Create the text file for summary output
    summary_report = open(output_directory + '/' + period + analysis_year + vehicles +'.txt', "w")
    summary_report.write('Summary results for ' + vehicles + ' traffic message channels (TMC)' + '\n')
    summary_report.write('Analysis Month(s): '+ period + '\n')
    summary_report.write('Analysis Year: '+ analysis_year + '\n')
             
    # Open the vehicle specific TMC Identification file and store in dataframe
    print 'Loading the ' + period + ' ' + vehicles + ' TMC file into a Pandas Dataframe'
    df_working_tmc = pd.read_csv(tmc_file)     
    keep_columns = ['tmc','road','direction','county','miles','thrulanes','route_numb','aadt','aadt_singl','aadt_combi']
    df_working_tmc = df_working_tmc.loc[:,keep_columns]
    df_working_tmc  = df_working_tmc.rename(columns={'tmc':'Tmc','miles':'length','thrulanes':'lanes','route_numb':'SR'})
        
    # Add the posted speed limit to the TMC file
    print 'Adding the posted speed limit to the  ' + period + ' ' + vehicles + ' TMC file'
    df_working_tmc = pd.merge(df_working_tmc, posted_df, on='Tmc', suffixes=('_x','_y'), how='left')

    # Remove any TMC's in the exclusion list
    for removal_tmc in exclude_list:
        df_working_tmc = df_working_tmc[df_working_tmc.Tmc != removal_tmc]
                    
    df_output = df_working_tmc
    summary_report.write('Total Number of TMC segments: '+ str(len(df_output)) + '\n')
    os.remove(tmc_file)

    # Travel Times by Time Period 
    for x in range(0,(len(analysis_time)-1)):

        display_hour = str(analysis_time[x].hour)
        
        if analysis_time[x].minute == 0:
            display_minute = '00'
            
        else: 
            display_minute = str(analysis_time[x].minute)
        
        column_time = display_hour + display_minute
        display_time = display_hour + ':' + display_minute

        print 'Working on ' + vehicles + ' ' + display_time + ' calculations.'
    
        # Trim the Full Dataframe so it only has observations for the Time Period of Interest
        df_tod = time_period(df_analysis, analysis_time[x], analysis_time[x+1])
           
        # Calculate the Speed for the Current Time Period for the Desired Percentile
        df_spd = calculate_speed(df_tod, speed_percentile)
        df_spd['time'] = display_time
            
        # Append the Time Series Dataframe if that output is called for
        if output_timeseries == 'yes':
                
            if x == 0:
                df_timeseries_output = df_spd
                
            else: 
                df_timeseries_output = df_timeseries_output.append(df_spd,ignore_index=True)
            
        # Rename columns for cleaner output
        df_spd  = df_spd.rename(columns={'speed':column_time + '_speed'})
        df_spd  = df_spd.rename(columns={'travel_time':column_time + '_time'})
        df_spd  = df_spd.rename(columns={'ratio':column_time + '_ratio'})
        df_spd = df_spd.drop(columns=['time'])

        # Summarize Time Period Specific Data in a text file
        summary_report = results_output_summary(df_spd, summary_report, vehicles, output_directory, column_time, moderate_congestion, heavy_congestion, severe_congestion, str(len(df_output)))
        
        # Merge the Speed data with the TMC summary file
        df_output = pd.merge(df_output, df_spd, on='Tmc', suffixes=('_x','_y'), how='left')

    # Write out the vehicle specific dataframe to csv if desired
    if output_csv == 'yes':
        df_output.to_csv(output_directory + '/' + period +'_' + analysis_year + '_' + vehicles +'_tmc_'+str(int(speed_percentile*100))+'th_percentile_speed.csv',index=False)

    if output_timeseries == 'yes':
        df_timeseries_output.to_csv(output_directory + '/' + period +'_' + analysis_year + '_' + vehicles +'_tmc_'+str(int(speed_percentile*100))+'th_percentile_timeseries.csv',index=False)

    df_timeseries_output['timestamp'] = pd.to_datetime(df_timeseries_output.time)    
    df_timeseries_output['timestamp'] = df_timeseries_output['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_timeseries_output = df_timeseries_output.drop(columns=['time'])
                   
    # Now join the CSV to the NPMRDS Shapefile based on a table join and save the revised shapefile
    print 'Creating the ' + period + ' ' + vehicles + ' shapefile'
    working_shapefile = gp_table_join(df_output,tmc_shapefile)
    working_shapefile.to_file(output_directory + '/' + period +'_' + analysis_year + '_' + vehicles + '_travel_time_by_tod.shp')     
    shutil.copyfile(tmc_projection, output_directory + '/' + period +'_' + analysis_year + '_' + vehicles + '_travel_time_by_tod.prj')
    
    if output_timeseries == 'yes':
        print 'Creating the ' + period + ' ' + vehicles + ' timeseries shapefile'
        working_shapefile = gp_table_join(df_timeseries_output,tmc_shapefile)
        working_shapefile.to_file(output_directory + '/' + period +'_' + analysis_year + '_' + vehicles + '_timeseries.shp')
        shutil.copyfile(tmc_projection, output_directory + '/' + period +'_' + analysis_year + '_' + vehicles + '_timeseries.prj')
           
    # Close the summary report file
    summary_report.close()

end_of_production = time.time()
print 'The Total Time for all processes took', (end_of_production-start_of_production)/60, 'minutes to execute.'
exit()