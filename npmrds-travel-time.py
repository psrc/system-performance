# This file creates Time of Day Speed information from the NPMRDS INRIX Datasets
# Created by Puget Sound Regional Council Staff

import pandas as pd
import time
import geopandas as gp
import os
import shutil
import zipfile

# Year to Analyze in a 4 digit format
analysis_year = '2018'

# List of Months to analyze using the two digit integer code
analysis_months = ['mar']

# Path to the System Performance Data on the server
working_path = 'c:/System_Performance/travel-time/'
#working_path = '//file2/Projects/System_Performance/travel-time/'
data_directory = working_path + 'downloads/'

month_lookup = {'jan' : '01',
                'feb' : '02',
                'mar' : '03',
                'apr' : '04',
                'may' : '05',
                'jun' : '06',
                'jul' : '07',
                'aug' : '08',
                'sep' : '09',
                'oct' : '10',
                'nov' : '11',
                'dec' : '12'}  
                       
# Shapefile of TMC's for use in shapefile joining
tmc_shapefile = working_path + 'reference/shapefiles/Washington.shp' 
tmc_projection = working_path + 'reference/shapefiles/Washington.prj' 

# Reference Files for use in Analysis
tmc_posted_speed_file = working_path + 'reference/tmc_posted_speed.csv' 
tmc_exclusion_file = working_path + 'tmc_exclusions.csv' 

# Percentile to be used for the Average Speed Calculation
speed_percentile = 0.80
low_spd = 10
high_spd = 75

# Congestion Thresholds
moderate_congestion = 0.70
severe_congestion = 0.50

# Flag to determine if csv file is output or not (yes or no)
output_csv = 'no'

# Flag to determine if a csv file for a time series animation in ArcMap is desired yes or no
output_timeseries = 'yes'

# Dictionary Defining the Start and End Times for the Periods of Analysis
time_of_day = {"TOD_Name":['5am','6am','7am','8am','9am','10am','11am','12pm','1pm',
                           '2pm','3pm','4pm','5pm','6pm','7pm','8pm','9pm','10pm'],
               "start_time":[5,6,7,8,9,10,11,12,13,
                             14,15,16,17,18,19,20,21,22],
               "end_time":[5,6,7,8,9,10,11,12,13,
                           14,15,16,17,18,19,20,21,22]
           }

# List of Vehicle Type to analyze
vehicle_types = ['cars']

# This next part of the script contains a couple functions that are run on the dataset                                                               
                                                                   
# Function to trim the dataframe to the time period of interest
def time_period(df_region, start_time, end_time):
    
    if start_time == end_time:
        df_tod=df_region[df_region.hour == start_time]
    
    else:
        df_tod=df_region[df_region.hour >= start_time]
        df_tod=df_tod[df_tod.hour < end_time]
             
    return df_tod

# Function to Return the Timeperiod Travel Time
def travel_time(df_timeperiod, average_per):

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
def results_output_summary(working_df, working_file, vehicle_type, results_directory, time_period, moderate, severe, total_tmc):
      
    all_tmc = float(total_tmc)
    total_tmc_data = float(len(working_df))
    percent_tmc_data = (round((total_tmc_data / all_tmc),2))*100
                       
    segments_moderately_congested = float(len(working_df[(working_df[time_period+'_ratio']<moderate)]))
    percent_moderately_congested = (round((segments_moderately_congested / total_tmc_data),2))*100
    
    segments_severely_congested = float(len(working_df[(working_df[time_period+'_ratio']<severe)]))
    percent_severely_congested = (round((segments_severely_congested / total_tmc_data),2))*100
    
    working_file.write(' ' + '\n')
    working_file.write('Summary of Data for: ' + time_period + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments with data: ' + str(total_tmc_data) + '\n')  
    working_file.write('  --- % of Total TMC segments with data: ' + str(percent_tmc_data) + '%' + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments under 70% of the posted speed: ' + str(segments_moderately_congested) + '\n')
    working_file.write('  --- % of TMC segments under 70% of the posted speed: ' + str(percent_moderately_congested) + '%' + '\n')
    working_file.write(' ' + '\n')
    working_file.write('  --- Total Number of TMC segments under 50% of the posted speed: ' + str(segments_severely_congested) + '\n')
    working_file.write('  --- % of TMC segments under 50% of the posted speed: ' + str(percent_severely_congested) + '%' + '\n')
    working_file.write(' ' + '\n')

    return working_file


# The next section is the main body for script execution
start_of_production = time.time()

# Open a dataframe with posted speed limits in it by TMC
posted_df = pd.read_csv(tmc_posted_speed_file)
keep_columns = ['Tmc','PostedSpeed']
posted_df = posted_df.loc[:,keep_columns]
posted_df['PostedSpeed']=  posted_df['PostedSpeed'].astype(float)

# loop over the monthly data to be analyzed
for months in analysis_months:
    
    monthly_code = month_lookup[months]
    
    print 'Working on '+ months +' data files.'
    
    # Loop over the vehicle types (cars and trucks) for analysis
    for vehicles in vehicle_types:

        output_directory = working_path + '/output/' + months + analysis_year

        # Create the output directory for the speed results if it does not already exist
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        # Create the text file for summary output
        summary_report = open(output_directory + '/' + months + analysis_year + vehicles +'.txt', "w")
        summary_report.write('Summary results for ' + vehicles + ' traffic message channels (TMC)' + '\n')
        summary_report.write('Analysis Month: '+ months + '\n')
        summary_report.write('Analysis Year: '+ analysis_year + '\n')
             
        # First uncompress files for use in analysis
        print 'Uncompressing the temporary ' + months + ' ' + vehicles + ' working files for analysis.'
        npmrds_archive = zipfile.ZipFile(data_directory+ months + analysis_year+vehicles+'.zip', 'r')
        npmrds_archive.extractall(data_directory)
        npmrds_archive.close()    

        data_file = data_directory + months + analysis_year + vehicles +'.csv'      
        tmc_file = data_directory + '/TMC_Identification.csv'
        contents_file = data_directory + '/Contents.txt'

        # Open the vehicle specific TMC Identification file and store in dataframe
        print 'Loading the ' + months + ' ' + vehicles + ' TMC file into a Pandas Dataframe'
        df_working_tmc = pd.read_csv(tmc_file)     
        keep_columns = ['tmc','road','direction','county','miles','thrulanes','route_numb','aadt','aadt_singl','aadt_combi']
        df_working_tmc = df_working_tmc.loc[:,keep_columns]
        df_working_tmc  = df_working_tmc.rename(columns={'tmc':'Tmc','miles':'length','thrulanes':'lanes','route_numb':'SR'})
        
        # Add the posted speed limit to the TMC file
        print 'Adding the posted speed limit to the  ' + months + ' ' + vehicles + ' TMC file'
        df_working_tmc = pd.merge(df_working_tmc, posted_df, on='Tmc', suffixes=('_x','_y'), how='left')

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

        # Split out the date and time into their own columns
        print 'Splitting out the Date and Time in the ' + months + ' ' + vehicles + ' speed file'
        df_working_spd[['date', 'time']] = df_working_spd['measurement_tstamp'].str.split(' ', expand=True)
        df_working_spd[['hour', 'minute', 'seconds']] = df_working_spd['time'].str.split(':', expand=True)
        df_working_spd['hour']= df_working_spd['hour'].astype(str).astype(int)
        df_working_spd  = df_working_spd.drop(columns=['measurement_tstamp','date','time','minute','seconds'])
    
        df_output = df_working_tmc
        summary_report.write('Total Number of TMC segments: '+ str(len(df_output)) + '\n')

        # Travel Times by Time Period 
        for x in range(0,len(time_of_day['TOD_Name'])):
            print 'Working on '+str(time_of_day['TOD_Name'][x])+' Travel Speed calculation for ' + months + ' ' + vehicles + ' tmc records'
    
            # Trim the Full Dataframe so it only has observations for the Time Period of Interest and then trim to only TMC Code and Speed
            df_tod = time_period(df_working_spd, time_of_day['start_time'][x], time_of_day['end_time'][x])
    
            # Calculate the Speed for the Current Time Period for the Desired Percentile
            df_spd = travel_time(df_tod, speed_percentile)
            
            # Append the Time Series Dataframe if that output is called for
            if output_timeseries == 'yes':
                
                if x == 0:
                    df_timeseries_output = df_spd
                
                else: 
                    df_timeseries_output = df_timeseries_output.append(df_spd,ignore_index=True)
            
            # Rename columns for cleaner output
            df_spd  = df_spd.rename(columns={'speed':time_of_day['TOD_Name'][x] + '_speed'})
            df_spd  = df_spd.rename(columns={'travel_time':time_of_day['TOD_Name'][x] + '_time'})
            df_spd  = df_spd.rename(columns={'ratio':time_of_day['TOD_Name'][x] + '_ratio'})
            df_spd = df_spd.drop(columns=['hour'])

            # Summarize Time Period Specific Data in a text file
            summary_report = results_output_summary(df_spd, summary_report, vehicles, output_directory, time_of_day['TOD_Name'][x], moderate_congestion, severe_congestion, str(len(df_output)))
        
            # Merge the Speed data with the TMC summary file
            df_output = pd.merge(df_output, df_spd, on='Tmc', suffixes=('_x','_y'), how='left')

        # Write out the vehicle specific dataframe to csv if desired
        if output_csv == 'yes':
            df_output.to_csv(output_directory + '/' + months +'_' + analysis_year + '_' + vehicles +'_tmc_'+str(int(speed_percentile*100))+'th_percentile_speed.csv',index=False)

            if output_timeseries == 'yes':
                df_timeseries_output.to_csv(output_directory + '/' + months +'_' + analysis_year + '_' + vehicles +'_tmc_timeseries.csv',index=False)
            
        # Now join the CSV to the NPMRDS Shapefile based on a table join and save the revised shapefile
        print 'Creating the ' + months + ' ' + vehicles + ' shapefile'
        working_shapefile = gp_table_join(df_output,tmc_shapefile)
        working_shapefile.to_file(output_directory + '/' + months +'_' + analysis_year + '_' + vehicles + '_travel_time_by_tod.shp')
        shutil.copyfile(tmc_projection, output_directory + '/' + months +'_' + analysis_year + '_' + vehicles + '_travel_time_by_tod.prj')
    
        if output_timeseries == 'yes':
            print 'Creating the ' + months + ' ' + vehicles + ' timeseries shapefile'
            working_shapefile = gp_table_join(df_timeseries_output,tmc_shapefile)
            working_shapefile.to_file(output_directory + '/' + months +'_' + analysis_year + '_' + vehicles + '_timeseries.shp')
            shutil.copyfile(tmc_projection, output_directory + '/' + months +'_' + analysis_year + '_' + vehicles + '_timeseries.prj')
        
        # Delete the uncompressed files
        print 'Deleting the temporary ' + months + ' ' + vehicles + ' working files'
        os.remove(data_file)
        os.remove(tmc_file)
        os.remove(contents_file)
    
        # Close the summary report file
        summary_report.close()

end_of_production = time.time()
print 'The Total Time for all processes took', (end_of_production-start_of_production)/60, 'minutes to execute.'
exit()