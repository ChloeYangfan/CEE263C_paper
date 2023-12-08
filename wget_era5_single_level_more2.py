import cdsapi
import wget
c = cdsapi.Client()  # 创建用户
from subprocess import call
import pandas as pd
import os
import datetime

# pyroCb events info
csv_file_path='/Users/liuyangfan/Desktop/Stanford/pyrocast-database/pyrocb_events.csv'
events_df = pd.read_csv(csv_file_path)

# functions
# Function to download with wget
def Downloader(task_url, folder_path, file_name):
    """
    下载器
    :param task_url: 下载任务地址
    :param folder_path: 存放文件夹
    :param file_name: 文件名
    :return:
    """
    wget.download(task_url,out=folder_path+file_name)#folder_path needs '/' in the end

# Function to determine the geographical area
def determine_geographical_area(longitude, latitude):
    # Adjusting the coordinates to the nearest 0.25 degree grid
    north = latitude + 0.25 - (latitude % 0.25)
    south = latitude - (latitude % 0.25)
    if latitude % 0.25==0:
        south=south-0.25
    east = longitude + 0.25 - (longitude % 0.25)
    west = longitude - (longitude % 0.25)
    if longitude % 0.25==0:
        west=west-0.25
    return [north, west, south, east]

    
download_directory = '/Users/liuyangfan/Desktop/Stanford/pyrocast-database/era5/pyroCb_events_single_level/'
    
############### 48 hr before and after ####################
for index, row in events_df.iterrows():
    # Skip rows without PyroCb time information
    if pd.isna(row['pyroCb_time_utc']):
        continue
    if(int(row['pyroCb_time_utc'])>2359):
        continue
    # Formatting the event time
    event_time = f"{row['pyroCb_date_utc']} {int(row['pyroCb_time_utc']):04d}"
    event_datetime = datetime.datetime.strptime(event_time, '%Y-%m-%d %H%M')
    for dt in range(0, 49):
        # Your code here
        print(dt)
        # Calculate the time range
        time_before=event_datetime - datetime.timedelta(hours=dt)
        time_after=event_datetime + datetime.timedelta(hours=1+dt)
        time_before=datetime.datetime.strftime(time_before,'%Y-%m-%d %H:%M:%S')
        time_after=datetime.datetime.strftime(time_after,'%Y-%m-%d %H:%M:%S')
        # Determine the geographical area
        area = determine_geographical_area(row['pyroCb_longitude'], row['pyroCb_latitude'])
        # Create a directory for the event
        event_dir = os.path.join(download_directory, str(row['pyroCb_id']))
        if not os.path.exists(event_dir):
            os.makedirs(event_dir)
    
        r=c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', 'instantaneous_10m_wind_gust','10m_wind_gust_since_previous_post_processing',
                '2m_dewpoint_temperature','2m_temperature', 
                'surface_pressure', 
                'total_precipitation',
                'convective_inhibition', 'convective_available_potential_energy', 'k_index','total_totals_index', # thunderstorm and instability
                'vertical_integral_of_divergence_of_cloud_frozen_water_flux', 'vertical_integral_of_divergence_of_cloud_liquid_water_flux', 'vertical_integral_of_eastward_cloud_frozen_water_flux',
                'vertical_integral_of_eastward_cloud_liquid_water_flux', 'vertical_integral_of_northward_cloud_frozen_water_flux', 'vertical_integral_of_northward_cloud_liquid_water_flux',
                'geopotential',
                'surface_latent_heat_flux', 'surface_sensible_heat_flux',
                'boundary_layer_height',
                'cloud_base_height', 'high_cloud_cover','medium_cloud_cover', 'low_cloud_cover','total_cloud_cover',
                'total_column_cloud_ice_water', 'total_column_cloud_liquid_water', 'total_column_water',#cloud
            ],

                'year': time_before[:4],#change to loop variable
                'month': time_before[5:7],#change to loop variable
                'day': time_before[8:10],#change to loop variable
                'time': time_before[11:13]+':00',#change to loop variable
                'area': area,#change to loop variable
            },
            )
    
        url = r.location  # 获取文件下载地址
        path = event_dir+'/'  # 存放文件夹
        filename = 'time_before_'+str(dt)+'.nc'  # 文件名
        Downloader(url, path, filename)  # 添加进IDM中下载

        r=c.retrieve(
             'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', 'instantaneous_10m_wind_gust','10m_wind_gust_since_previous_post_processing',
                '2m_dewpoint_temperature','2m_temperature', 
                'surface_pressure', 
                'total_precipitation',
                'convective_inhibition', 'convective_available_potential_energy', 'k_index','total_totals_index', # thunderstorm and instability
                'vertical_integral_of_divergence_of_cloud_frozen_water_flux', 'vertical_integral_of_divergence_of_cloud_liquid_water_flux', 'vertical_integral_of_eastward_cloud_frozen_water_flux',
                'vertical_integral_of_eastward_cloud_liquid_water_flux', 'vertical_integral_of_northward_cloud_frozen_water_flux', 'vertical_integral_of_northward_cloud_liquid_water_flux',
                'geopotential',
                'surface_latent_heat_flux', 'surface_sensible_heat_flux',
                'boundary_layer_height',
                'cloud_base_height', 'high_cloud_cover','medium_cloud_cover', 'low_cloud_cover','total_cloud_cover',
                'total_column_cloud_ice_water', 'total_column_cloud_liquid_water', 'total_column_water',#cloud
            ],
                'year': time_after[:4],#change to loop variable
                'month': time_after[5:7],#change to loop variable
                'day': time_after[8:10],#change to loop variable
                'time': time_after[11:13]+':00',#change to loop variable
                'area': area,#change to loop variable
            },
            )
    
        url = r.location  # 获取文件下载地址
        path = event_dir+'/'  # 存放文件夹
        filename = 'time_after_'+str(dt)+'.nc'  # 文件名
        Downloader(url, path, filename)  # 添加进IDM中下载
