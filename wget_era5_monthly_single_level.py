mport cdsapi
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

    
download_directory = '/Users/liuyangfan/Desktop/Stanford/pyrocast-database/era5/pyroCb_events_single_level_monthly/'

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
    event_datetime = datetime.datetime.strftime(event_datetime,'%Y-%m-%d %H:%M:%S')
    # Determine the geographical area
    area = determine_geographical_area(row['pyroCb_longitude'], row['pyroCb_latitude'])
    # Create a directory for the event
    event_dir = os.path.join(download_directory, str(row['pyroCb_id']))
    if not os.path.exists(event_dir):
        os.makedirs(event_dir)
    r=c.retrieve(
        'reanalysis-era5-single-levels-monthly-means',
        {
            'format': 'netcdf',
            'product_type': 'monthly_averaged_reanalysis_by_hour_of_day',
            'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', 'instantaneous_10m_wind_gust',#no monthly '10m_wind_gust_since_previous_post_processing',
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
            'year': [
                '2018', '2019', '2020',
                '2021', '2022',
            ],
            'month':  event_datetime[5:7],
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': area,
        },
        )
    url = r.location  # 获取文件下载地址
    path = event_dir+'/'  # 存放文件夹
    filename = '24hrs.nc'  # 文件名
    Downloader(url, path, filename)  # 添加进IDM中下载
