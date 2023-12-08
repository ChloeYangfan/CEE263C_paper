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

    
download_directory = '/Users/liuyangfan/Desktop/Stanford/pyrocast-database/era5/pyroCb_events_pressure_level/'
vars=['divergence', 'fraction_of_cloud_cover', 'geopotential',
        'ozone_mass_mixing_ratio', 'potential_vorticity', 'relative_humidity',
        'specific_cloud_ice_water_content', 'specific_cloud_liquid_water_content', 'specific_humidity',
        'specific_rain_water_content', 'specific_snow_water_content', 'temperature',
        'u_component_of_wind', 'v_component_of_wind', 'vertical_velocity',
        'vorticity',
    ]
p_levels=[
                '1', '2', '3',
                '5', '7', '10',
                '20', '30', '50',
                '70', '100', '125',
                '150', '175', '200',
                '225', '250', '300',
                '350', '400', '450',
                '500', '550', '600',
                '650', '700', '750',
                '775', '800', '825',
                '850', '875', '900',
                '925', '950', '975',
                '1000',
            ]
############### 48 hr before and after ####################
cnt=0
for index, row in events_df.iterrows():
    # Skip rows without PyroCb time information
    if pd.isna(row['pyroCb_time_utc']):
        continue
    if(int(row['pyroCb_time_utc'])>2359):
        continue
    cnt=cnt+1
    if cnt<53:
        continue
    print(row['pyroCb_id'])
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
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': vars,
                'pressure_level': p_levels,
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
            'reanalysis-era5-pressure-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': vars,
                'pressure_level': p_levels,
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
