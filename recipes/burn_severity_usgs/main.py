#%%
"""
Code Mostly based on example here:
https://code.usgs.gov/eros-user-services/machine_to_machine/m2m_landsat_9_search_download/-/blob/main/M2M_Landsat_9_Metadata_Search_Download.ipynb?ref_type=heads#dataset-search
"""
import requests
import json
import geojson
import socket
from shapely.geometry import Polygon, box
from getpass import getpass
import sys
import time
from dateutil.relativedelta import relativedelta
import cgi
import os
import pandas as pd
import geopandas as gpd
import warnings
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
warnings.filterwarnings("ignore")


# import the south coast function library and the email function 
sys.path.append(r'\\spatialfiles.bcgov\work\srm\sry\Local\scripts\python')
from sc_python_function_library import * 
import email_function 
from email_function import SendEmail

# %%
class fire_severity_analysis:
    def __init__(self, fire_ID):
        """
        This class is used to compile and organize the information required for creating the 
        payload requests for USGS imagery. 

        THE INTENT OF THIS ANALYSIS IS TO PROVIDE AN EARLY GLIMPSE OF FIRE SEVERITY ON THE
        IN THE IMMEDIATE AFTERMATH OF A WILDFIRE SEASON. AS A RESULT, SOME WEB SCRAPING WILL 
        FROM THE "FIRE PERIMETERS - CURRENT" DATASET. 

        ANY FIRES MOVED INTO THE "HISTORICAL" AREA SHOULD DEFER TO THE OFFICIAL FAIB WILDFIRE 
        SEVERITY LAYER. 
        """

        print("CONSTRUCTING QUERY DATA...")
        try:
            self.fire_ID = fire_ID
        except:
            raise ValueError("     Error Encountered")
        
        print("CREATING AGOL CONNECTION...")
        self.url = 'https://governmentofbc.maps.arcgis.com'
        self.agol_username, self.agol_password = get_credentials("agol")
        self.gis = GIS(self.url, self.agol_username, self.agol_password, verify_cert=False)
     

        # CONNECT TO FIRE LOCATIONS - CURRENT - VIEW (AGOL ITEM 397a1defe7f04c2b8ef6511f6c087dbf)
        fire_location = self.gis.content.get("397a1defe7f04c2b8ef6511f6c087dbf")
        fire_location_feature_layer = FeatureLayerCollection.fromitem(fire_location).layers[0]
        fire_location_fc_query = fire_location_feature_layer.query(where=f"FIRE_NUMBER = '{self.fire_ID}'")
        df = fire_location_fc_query.sdf
        num_rows = len(df)
        
        if len(df) > 1:
            print(f"     ERROR: More than 1 Record Found in 'FIRE Locations - Current' with ID {self.fire_ID}")
        elif len(df) < 1:
            print(f"     ERROR: No Records Found in 'FIRE Locations - Current' with ID {self.fire_ID}")
        elif len(df) == 1:
            print(f"     Scraping Data from 'FIRE LOCATIONS - CURRENT' for fire {self.fire_ID}'...")
            self.estimated_fire_size = df['CURRENT_SIZE'].iloc[0]
            self.fire_centre = df['FIRE_CENTRE'].iloc[0]
            self.fire_zone = df['ZONE'].iloc[0]

            # GET DATE SEARCH PARAMS (PRE-FIRE)
            self.ignition_date = df['IGNITION_DATE'].iloc[0]
            self.string_ignition_date = self.ignition_date.strftime("%Y-%m-%d")
            self.pre_fire_search_start = (self.ignition_date - relativedelta(months=3)).strftime("%Y-%m-%d")
            self.pre_fire_search_end = (self.ignition_date - relativedelta(days=1)).strftime("%Y-%m-%d")

            self.suspected_cause = df['FIRE_CAUSE'].iloc[0]
            self.fire_type = df['FIRE_TYPE'].iloc[0]
            self.approzimate_location = df['GEOGRAPHIC_DESCRIPTION'].iloc[0]
            self.fire_url = df['FIRE_URL'].iloc[0]
            self.response_type = df['RESPONSE_TYPE_DESC'].iloc[0]
            self.out_date = df['FIRE_OUT_DATE'].iloc[0]

        # CONNECT TO FIRE PERIMETERS - CURRENT - VIEW (AGOL ITEM 6ed3ec9b90f844fcaf9fea499bacae8e)
        fire_perimeters = self.gis.content.get("6ed3ec9b90f844fcaf9fea499bacae8e")
        fire_perimeters_feature_layer = FeatureLayerCollection.fromitem(fire_perimeters).layers[0]
        fire_perimeters_fc_query = fire_perimeters_feature_layer.query(where=f"FIRE_NUMBER = '{self.fire_ID}'", return_geometry=True, out_fields='*')
        df = fire_perimeters_fc_query.sdf
        num_rows = len(df)

        
        if len(df) > 1:
            print(f"     ERROR: More than 1 Record Found in 'FIRE PERIMETERS - Current' with ID {self.fire_ID}")
        elif len(df) < 1:
            print(f"     ERROR: No Records Found in 'FIRE PERIMETERS - Current' with ID {self.fire_ID}")
        elif len(df) == 1:
            print(f"     Scraping Data from 'FIRE PERIMETERS - CURRENT' for fire {self.fire_ID}'...")


            self.actual_fire_size = df['FIRE_SIZE_HECTARES'].iloc[0]

            # GET DATE SEARCH PARAMS (POST FIRE)
            self.last_update = df['LOAD_DATE'].iloc[0]
            self.string_last_update = self.last_update.strftime("%Y-%m-%d")
            self.post_fire_search_end = (self.last_update + relativedelta(months=3)).strftime("%Y-%m-%d")
            self.post_fire_search_start = (self.last_update + relativedelta(days=1)).strftime("%Y-%m-%d")

            self.current_fire_status = df['FIRE_STATUS'].iloc[0]
            self.perimeter_data_source = df['SOURCE'].iloc[0]
            self.spatial = df['SHAPE'].iloc[0]

            # CREATE GeoJSON FILE for the Requested Fire
            self.web_merc_spatial = fire_perimeters_fc_query.features[0].geometry
            polygon_coords = self.web_merc_spatial['rings'][0]
            polygon = Polygon(polygon_coords)
            gdf = gpd.GeoDataFrame(index=[0], crs='EPSG:3857', geometry=[polygon])
            gdf_84 = gdf.to_crs(epsg=4326)
            gdf_84.to_file(f"data/fire_{self.fire_ID}.geojson", driver="GeoJSON")

            # Determine the Lower Left and Upper Right for Querying USGS
            with open(f"data/fire_{self.fire_ID}.geojson", 'r') as file:
                data = json.load(file)

            def extract_coordinates(feature):
                if feature['geometry']['type'] == 'Point':
                    return [feature['geometry']['coordinates']]
                elif feature['geometry']['type'] in ['MultiPoint', 'LineString']:
                    return feature['geometry']['coordinates']
                elif feature['geometry']['type'] in ['MultiLineString', 'Polygon']:
                    return [coord for part in feature['geometry']['coordinates'] for coord in part]
                elif feature['geometry']['type'] == 'MultiPolygon':
                    return [coord for part in feature['geometry']['coordinates'] for subpart in part for coord in subpart]

            all_coords = []
            for feature in data['features']:
                all_coords.extend(extract_coordinates(feature))
            
            # Transpose to get lists of all latitudes and longitudes
            longitudes, latitudes = zip(*all_coords)

            self.bottom_left_long = min(longitudes)
            self.bottom_left_lat = min(latitudes)
            self.top_right_long = max(longitudes)
            self.top_right_lat = max(latitudes)

class usgs_queries:
    def __init__(self, fire_severity_object):
        """
        Requires a fire query object to run
        """
        print("")
        print("CREATING USGS CONNECTION...")

        self.fire_object = fire_severity_object
        self.usgs_username = "eric.millan@gov.bc.ca"
        self.usgs_password = "07231885Grant!"
        self.service_url = "https://m2m.cr.usgs.gov/api/api/json/stable/"
        self.landsat_dataset_name = "landsat_ot_c2_l2"


        self.spatialFilter = {'filterType' : 'mbr',
        'lowerLeft' : {'latitude' : self.fire_object.bottom_left_lat,
                        'longitude' : self.fire_object.bottom_left_long},
        'upperRight' : { 'latitude' : self.fire_object.top_right_lat,
                        'longitude' : self.fire_object.top_right_long}}
        
        # Retrieve API KEY:
        response = requests.post(f"{self.service_url}login", json={'username': self.usgs_username, 'password': self.usgs_password})

        if response.status_code == 200:  # Check for successful response
            apiKey = response.json()['data']
            print('     Login Successful, API Key Received!')
            self.apiKey = apiKey
        else:
            print("\nLogin was unsuccessful, please try again or create an account at: https://ers.cr.usgs.gov/register.")

    def sendRequest(self, data, serviceURL, exitIfNoResponse = True):
        """
        ---- COMPLETE ----
        Generic function for creating a post request to the USGS to query/ interact with their data. There are many ways to
        customize this post request. The URL will need to be modified for specific actions:

        eg: 
        serviceURL + "login" =              Generates API Key, Used in the "retrieve_api_key" function
        serviceURL + "dataset-search"       Queries the datasets
        serviceURL + "scene-search"         Identifies Scenes (images) within datasets, Scene Filters available for time, spatial, cloud, etc
        serviceURL + "download-options"     Identifies products available for download
        serviceURL + "download-request"     Requests URLS for download (Some URLS returned will be 'preparing' and need to be accessed later)
        serviceURL + "download-retrieve"    Retrieves downloads previouslys marked as 'preparing'
        serviceURL + "logout"               Invalidates the API Key

        More information here: https://m2m.cr.usgs.gov/api/docs/json/#section-issues

        """
        json_data = json.dumps(data)

        if self.apiKey == None:
            response = requests.post(serviceURL, json_data)
        else:
            headers = {'X-Auth-Token': self.apiKey}              
            response = requests.post(serviceURL, json_data, headers = headers)    

        try:
            httpStatusCode = response.status_code 
            if response == None:
                print("No output from service")
                if exitIfNoResponse: sys.exit()
                else: return False
            output = json.loads(response.text)
            if output['errorCode'] != None:
                print(output['errorCode'], "- ", output['errorMessage'])
                if exitIfNoResponse: sys.exit()
                else: return False
            if  httpStatusCode == 404:
                print("404 Not Found")
                if exitIfNoResponse: sys.exit()
                else: return False
            elif httpStatusCode == 401: 
                print("401 Unauthorized")
                if exitIfNoResponse: sys.exit()
                else: return False
            elif httpStatusCode == 400:
                print("Error Code", httpStatusCode)
                if exitIfNoResponse: sys.exit()
                else: return False
        except Exception as e: 
            response.close()
            print(e)
            if exitIfNoResponse: sys.exit()
            else: return False
        response.close()
        
        return output['data']

    def pre_fire_search(self):
        """
        Create Data Packet for Prefire Search
        """
        print("     Sending Pre Fire Scene Search....")
        
        search_payload = {
                'datasetName': self.landsat_dataset_name,
                    'sceneFilter': 
                    {'metadataFilter': 
                        {'filterType': 'value', 'filterId': '61af9273566bb9a8','value': '9'},
                        'spatialFilter': self.spatialFilter,
                        'acquisitionFilter': {'start': self.fire_object.pre_fire_search_start, 'end': self.fire_object.pre_fire_search_end},
                        'cloudCoverFilter': {'min': 0, 'max': 100}
                    }
                }
                
        results = usgs_connection.sendRequest(search_payload, self.service_url + "scene-search")

        self.pre_fire_results = pd.json_normalize(results['results'])

    def post_fire_search(self):
        """
        Create Data Packet for Prefire Search
        """
        print("     Sending Post Fire Scene Search....")
        
        search_payload = {
                'datasetName': self.landsat_dataset_name,
                    'sceneFilter': 
                    {'metadataFilter': 
                        {'filterType': 'value', 'filterId': '61af9273566bb9a8','value': '9'},
                        'spatialFilter': self.spatialFilter,
                        'acquisitionFilter': {'start': self.fire_object.post_fire_search_start, 'end': self.fire_object.post_fire_search_end},
                        'cloudCoverFilter': {'min': 0, 'max': 100}
                    }
                }
                
        results = usgs_connection.sendRequest(search_payload, self.service_url + "scene-search")

        print("     Compiling Results...")
        self.post_fire_results = pd.json_normalize(results['results'])
        self.min_cloud_cover_row = self.post_fire_results.loc[self.post_fire_results['cloudCover'].idxmin()]
        self.post_fire_min_cloud = pd.DataFrame([self.min_cloud_cover_row])
        self.post_fire_min_cloud = self.post_fire_min_cloud.reset_index(drop=True)

        # We look for the best post-fire image we can first, and then look for a similar image from +/- 1 month the pregious year.
        self.post_fire_scene_id = self.post_fire_min_cloud.loc[0, 'entityId']
        self.post_fire_scene_date = self.post_fire_min_cloud.loc[0, 'temporalCoverage.startDate']


    def download_post_fire_image(self):
        """
        """
        print("     Preparing to Download Post Fire Imagery")
        sceneIDs = []
        sceneIDs.append(self.post_fire_scene_id)

        download_payload = {"datasetName":self.landsat_dataset_name, 
                    'entityIds': sceneIDs
                    }

        download_options = usgs_connection.sendRequest(download_payload, self.service_url + "download-options")
        print("     Analysing Available Products...")

        available_products = []
        for product in download_options:
            if product['available'] == True and product['downloadSystem'] != 'folder':
                available_products.append({'entityId':product['entityId'], 'productId':product['id']}
                )

        request_count = len(available_products)
        label= "test_download_request"
        download_req_payload = {'downloads': available_products, 
                                'label': label}

        print("     Requesting Download URLs...")
        request_results = usgs_connection.sendRequest(download_req_payload, self.service_url + "download-request")

        folder = f"data/scripts/data/incoming/test_download_folder/file_{self.fire_object.fire_ID}"

        if not os.path.exists(folder):
            print("     Creaing Output Folder...")
            os.makedirs(folder)
        
        # Iterate over each available download
        for item in request_results['availableDownloads']:
            download_url = item['url']
            download_id = item['downloadId']
            filename = f"{download_id}.tar"  
            print(f"     Downloading Item {filename}")
            
            # Download the file
            response = requests.get(download_url, stream=True, verify=False)
            if response.status_code == 200:
                file_path = os.path.join(folder, filename)
                with open(file_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                print(f"     Downloaded: {filename}")
            else:
                print(f"Failed to download file with ID: {download_id}")


# %%

fire_query = fire_severity_analysis("G41493")
usgs_connection = usgs_queries(fire_query)

usgs_connection.post_fire_search()
# %%

usgs_connection.download_post_fire_image()
# usgs_connection.pre_fire_search()

# Functions:
# - Create a "Evaluation" Spreadsheed which looks for "
#   % of the scene overlapping with the fire perimeter"
#  Amount of cloud coverage over the fire area
# Cloud Coverage
# Closest Matching from 1 Year Ago (To get the Pre Fire area)
# Graph comparing the different imagery available! (Matching closest pairs )


# %%
def calc_overlap_percentage(search_area, raster_boundary):
    """
    """
    intersection = search_area.intersection(raster_boundary)
    if not intersection.is_empty:
        overlap_area = intersection.area
        search_area_area = search_area.area
        return (overlap_area / search_area_area) * 100
    return 0

def is_search_area_within_raster(df, spatial_filter):
    """
    """
    search_area = box(
        spatial_filter['lowerLeft']['longitude'], spatial_filter['lowerLeft']['latitude'],
        spatial_filter['upperRight']['longitude'], spatial_filter['upperRight']['latitude']
    )

    results = []
    overlap_percentages = []

    for index, row in df.iterrows():
        raster_coverage = Polygon(row['spatialCoverage.coordinates'][0])
        if search_area.within(raster_coverage):
            results.append('within')
            overlap_percentages.append(calc_overlap_percentage(search_area, raster_coverage))
        elif search_area.intersects(raster_coverage):
            results.append('partially within')
            overlap_percentages.append(calc_overlap_percentage(search_area, raster_coverage))
        else:
            results.append('outside')
            overlap_percentages.append(calc_overlap_percentage(search_area, raster_coverage))
    
    df['search_area_status'] = results
    df['overlap_percentage'] = overlap_percentages
    print(overlap_percentages)





is_search_area_within_raster(usgs_connection.post_fire_results, usgs_connection.spatialFilter)
is_search_area_within_raster(usgs_connection.pre_fire_results, usgs_connection.spatialFilter)

print(usgs_connection.pre_fire_results)
print(usgs_connection.post_fire_results)
# %%





