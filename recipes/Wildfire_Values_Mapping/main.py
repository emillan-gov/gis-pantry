# %% Import Required Modules
import arcpy
import pandas as pd
import math
from datetime import datetime
import os

sys.path.append(r'\\spatialfiles.bcgov\work\srm\sry\Local\scripts\python')
from sc_python_function_library import * 

# Set up the environment
workspace = r"\\spatialfiles.bcgov\work\srm\sry\Workarea\emillan\!PythonTools\Wildfire\Workspace_71W25GA\Strategic_Plan_Work.gdb"
arcpy.env.workspace = workspace
arcpy.env.overwriteOutput = True
arcpy.env.parallelProcessingFactor = "100%"
oracle_username, oracle_password, agol_username, agol_password = get_credentials(
  "oracle", "agol")

# %% Connect to Oracle FC
bcgw_connection_name = r"\\spatialfiles.bcgov\work\srm\sry\Workarea\emillan\!PythonTools\Wildfire\Workspace_71W25GA\bcgw_connection.sde"
fc = f"{bcgw_connection_name}\\WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP"
create_bcgw_connection(bcgw_connection_name, oracle_username, oracle_password)

# config
values_fc = f"{workspace}/StrategicValues_Jul11"
output_fc = f"{workspace}/perimeter_10km"
out_name = "Test_Output_Name"
fire_layer = f"{workspace}/Fires_July16"
pizza_slice_fc = f"{workspace}/PizzaSlices"
fire_fc = f"{bcgw_connection_name}\\WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP"
spatial_reference = arcpy.Describe(values_fc).spatialReference
fields_to_update = ["North_Risk", "East_Risk", "South_Risk", "West_Risk"]

def create_output_fc(input_name):
    """
    creates FC to build slices and rainbows
    """
    arcpy.management.CreateFeatureclass(arcpy.env.workspace, input_name, "POLYGON")
    arcpy.management.AddField(input_name, "Direction", "TEXT")
    arcpy.management.AddField(input_name, "Distance", "TEXT")
    arcpy.management.AddField(input_name, "Value", "TEXT")

def point_from_bearing_and_distance(origin, angle, distance):
    angle_rad = math.radians(angle)  # Convert angle from degrees to radians
    dx = distance * math.cos(angle_rad)
    dy = distance * math.sin(angle_rad)
    return [origin[0] + dx, origin[1] + dy]

def process_points(distance, input_fc, values_fc_erased):
    """
    """
    # Define angles for each cardinal direction including wrap-around for north
    directions = {"east": (315, 45), "north": (45, 135), "west": (135, 225), "south": (225, 315)}

    # Process each point in the input feature class
    with arcpy.da.SearchCursor(values_fc_erased, ['SHAPE@XY', "Label"]) as cursor:
        with arcpy.da.InsertCursor(input_fc, ['SHAPE@', 'Direction', 'Distance', 'Value']) as insert_cursor:
            for point in cursor:
                input_point = point[0]
                point_label = point[1]
                for direction, (start_angle, end_angle) in directions.items():
                    points = [input_point]  # Start with the input point
                    # Handle wrap-around for angles
                    if start_angle > end_angle:  # This checks if the range crosses the zero line
                        angles = list(range(start_angle, 360)) + list(range(0, end_angle + 1))
                    else:
                        angles = range(start_angle, end_angle + 1)

                    # Generate points for each degree within the defined range
                    for angle in angles:
                        new_point = point_from_bearing_and_distance(input_point, angle, distance)
                        points.append(new_point)

                    points.append(input_point)  # Close the sector polygon by repeating the first point

                    # Create an arcpy array and polygon object
                    array = arcpy.Array([arcpy.Point(*coords) for coords in points])
                    polygon = arcpy.Polygon(array, spatial_reference)
                    
                    # Insert the new polygon into the feature class
                    insert_cursor.insertRow([polygon, direction, str(distance), point_label])
                    print(f"Created {direction} slice for point {point_label}")
                    print("Checking for Overlap with Fires...")

def create_feature_class_copy(input_fc):
    # Get the current date and time
    current_time = datetime.now()
    # Format the date and time as MMDDHHMM
    formatted_time = current_time.strftime('%m%d%H%M')
    # Create the new feature class name
    output_fc = f"{input_fc}_copy_{formatted_time}"
    
    try:
        # Copy the feature class
        arcpy.CopyFeatures_management(input_fc, output_fc)
        print(f"Feature class copied successfully to {output_fc}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return output_fc

def erase_by_label(feature_class_one, feature_class_two, output_feature_class):
    # Set environment settings
    arcpy.env.overwriteOutput = True
    
    # Create a feature layer for each input feature class
    arcpy.MakeFeatureLayer_management(feature_class_one, "fc1_layer")
    arcpy.MakeFeatureLayer_management(feature_class_two, "fc2_layer")
    
    # Get unique labels from feature class one
    labels = [row[0] for row in arcpy.da.SearchCursor(feature_class_one, ["Value"])]
    unique_labels = set(labels)
    
    # List to store intermediate results
    intermediate_results = []
    counter = 0
    for label in unique_labels:
        # Create SQL query to select polygons with the same label
        query = f"Value = '{label}'"
        
        # Select polygons from feature class one with the current label
        arcpy.SelectLayerByAttribute_management("fc1_layer", "NEW_SELECTION", query)
        
        # Select polygons from feature class two with the current label
        arcpy.SelectLayerByAttribute_management("fc2_layer", "NEW_SELECTION", query)
        
        # Perform the erase operation
        temp_output = f"erase_{counter}"
        arcpy.Erase_analysis("fc1_layer", "fc2_layer", temp_output)
        intermediate_results.append(temp_output)
        counter +=1

    # Merge all intermediate results into the final output feature class
    arcpy.Merge_management(intermediate_results, output_feature_class)
    
    # Cleanup intermediate results
    for temp in intermediate_results:
        arcpy.Delete_management(temp)

    # Clean up layers
    arcpy.Delete_management("fc1_layer")
    arcpy.Delete_management("fc2_layer")
    
    print("Erase by label operation completed successfully.")

def get_unique_values(fc1, fc2, field):
    values = set()
    with arcpy.da.SearchCursor(fc1, [field]) as cursor:
        for row in cursor:
            values.add(row[0])

    with arcpy.da.SearchCursor(fc2, [field]) as cursor:
        for row in cursor:
            values.add(row[0])

    return values

def get_direction_risk(pizza, rainbow, value, direction):
    """
    """
    Result = "Low Risk"

    with arcpy.da.SearchCursor(rainbow, ["Value", "Direction"]) as Medium_Risk_Cursor:
        for row in Medium_Risk_Cursor:
            if row[0] == value and row[1].lower() == direction:
                Result = "Medium Risk"
    del Medium_Risk_Cursor, row

    with arcpy.da.SearchCursor(pizza, ["Value", "Direction"]) as High_Risk_Cursor:
        for row in High_Risk_Cursor:
            if row[0] == value and row[1].lower() == direction:
                Result = "High Risk"

    return Result

def copy_feature_class_with_date_suffix(input_fc, output_workspace, fields_to_update):
    # Get the current date in the format YYYYMMDD
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Extract the name of the input feature class (without path)
    fc_name = arcpy.Describe(input_fc).baseName
    
    # Construct the new feature class name
    new_fc_name = f"{fc_name}_RiskData_{current_date}"
    
    # Construct the full path for the new feature class
    output_fc = arcpy.management.CreateFeatureclass(output_workspace, new_fc_name)
    
    # Copy the feature class
    arcpy.management.CopyFeatures(input_fc, output_fc)

    for field in fields_to_update:
        arcpy.management.AddField(output_fc, field, "TEXT")

    print(f"Feature class copied to: {output_fc}")
    return output_fc
# %% This is Step one. It takes an input shape (Your values Tool) and then Creates your search area.
# The smaller 5km "Slices" and the larger 10km "Rainbows" - DON'T RERUN UNLESS NECESSARY...IT CAN TAKE QUITE A WHILE...
arcpy.analysis.Erase(values_fc, fire_fc, "un_burnt_values")

create_output_fc(f"search_quadrants_5km")
process_points(5000,f"{workspace}\\search_quadrants_5km","un_burnt_values")

create_output_fc(f"search_quadrants_10km")
process_points(10000,f"{workspace}\\search_quadrants_10km","un_burnt_values")

erase_by_label(f"{workspace}\\search_quadrants_10km", f"{workspace}\\search_quadrants_5km", "Rainbows")
# %% Perform the Fire Analysis:
arcpy.analysis.Intersect([f"{workspace}\\search_quadrants_5km", fire_fc], "Pizza_Slice_Intersects")
arcpy.analysis.Intersect([f"{workspace}\\Rainbows", fire_fc], "Rainbow_Intersects")

results_dict = {}

# %% Create Risk Matrix
unique_ids = get_unique_values("Rainbow_Intersects", "Pizza_Slice_Intersects", "Value")
directions = ["north", "east", "south", "west"]

output_dataset = copy_feature_class_with_date_suffix("un_burnt_values", arcpy.env.workspace, fields_to_update)
# %%
for value in unique_ids:
    temp_risk_list = []
    for direction in directions:
        temp_risk_list.append(get_direction_risk("Pizza_Slice_Intersects", "Rainbow_Intersects", value, direction))

    print(temp_risk_list)

    with arcpy.da.UpdateCursor(output_dataset, fields_to_update+["Label"]) as cursor:
        for row in cursor:
            if row[4] == value:
                row[0] = temp_risk_list[0]
                row[1] = temp_risk_list[1]
                row[2] = temp_risk_list[2]
                row[3] = temp_risk_list[3]
                cursor.updateRow(row)
    
    with arcpy.da.UpdateCursor(output_dataset, fields_to_update+["Label"]) as cursor:
        for row in cursor:
            for i in range(len(fields_to_update)):
                # If the field value is None or "NULL", replace it with "Low Risk"
                if row[i] is None or row[i] == "NULL":
                    row[i] = "Low Risk"
            # Update the row
            cursor.updateRow(row)


print("")
print("----------------------------")
print("RISK VALUE LAYER COMPLETE")




# %%
def feature_class_to_excel(feature_class, output_excel):
    """
    Export attributes of an ESRI Feature Class to an Excel table.
    
    :param feature_class: Path to the input feature class
    :param output_excel: Path to the output Excel file
    """
    # Create a list to hold the field names
    fields = [field.name for field in arcpy.ListFields(feature_class) if field.type not in ('Geometry')]

    # Use a SearchCursor to iterate through the feature class and collect the data
    data = []
    with arcpy.da.SearchCursor(feature_class, fields) as cursor:
        for row in cursor:
            data.append(row)

    # Create a DataFrame from the collected data
    df = pd.DataFrame.from_records(data, columns=fields)

    # Write the DataFrame to an Excel file
    df.to_excel(output_excel, index=False)

def feature_class_to_shapefile(feature_class, output_shapefile):
    """
    Export an ESRI Feature Class to a shapefile.
    
    :param feature_class: Path to the input feature class
    :param output_shapefile: Path to the output shapefile
    """
    # Ensure the output directory exists
    output_dir = os.path.dirname(output_shapefile)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Use arcpy to copy the feature class to a shapefile
    arcpy.FeatureClassToShapefile_conversion(feature_class, output_dir)

def create_kmls(input_fc, fields, output_kmz_directory):
    """
    """
    risk_list = ["High Risk", "Medium Risk", "Low Risk"]
    for direction in fields:
        for risk in risk_list:
                    
            where_clause = f"{direction} = '{risk}'"

            current_date = datetime.now().strftime("%Y%m%d")
            risk_string = risk.replace(" ","")
            output_layer = f"{direction}_{risk_string}_{current_date}"


            arcpy.management.MakeFeatureLayer(
                in_features=input_fc,
                out_layer=output_layer,
                where_clause= where_clause,
                workspace=None,
                field_info="OBJECTID OBJECTID VISIBLE NONE;Shape Shape VISIBLE NONE;Fire_Num Fire_Num VISIBLE NONE;Label Label VISIBLE NONE;Comments Comments VISIBLE NONE;Latitude_D Latitude_D VISIBLE NONE;Longitude_ Longitude_ VISIBLE NONE;Status Status VISIBLE NONE;Feature_Ty Feature_Ty VISIBLE NONE;Feature_De Feature_De VISIBLE NONE;Name Name VISIBLE NONE;Type Type VISIBLE NONE;July5_W_ID July5_W_ID VISIBLE NONE;Jul11_W_ID Jul11_W_ID VISIBLE NONE;SymbolType SymbolType VISIBLE NONE;TableName TableName VISIBLE NONE;Rank Rank VISIBLE NONE;Label_2 Label_2 VISIBLE NONE;North_Risk North_Risk VISIBLE NONE;East_Risk East_Risk VISIBLE NONE;South_Risk South_Risk VISIBLE NONE;West_Risk West_Risk VISIBLE NONE"
            )

            output_kmz = f"{output_kmz_directory}\\{output_layer}.kmz"

            arcpy.conversion.LayerToKML(output_layer, output_kmz)



# Example usage
output_dataset = ""
base_directory = r"\\bcwsdata.nrs.bcgov\Incident$\G-Prince_George\2024\G9 Fort Nelson\71W25GA\Plans\Technical Specialists\Maps\Values_Modelling"
feature_class = f"{workspace}\\un_burnt_values_RiskData_20240720"
output_excel = f"{base_directory}\\Wildfiretest_analysis2.xlsx"
output_shapefile = f"{base_directory}\\test_export.shp"
output_kmz_directory = f"{base_directory}\\test_export2.kmz"


# feature_class_to_excel(feature_class, output_excel)
# feature_class_to_shapefile(output_dataset, output_shapefile)
create_kmls(feature_class, fields_to_update, base_directory)
