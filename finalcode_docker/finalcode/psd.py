

import requests
import secret
import json
import csv
from dateutil.relativedelta import relativedelta
import os
import datetime
import pandas as pd
import paramiko

# import boto3

# ssm=boto3.client('ssm')
# param=ssm.get_parameter


def psd():
    with open('orion_property_dimensions_import_trial.csv','w',newline='') as csvfile:
        firstrow=csv.writer(csvfile)
        firstrow.writerow(['CommonCustomTables'])

    client_id=secret.client_id
    client_secret=secret.client_secret

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type': 'client_credentials'
    }
    response = requests.post('http://172.26.254.17/api/authorize', headers=headers, data=data).json()
    access_token=response['access_token']

    headers = {
        'authorization': 'Bearer '+access_token,
        'cache-control': 'no-cache',
        'content-type': 'application/json',
    }

    yesterday=datetime.datetime.today()-datetime.timedelta(days=1)
    yesterday_date=yesterday.strftime('%m/%d/%Y')

    data = '{ "Dates":["'+yesterday_date+'"],"Date_filter_type": "0", "Display_field": [ "property_name","accounting_manager","acquisition_origin", "affordable_comp","affordable_financed","affordable_population_served","hmy_number","affordable_unit","ap_specialist_name","application_portal",	"archive_disposition_date",	"archive_status","asbestos_lead_paint","bcc_project","bdc_project","billed_back_utilities_residential",	"bmc_start_date_year","bms_building_management_system","building_amenities","building_type","call_center_plan_type","call_center_type","call_tracking_system","carbon_monoxide_residential",	"central_hvac_system_type",	"city",	"collect_agency",	"commercial_office_sf",	"cooling_tower_submeter",	"corridor_light_bulb_type",	"county",	"credit_screen",	"date_of_stabilization",	"developer_name",	"director_of_accounting",	"disposition_type",	"domestic_hot_water_fuel_source_residential",	"domestic_hot_water_source_residential",	"domestic_water_booster_pump",	"dry_fire_sprinkler_system",	"e_contract",	"e_tool",	"elan_id",	"electrical_utility_service_provider",	"elevator_system",	"emergency_generator_fuel_source",	"exterior_light_sensor_on_off",	"exterior_light_type",	"fire_pump_fuel_type",	"fire_sprinklers_common_area",	"fire_sprinklers_residential",	"first_move_date",	"fiscal_year_start_month",	"fuel_oil_storage_tank",	"garage_light_bulb_type",	"garage_light_sensor_on_off",	"green_cert",	"grill_fuel_source",	"grills",	"guest_suite_nightly_feeney",	"guest_suites_at_property",	"hvac_boiler_fuel_source",	"hvac_refrigerant_type",	"hvac_system_amenity",	"hvac_system_corridor",	"hvac_system_residential",	"hvac_system_residential_fuel_source",	"hvac_system_sub_metered_utilities",	"hycard",	"indoor_fireplace_fuel_source",	"insurance_required",	"irrigated_area_sf",	"irrigation_submetering",	"irrigation_system",	"lead_management_system",	"lease_exeportal",	"leed_building",	"majority_owner_name",	"managing_director",	"market_type",	"marketing_coordinator_name",	"marketing_director",	"marketing_manager",	"metro_region",	"min_owner_name",	"min_owner_name2",	"min_owner_name3",	"natural_gas_utility_service_provider",	"neighborhood",	"ng_contract",	"noofunits",	"num_of_commercial_office_spaces",	"num_of_courtyards",	"num_of_electric_vehicle_charging_stations",	"num_of_retail_commercial_parking_spaces",	"online_rent_pay_process",	"on_site_energy_generation",	"open_date",	"outdoor_fireplace_fuel_source",	"package_locker_system",	"parking_spaces_num",	"parking_type",	"pets_allowed",	"pool",	"pool_submeter",	"pre_lease_start_date",	"pricing_and_availability_system",	"project_manager_name",	"property_accountant",	"property_has_retail",	"property_manager_name",	"property_status",	"raw_sewage_ejector_system",	"regional_manager",	"rentersinsurance",	"resident_pays_these_utilities_directly",	"resportal",	"retail_bays",	"retail_lease_analyst",	"retail_accessible_garage_electric_submetering",	"retail_accessible_garage_water_submetering",	"retailsf",	"retail_parking_space",	"rev_mgmt",	"rev_mgt_system",	"sewer_utility_service_provider",	"smartrent",	"smoke_detectors_residential",	"smoke_free",	"spill_prevention_control_and_containment_plan",	"state",	"storm_water_management_vaults",	"submetered_by_property_residential",	"svp_name",	"takeover_date","telecom_service_provider",	"tom_owner_ship","total_sq_ft_common_area","total_sq_ft_community_room","total_sq_ft_corridor_space","total_sq_ft_fitness_center","total_sq_ft_lobby","total_sq_ft_office","total_sq_ft_outdoor_space","unit_bulb_type","utility_biller","utility_company_metered_residential","water_utility_service_provider","website_type","year_built","zipcode" ]}'

    def operations(data):
        response = requests.post('http://172.26.254.17/api/property', headers=headers, data=data).json()

        response_dict_to_string=json.dumps(response)
        response_parse = json.loads(response_dict_to_string)

        list_len=len(response_parse['result'][yesterday_date]['properties'])
        response_list=[]

        for i in range(0,list_len):
            dict_filter = response_parse['result'][yesterday_date]['properties'][i]['fields']
            dict_elan_filter=response_parse['result'][yesterday_date]['properties'][i]['elan_id']
            dict_filter['elan_id']=dict_elan_filter
            response_list.append(dict(dict_filter))

        json_to_df=pd.DataFrame.from_dict(response_list,orient='columns')

        #reordering property_name to be the first column 

        del json_to_df['company']
        del json_to_df['date']
        return json_to_df

    json_to_df=operations(data)

    yearsago_list=[]
    for years in range(1,6):
        yearsago=datetime.datetime.today()-datetime.timedelta(days=1)
        yearsago=yearsago-relativedelta(years=years)
        yearsago_date=yearsago.strftime('%m/%d/%Y')
        yearsago_list.append(yearsago_date)


    yearloop_ctr=0
    for yearloop in yearsago_list:
        data_year_ago_stab='{ "Dates":["'+yesterday_date+'","'+yearloop+'"],"Date_filter_type": "0", "Filter_field_values":[{"archive_status":"NO","property_status":"Stabilized","filter_field_type":"true"}],"Same_store":"true", "Display_field": ["archive_status","property_status"]}'  
        json_to_df_year_stab=operations(data_year_ago_stab)
        data_year_ago_stab_lease='{ "Dates":["'+yesterday_date+'","'+yearloop+'"],"Date_filter_type": "0", "Filter_field_values":[{"archive_status":"NO","property_status":["Stabilized","Leaseup"],"filter_field_type":"true"}],"Same_store":"true", "Display_field": ["archive_status","property_status"]}'
        json_to_df_year_leaseup=operations(data_year_ago_stab_lease)

        yearloop_ctr+=1

        if(yearloop_ctr==1):
            json_to_df_year_stab['samestore_1yr']='True'
            json_to_df_year_leaseup['samestore_1yr_lease']='True'
        if(yearloop_ctr==2):
            json_to_df_year_stab['samestore_2yr']='True'
            json_to_df_year_leaseup['samestore_2yr_lease']='True'
        if(yearloop_ctr==3):
            json_to_df_year_stab['samestore_3yr']='True'
            json_to_df_year_leaseup['samestore_3yr_lease']='True'
        if(yearloop_ctr==4):
            json_to_df_year_stab['samestore_4yr']='True'
            json_to_df_year_leaseup['samestore_4yr_lease']='True'
        if(yearloop_ctr==5):
            json_to_df_year_stab['samestore_5yr']='True'
            json_to_df_year_leaseup['samestore_5yr_lease']='True'

        json_to_df=pd.merge(json_to_df,json_to_df_year_stab,how='left')
        json_to_df=pd.merge(json_to_df,json_to_df_year_leaseup,how='left')

        if(yearloop_ctr==1):
            json_to_df['samestore_1yr'].fillna('False',inplace=True)
            json_to_df['samestore_1yr_lease'].fillna('False',inplace=True)
        if(yearloop_ctr==2):
            json_to_df['samestore_2yr'].fillna('False',inplace=True)
            json_to_df['samestore_2yr_lease'].fillna('False',inplace=True)
        if(yearloop_ctr==3):
            json_to_df['samestore_3yr'].fillna('False',inplace=True)
            json_to_df['samestore_3yr_lease'].fillna('False',inplace=True)
        if(yearloop_ctr==4):
            json_to_df['samestore_4yr'].fillna('False',inplace=True)
            json_to_df['samestore_4yr_lease'].fillna('False',inplace=True)
        if(yearloop_ctr==5):
            json_to_df['samestore_5yr'].fillna('False',inplace=True)
            json_to_df['samestore_5yr_lease'].fillna('False',inplace=True)

    json_to_df["TableName"]=" "

    cols=list(json_to_df.columns)
    cols=[cols[-1]]+[cols[59]]+[cols[110]]+[cols[151]]+[cols[153]]+[cols[155]]+[cols[157]]+[cols[159]]+[cols[152]]+[cols[154]]+[cols[156]]+[cols[158]]+[cols[160]]+cols[0:59]+cols[60:110]+cols[111:151]
    json_to_df=json_to_df[cols]

    #Renaming cols
    json_to_df.columns=[
                        "TableName",
                        "PropertyId",
                        "ComplexName",
                        "SameStore_Stabilized_1yr",
                        "SameStore_Stabilized_2yr",
                        "SameStore_Stabilized_3yr",
                        "SameStore_Stabilized_4yr",
                        "SameStore_Stabilized_5yr",
                        "SameStore_Portfolio_1yr",
                        "SameStore_Portfolio_2yr",
                        "SameStore_Portfolio_3yr",
                        "SameStore_Portfolio_4yr",
                        "SameStore_Portfolio_5yr",
                        "ACCOUNTING MANAGER",
                        "ACQUISITION ORIGIN",
                        "AFFORDABLE COMP",
                        "AFFORDABLE FINANCED",
                        "AFFORDABLE POPULATION SERVED",
                        "AFFORDABLE UNIT",
                        "AP SPECIALIST NAME",
                        "APPLICATION PORTAL",
                        "ARCHIVE DISPOSITION DATE",
                        "ARCHIVE STATUS",
                        "ASBESTOS LEAD PAINT",
                        "BCC PROJECT",
                        "BDC PROJECT",
                        "BILLED BACK UTILITIES RESIDENTIAL",
                        "BMC START DATE YEAR",
                        "BMS BUILDING MANAGEMENT SYSTEM",
                        "BUILDING AMENITIES",
                        "BUILDING TYPE",
                        "CALL CENTER PLAN TYPE",
                        "CALL CENTER TYPE",
                        "CALL TRACKING SYSTEM",
                        "CARBON MONOXIDE RESIDENTIAL",
                        "CENTRAL HVAC SYSTEM TYPE",
                        "CITY",
                        "COLLECT AGENCY",
                        "COMMERCIAL OFFICE SF",
                        "COOLING TOWER SUBMETER",
                        "CORRIDOR LIGHT BULB TYPE",
                        "COUNTY",
                        "CREDIT SCREEN",
                        "DATE OF STABILIZATION",
                        "DEVELOPER NAME",
                        "DIRECTOR OF ACCOUNTING",
                        "DISPOSITION TYPE",
                        "DOMESTIC HOT WATER FUEL SOURCE RESIDENTIAL",
                        "DOMESTIC HOT WATER SOURCE RESIDENTIAL",
                        "DOMESTIC WATER BOOSTER PUMP",
                        "DRY FIRE SPRINKLER SYSTEM",
                        "E CONTRACT",
                        "E TOOL",
                        "ELAN ID",
                        "ELECTRICAL UTILITY SERVICE PROVIDER",
                        "ELEVATOR SYSTEM",
                        "EMERGENCY GENERATOR FUEL SOURCE",
                        "EXTERIOR LIGHT SENSOR ON OFF",
                        "EXTERIOR LIGHT TYPE",
                        "FIRE PUMP FUEL TYPE",
                        "FIRE SPRINKLERS COMMON AREA",
                        "FIRE SPRINKLERS RESIDENTIAL",
                        "FIRST MOVE DATE",
                        "FISCAL YEAR START MONTH",
                        "FUEL OIL STORAGE TANK",
                        "GARAGE LIGHT BULB TYPE",
                        "GARAGE LIGHT SENSOR ON OFF",
                        "GREEN CERT",
                        "GRILL FUEL SOURCE",
                        "GRILLS",
                        "GUEST SUITE NIGHTLY FEENEY",
                        "GUEST SUITES AT PROPERTY",
                        "HVAC BOILER FUEL SOURCE",
                        "HVAC REFRIGERANT TYPE",
                        "HVAC SYSTEM AMENITY",
                        "HVAC SYSTEM CORRIDOR",
                        "HVAC SYSTEM RESIDENTIAL",
                        "HVAC SYSTEM RESIDENTIAL FUEL SOURCE",
                        "HVAC SYSTEM SUB METERED UTILITIES",
                        "HYCARD",
                        "INDOOR FIREPLACE FUEL SOURCE",
                        "INSURANCE REQUIRED",
                        "IRRIGATED AREA SF",
                        "IRRIGATION SUBMETERING",
                        "IRRIGATION SYSTEM",
                        "LEAD MANAGEMENT SYSTEM",
                        "LEASE EXEPORTAL",
                        "LEED BUILDING",
                        "MAJORITY OWNER NAME",
                        "MANAGING DIRECTOR",
                        "MARKET TYPE",
                        "MARKETING COORDINATOR NAME",
                        "MARKETING DIRECTOR",
                        "MARKETING MANAGER",
                        "METRO REGION",
                        "MIN OWNER NAME",
                        "MIN OWNER NAME2",
                        "MIN OWNER NAME3",
                        "NATURAL GAS UTILITY SERVICE PROVIDER",
                        "NEIGHBORHOOD",
                        "NG CONTRACT",
                        "NOOFUNITS",
                        "NUM OF COMMERCIAL OFFICE SPACES",
                        "NUM OF COURTYARDS",
                        "NUM OF ELECTRIC VEHICLE CHARGING STATIONS",
                        "NUM OF RETAIL COMMERCIAL PARKING SPACES",
                        "ONLINE RENT PAY PROCESS",
                        "ON SITE ENERGY GENERATION",
                        "OPEN DATE",
                        "OUTDOOR FIREPLACE FUEL SOURCE",
                        "PACKAGE LOCKER SYSTEM",
                        "PARKING SPACES NUM",
                        "PARKING TYPE",
                        "PETS ALLOWED",
                        "POOL",
                        "POOL SUBMETER",
                        "PRE LEASE START DATE",
                        "PRICING AND AVAILABILITY SYSTEM",
                        "PROJECT MANAGER NAME",
                        "PROPERTY ACCOUNTANT",
                        "PROPERTY HAS RETAIL",
                        "PROPERTY MANAGER NAME",
                        "PROPERTY STATUS",
                        "RAW SEWAGE EJECTOR SYSTEM",
                        "REGIONAL MANAGER",
                        "RENTERSINSURANCE",
                        "RESIDENT PAYS THESE UTILITIES DIRECTLY",
                        "RESPORTAL",
                        "RETAIL BAYS",
                        "RETAIL LEASE ANALYST",
                        "RETAIL ACCESSIBLE GARAGE ELECTRIC SUBMETERING",
                        "RETAIL ACCESSIBLE GARAGE WATER SUBMETERING",
                        "RETAILSF",
                        "RETAIL PARKING SPACE",
                        "REV MGMT",
                        "REV MGT SYSTEM",
                        "SEWER UTILITY SERVICE PROVIDER",
                        "SMARTRENT",
                        "SMOKE DETECTORS RESIDENTIAL",
                        "SMOKE FREE",
                        "SPILL PREVENTION CONTROL AND CONTAINMENT PLAN",
                        "STATE",
                        "STORM WATER MANAGEMENT VAULTS",
                        "SUBMETERED BY PROPERTY RESIDENTIAL",
                        "SVP NAME",
                        "TAKEOVER DATE",
                        "TELECOM SERVICE PROVIDER",
                        "TOM OWNER SHIP",
                        "TOTAL SQ FT COMMON AREA",
                        "TOTAL SQ FT COMMUNITY ROOM",
                        "TOTAL SQ FT CORRIDOR SPACE",
                        "TOTAL SQ FT FITNESS CENTER",
                        "TOTAL SQ FT LOBBY",
                        "TOTAL SQ FT OFFICE",
                        "TOTAL SQ FT OUTDOOR SPACE",
                        "UNIT BULB TYPE",
                        "UTILITY BILLER",
                        "UTILITY COMPANY METERED RESIDENTIAL",
                        "WATER UTILITY SERVICE PROVIDER",
                        "WEBSITE TYPE",
                        "YEAR BUILT",
                        "ZIPCODE"]

    with open('orion_property_dimensions_import_trial.csv', 'a') as append_to_csv:
        json_to_df.to_csv(append_to_csv, index=False)
    
    host="addsftp.bozzutolink.com"
    port=22
    transport=paramiko.Transport((host,port))

    username=secret.username
    password=secret.password
    transport.connect(username=username,password=password)
    sftp=paramiko.SFTPClient.from_transport(transport)

    filepath='orion_property_dimensions_import.csv'
    localpath='orion_property_dimensions_import_trial.csv'
    sftp.put(localpath,filepath)
    sftp.close()
    transport.close()
    os.remove('orion_property_dimensions_import_trial.csv')
