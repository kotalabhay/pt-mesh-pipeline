from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import requests
import json
import base64
import time
from geopy.geocoders import Nominatim

import csv
import glob



# =============================================================================
# file = open("493215_2021.xml", "r")
# contents = file.read()
# soup = BeautifulSoup(contents, 'xml')
# final_data = pd.DataFrame()
# 
# 
# #texts = str(soup.findAll(text=True)).replace('\\n','')
# 
# try:
#     
#     reception_id= soup.find('RECEPTION_ID')
#     reception_id_data =reception_id.get_text() 
#     doc_id= soup.find('URI_DOC')
#     doc_id_data=doc_id.get_text()
#     doc_id_data=re.search(r'NOTICE:(.*?):TEXT', doc_id_data).group(1)
#     
#     cpv_code= soup.find('CPV_MAIN')
#     cpv_code= str(cpv_code)
#     cpv_code =''.join(filter(lambda i: i.isdigit(), cpv_code ))
#     
#     final_data.at[0,'Doc ID']= doc_id_data
#     final_data.at[0,'Reception ID']= reception_id_data
#     final_data.at[0,'CPV CODE']= cpv_code
# 
#     
# 
#     #cpv_code = re.findall('[0-9]+', cpv_code)
# 
# except Exception :
#     pass    
# 
# 
# 
# basic_data= soup.find('ML_TI_DOC')
# 
# try:
#     if basic_data:
#         
# 
#         country_iso=soup.find("ML_TI_DOC", { "LG" : "EN" }).find("TI_CY", recursive=True)
#         country_iso= country_iso.get_text() 
#         town=soup.find("ML_TI_DOC", { "LG" : "EN" }).find("TI_TOWN", recursive=True)
#         town= town.get_text()  
#         project_name=soup.find("ML_TI_DOC", { "LG" : "EN" }).find("TI_TEXT", recursive=False)
#         project_name= project_name.get_text() 
# 
#         
#         final_data.at[0,'Country ISO']= country_iso
#         final_data.at[0,'TOWN ISO']=  town
#         final_data.at[0,'Project Name']= project_name
#     else:
#         pass
# 
# except Exception:
#     pass
# 
# 
# 
# form_data= soup.find('F03_2014')
# 
# 
# try:
#     if form_data:    
#         description= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("SHORT_DESCR", recursive=True)
#         description= description.get_text()
#         postal_code= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("POSTAL_CODE", recursive=True)
#         postal_code= postal_code.get_text()   
#         email= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("E_MAIL", recursive=True)
#         email= email.get_text()
#         amount= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("VAL_TOTAL", recursive=True)
#         currency= re.findall(r'"(.*?)"', str(amount))
#         amount= amount.get_text()   
#         og_notice_no= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("NOTICE_NUMBER_OJ", recursive=True)
#         og_notice_no= og_notice_no.get_text()   
#         
#         
#         
#         final_data.at[0,'Description']= description
#         final_data.at[0,'Postal Code']= postal_code
#         final_data.at[0,'Email']= email
#         final_data.at[0,'Amount']= amount
#         final_data.at[0,'Currency']= currency
#         final_data.at[0,'Og Notice No']= currency
# 
#     else:
#         pass        
# except Exception:
#     pass
# 
# 
#     
#     ###### WEB Scrapping For Additional Information #########
#     
#     
#     ##### Scrapping Of Tab ######
# try:  
#     url='https://ted.europa.eu/udl?uri=TED:NOTICE:{0}:DATA:EN:HTML&src=0&tabId=3'.format(doc_id_data)
#     
#     response = requests.get(url)
#     
#     html_soup = BeautifulSoup(response.content, 'html.parser')
#     
#     table_data = pd.read_html(response.text) # this parses all the tables in webpages to a list
#     table_df = table_data[0]
#     # =============================================================================
#      #table_df_copy=table_df.drop(table_df.columns[[0]],axis=1)
#     # =============================================================================
#     table_df.columns =['Symbol','Header','Values']
#     
#     
#     if table_df.empty:
#         print("Empty Data Frame")
#  
#     else:
#             final_data.at[0,'Title']= table_df.loc[table_df['Symbol'] == 'TI','Values'].values
#             final_data.at[0,'Notice publication number']= table_df.loc[table_df['Symbol'] == 'ND','Values'].values
#             final_data.at[0,'Publication date']= table_df.loc[table_df['Symbol'] == 'PD','Values'].values
#             final_data.at[0,'OJ S issue number']= table_df.loc[table_df['Symbol'] == 'OJ','Values'].values
#             final_data.at[0,'Town/city of the buyer']= table_df.loc[table_df['Symbol'] == 'TW','Values'].values
#             final_data.at[0,'Official name of the buyer']= table_df.loc[table_df['Symbol'] == 'AU','Values'].values
#             final_data.at[0,'Original language']= table_df.loc[table_df['Symbol'] == 'OL','Values'].values
#             final_data.at[0,'Country of the buyer']= table_df.loc[table_df['Symbol'] == 'CY','Values'].values
#             final_data.at[0,'Type of buyer']= table_df.loc[table_df['Symbol'] == 'AA','Values'].values
#             final_data.at[0,'EU institution/agency']= table_df.loc[table_df['Symbol'] == 'HA','Values'].values
#             final_data.at[0,'Document sent']= table_df.loc[table_df['Symbol'] == 'DS','Values'].values
#             final_data.at[0,'Type of contract']= table_df.loc[table_df['Symbol'] == 'NC','Values'].values
#             final_data.at[0,'Type of procedure']= table_df.loc[table_df['Symbol'] == 'PR','Values'].values
#             final_data.at[0,'Notice type']= table_df.loc[table_df['Symbol'] == 'TD','Values'].values
#             final_data.at[0,'Regulation']= table_df.loc[table_df['Symbol'] == 'RP','Values'].values
#             final_data.at[0,'Type of bid']= table_df.loc[table_df['Symbol'] == 'TY','Values'].values
#             final_data.at[0,'Award criteria']= table_df.loc[table_df['Symbol'] == 'AC','Values'].values
#             final_data.at[0,'Common procurement vocabulary (CPV)']= table_df.loc[table_df['Symbol'] == 'PC','Values'].values
#             final_data.at[0,'Place of performance (NUTS)']= table_df.loc[table_df['Symbol'] == 'RC','Values'].values
#             final_data.at[0,'Internet address (URL)']= table_df.loc[table_df['Symbol'] == 'IA','Values'].values
#             final_data.at[0,'Legal basis']= table_df.loc[table_df['Symbol'] == 'DI','Values'].values
#             final_data.at[0,'Deadline Date']= table_df.loc[table_df['Symbol'] == 'DT','Values'].values
#             
# 
# except Exception:
#     pass     
#     
#     ##### Scraping OF Other Tab  ################
#     
#  
# try:
#     
#     url='https://ted.europa.eu/udl?uri=TED:NOTICE:{0}:DATA:EN:HTML&src=0&tabId=4'.format(doc_id_data)
#     
#     response = requests.get(url)
#     
#     html_soup = BeautifulSoup(response.content, 'html.parser')
#     
#     # =============================================================================
#     # table_data = pd.read_html(response.text) # this parses all the tables in webpages to a list
#     # table_df_1 = table_data[0]
#     # table_df_2 =table_data[1]
#     # table_df_3 = table_data[2]
#     # =============================================================================
#     
#     
#     anchor_data = html_soup.find_all("a", {"class": "noBg"})
#     
#     past_doc_list=[]
#     for values in anchor_data:
#         text= values.get_text()
#         doc_ids=text[0:text.find(':')]
#         past_doc_list.append(doc_ids)
#         
#         
# except Exception:
#     pass
# 
# # =============================================================================
# # 
# # #past_doc_list.remove(doc_id_data)
# # try:
# #     final_data.at[0,'Doc ID']= doc_id_data
# #     final_data.at[0,'Reception ID']= reception_id_data
# #     final_data.at[0,'Country ISO']= country_iso
# #     final_data.at[0,'TOWN ISO']=  town
# #     final_data.at[0,'Project Name']= project_name
# #     final_data.at[0,'Description']= description
# #     final_data.at[0,'CPV CODE']= cpv_code
# #     final_data.at[0,'Postal Code']= postal_code
# #     final_data.at[0,'Email']= email
# #     final_data.at[0,'Amount']= amount
# #     final_data.at[0,'Currency']= currency
# #     final_data.at[0,'Og Notice No']= currency
# #     final_data.at[0,'Title']= table_df.loc[table_df['Symbol'] == 'TI','Values'].values
# #     final_data.at[0,'Notice publication number']= table_df.loc[table_df['Symbol'] == 'ND','Values'].values
# #     final_data.at[0,'Publication date']= table_df.loc[table_df['Symbol'] == 'PD','Values'].values
# #     final_data.at[0,'OJ S issue number']= table_df.loc[table_df['Symbol'] == 'OJ','Values'].values
# #     final_data.at[0,'Town/city of the buyer']= table_df.loc[table_df['Symbol'] == 'TW','Values'].values
# #     final_data.at[0,'Official name of the buyer']= table_df.loc[table_df['Symbol'] == 'AU','Values'].values
# #     final_data.at[0,'Original language']= table_df.loc[table_df['Symbol'] == 'OL','Values'].values
# #     final_data.at[0,'Country of the buyer']= table_df.loc[table_df['Symbol'] == 'CY','Values'].values
# #     final_data.at[0,'Type of buyer']= table_df.loc[table_df['Symbol'] == 'AA','Values'].values
# #     final_data.at[0,'EU institution/agency']= table_df.loc[table_df['Symbol'] == 'HA','Values'].values
# #     final_data.at[0,'Document sent']= table_df.loc[table_df['Symbol'] == 'DS','Values'].values
# #     final_data.at[0,'Deadline Date']= table_df.loc[table_df['Symbol'] == 'DT','Values'].values
# #     final_data.at[0,'Type of contract']= table_df.loc[table_df['Symbol'] == 'NC','Values'].values
# #     final_data.at[0,'Type of procedure']= table_df.loc[table_df['Symbol'] == 'PR','Values'].values
# #     final_data.at[0,'Notice type']= table_df.loc[table_df['Symbol'] == 'TD','Values'].values
# #     final_data.at[0,'Regulation']= table_df.loc[table_df['Symbol'] == 'RP','Values'].values
# #     final_data.at[0,'Type of bid']= table_df.loc[table_df['Symbol'] == 'TY','Values'].values
# #     final_data.at[0,'Award criteria']= table_df.loc[table_df['Symbol'] == 'AC','Values'].values
# #     final_data.at[0,'Common procurement vocabulary (CPV)']= table_df.loc[table_df['Symbol'] == 'PC','Values'].values
# #     final_data.at[0,'Place of performance (NUTS)']= table_df.loc[table_df['Symbol'] == 'RC','Values'].values
# #     final_data.at[0,'Internet address (URL)']= table_df.loc[table_df['Symbol'] == 'IA','Values'].values
# #     final_data.at[0,'Legal basis']= table_df.loc[table_df['Symbol'] == 'DI','Values'].values
# # 
# # except Exception:
# #     pass
# # =============================================================================
# 
# 
# ### looping and finding past data
# counter=1
# Pub= 'Publication_Date'
# Dead= 'Deadline'
# Doc= 'Document'
# Auth= 'Authority_Name'
# 
# 
# try:
#     for value in past_doc_list :
#         url='https://ted.europa.eu/udl?uri=TED:NOTICE:{0}:DATA:EN:HTML&src=0&tabId=3'.format(value)
#         response = requests.get(url)
#         #html_soup = BeautifulSoup(response.content, 'html.parser')
#         past_table = pd.read_html(response.text) # this parses all the tables in webpages to a list
#         past_table = past_table[0] 
#         past_table.columns =['Symbol','Header','Values']
#         key= Doc + '_id_' +str(counter)
#         final_data[0,key]=value
#         key= Pub + '_' + str(counter)
#         final_data.at[0,key]=past_table.loc[past_table['Symbol'] == 'PD','Values'].values
#         key= Dead + '_' + str(counter)
#         final_data.at[0,key]= past_table.loc[past_table['Symbol'] == 'DT','Values'].values
#         key= Doc + '_' + str(counter)
#         final_data.at[0,key]= past_table.loc[past_table['Symbol'] == 'TD','Values'].values
#         key= Auth + '_' + str(counter)   
#         final_data.at[0,key]= past_table.loc[past_table['Symbol'] == 'AU','Values'].values
#         counter = counter+1
#         
# except  Exception:
#     pass
#     
#     
# 
#        
#     
#         
# 
# 
# 
# 
# ##### Code For loading Json #######
# 
# 
# #File I/O Open function for read data from JSON File
# with open('response_1634206499904.json') as file_object:
#         # store file data in object
#         data = json.load(file_object)
# print(data)
# 
# text=[]
# base64_output = data['results']
# for key in base64_output:
#     text.append(base64.b64decode(key['content']))
#     
# 
# 
# 
# 
# 
# ####### GET Request To Fetch DOC LIST BASED ON COUNTRY
# 
# 
# 
# 
# r = requests.get('https://ted.europa.eu/api/v2.0/notices/search?fields=ND&pageSize=1000&q=CY%3D%5BFR%5D&reverseOrder=true&scope=2&sortField=ND')
# document_id_list= r.json()
# 
# latest_document_list=document_id_list['results']
# 
# document_id_list=[]
# 
# for dict_data in latest_document_list:
#     document_id_list.append(dict_data['ND'])
# =============================================================================
    

##==================================#####
#  Paramters  
# country_name = Name of the country eg FR,IN
# scope = Active ,Inactive, ALl   Notices eg  2, 1,3
#
###==================================#######

def geolocate(country_name,city_name):
     geolocator = Nominatim(user_agent="tutorial")
     try:
         # Geolocate the center of the country
         time.sleep(1)
         location= city_name + ' ' + country_name
         data = geolocator.geocode(location).raw
         latitude = data["lat"]
         longitude = data["lon"]
         # And return latitude and longitude
         return (latitude, longitude)
     except:
         # Return missing value
# =============================================================================
          location= city_name + ' ' + country_name
          api_key = 'm8gyTg3agkqa5Vr-2a58bw4X9VyuvZDTGq1L3KZpO28' # Acquire from developer.here.com
          URL = "https://geocode.search.hereapi.com/v1/geocode?apikey={0}&q={1}".format(api_key,location)
# =============================================================================

          r = requests.get(url = URL) 
          data = r.json()
          latitude = data['items'][0]['position']['lat']
          longitude = data['items'][0]['position']['lng']
# =============================================================================
          return ( latitude , longitude )
         #return (np.nan,np.nan)



def data_clean(data):
    data=str(data)
    return data.strip("'[]''")

def get_doc_idlist(country_name,scope):
    url='https://ted.europa.eu/api/v2.0/notices/search?fields=ND&pageSize=1000&q=CY%3D%5B{0}%5D&reverseOrder=true&scope={1}&sortField=ND'.format(country_name,scope)
    r = requests.get(url)
    document_id_list= r.json()

    latest_document_list=document_id_list['results']

    document_id_list=[]

    for dict_data in latest_document_list:
        document_id_list.append(dict_data['ND'])
        
    return document_id_list
    




########## XML DOwnload from the list of DOC  ######

#### Parameter List ######
##
##doc_id= Unique doc id
##
###########################

def get_decoded_xml(doc_id):
    #doc_id='520807-2021'
    url='https://ted.europa.eu/api/v2.0/notices/search?fields=CONTENT&q=ND%3D%5B{0}%5D&reverseOrder=true&scope=3&sortField=ND'.format(doc_id)
    r = requests.get(url)
    xml_data=r.json()
    xml_data= xml_data['results']
    xml_data=xml_data[0]
    xml_data=xml_data['content']
    xml_data= base64.b64decode(xml_data)
    
    return xml_data






######## Fetch Data and Return DataFrame ###################
## xml_data Byte format xml data to be loaded in Beautiful Soup
## doc_id doc_id number of xml file which we are going to scrap the data
############################

def get_xml_data(xml_data,doc_id,scope):
    if scope ==1:
        scope='Inactive'
    elif scope==2:
        scope='Active'
    else:
        scope='Any'
        
    final_data = pd.DataFrame()
    soup = BeautifulSoup(xml_data, 'xml')
    try:        
        reception_id= soup.find('RECEPTION_ID')
        reception_id_data =reception_id.get_text() 
        #doc_id= soup.find('URI_DOC')
       # doc_id_data=doc_id.get_text()
        #doc_id_data=re.search(r'NOTICE:(.*?):TEXT', doc_id_data).group(1)       
        cpv_code= soup.find('CPV_MAIN')
        cpv_code= str(cpv_code)
        cpv_code =''.join(filter(lambda i: i.isdigit(), cpv_code ))       
        final_data.at[0,'Doc ID']= doc_id
        final_data.at[0,'Reception ID']= reception_id_data
        final_data.at[0,'CPV CODE']= cpv_code
        final_data.at[0,'Notice Status']=scope
        #cpv_code = re.findall('[0-9]+', cpv_code)
    
    except Exception :
        pass    
        
    
    ### Checking For Existence of Doc Fields
    basic_data= soup.find('ML_TI_DOC')   
    try:
        if basic_data:
            country_iso=soup.find("ML_TI_DOC", { "LG" : "EN" }).find("TI_CY", recursive=True)
            country_iso= country_iso.get_text() 
            town=soup.find("ML_TI_DOC", { "LG" : "EN" }).find("TI_TOWN", recursive=True)
            town= town.get_text()  
            project_name=soup.find("ML_TI_DOC", { "LG" : "EN" }).find("TI_TEXT", recursive=False)
            project_name= project_name.get_text()           
            ### Appending Data ####
            final_data.at[0,'Country ISO']= country_iso
            final_data.at[0,'TOWN ISO']=  town
            final_data.at[0,'Project Name']= project_name
        else:
            pass
    
    except Exception:
        pass   
    
    #### Checking For Form Data Fields
    form_data= soup.find('F03_2014')   
    try:
        if form_data:    
            description= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("SHORT_DESCR", recursive=True)
            description= description.get_text()
            postal_code= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("POSTAL_CODE", recursive=True)
            postal_code= postal_code.get_text()   
            email= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("E_MAIL", recursive=True)
            email= email.get_text()
            amount= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("VAL_TOTAL", recursive=True)
            currency= re.findall(r'"(.*?)"', str(amount))
            amount= amount.get_text()   
            og_notice_no= soup.find("F03_2014", { "CATEGORY":"ORIGINAL", "FORM":"F03","LG" : "EN" }).find("NOTICE_NUMBER_OJ", recursive=True)
            og_notice_no= og_notice_no.get_text()   
            ### Appending Data to Data Frmae
            final_data.at[0,'Description']= description
            final_data.at[0,'Postal Code']= postal_code
            final_data.at[0,'Email']= email
            final_data.at[0,'Amount']= amount
            final_data.at[0,'Currency']= currency
            final_data.at[0,'Og Notice No']= currency  
        else:
            pass        
    except Exception:
        pass    
        ###### WEB Scrapping For Additional Information #########
        ##### Scrapping Of Tab ######
    try:  
        url='https://ted.europa.eu/udl?uri=TED:NOTICE:{0}:DATA:EN:HTML&src=0&tabId=3'.format(doc_id)        
        response = requests.get(url)    
        html_soup = BeautifulSoup(response.content, 'html.parser')    
        table_data = pd.read_html(response.text) # this parses all the tables in webpages to a list
        table_df = table_data[0]
        # =============================================================================
         #table_df_copy=table_df.drop(table_df.columns[[0]],axis=1)
        # =============================================================================
        table_df.columns =['Symbol','Header','Values']   
        if table_df.empty:
            print("Empty Data Frame")
        else:
                final_data.at[0,'Title']= table_df.loc[table_df['Symbol'] == 'TI','Values'].values
                final_data.at[0,'Notice publication number']= table_df.loc[table_df['Symbol'] == 'ND','Values'].values
                final_data.at[0,'Publication date']= table_df.loc[table_df['Symbol'] == 'PD','Values'].values
                final_data.at[0,'OJ S issue number']= table_df.loc[table_df['Symbol'] == 'OJ','Values'].values
                final_data.at[0,'Town/city of the buyer']= table_df.loc[table_df['Symbol'] == 'TW','Values'].values
                final_data.at[0,'Official name of the buyer']= table_df.loc[table_df['Symbol'] == 'AU','Values'].values
                final_data.at[0,'Original language']= table_df.loc[table_df['Symbol'] == 'OL','Values'].values
                final_data.at[0,'Country of the buyer']= table_df.loc[table_df['Symbol'] == 'CY','Values'].values
                final_data.at[0,'Type of buyer']= table_df.loc[table_df['Symbol'] == 'AA','Values'].values
                final_data.at[0,'EU institution/agency']= table_df.loc[table_df['Symbol'] == 'HA','Values'].values
                final_data.at[0,'Document sent']= table_df.loc[table_df['Symbol'] == 'DS','Values'].values
                final_data.at[0,'Type of contract']= table_df.loc[table_df['Symbol'] == 'NC','Values'].values
                final_data.at[0,'Type of procedure']= table_df.loc[table_df['Symbol'] == 'PR','Values'].values
                final_data.at[0,'Notice type']= table_df.loc[table_df['Symbol'] == 'TD','Values'].values
                final_data.at[0,'Regulation']= table_df.loc[table_df['Symbol'] == 'RP','Values'].values
                final_data.at[0,'Type of bid']= table_df.loc[table_df['Symbol'] == 'TY','Values'].values
                final_data.at[0,'Award criteria']= table_df.loc[table_df['Symbol'] == 'AC','Values'].values
                final_data.at[0,'Common procurement vocabulary (CPV)']= table_df.loc[table_df['Symbol'] == 'PC','Values'].values
                final_data.at[0,'Place of performance (NUTS)']= table_df.loc[table_df['Symbol'] == 'RC','Values'].values
                final_data.at[0,'Internet address (URL)']= table_df.loc[table_df['Symbol'] == 'IA','Values'].values
                final_data.at[0,'Legal basis']= table_df.loc[table_df['Symbol'] == 'DI','Values'].values
                final_data.at[0,'Deadline Date']= table_df.loc[table_df['Symbol'] == 'DT','Values'].values               
    except Exception:
        pass     
        
        ##### Scraping OF Other Tab  ################             
    try:       
        url='https://ted.europa.eu/udl?uri=TED:NOTICE:{0}:DATA:EN:HTML&src=0&tabId=4'.format(doc_id)       
        response = requests.get(url)       
        html_soup = BeautifulSoup(response.content, 'html.parser')      
        # =============================================================================
        # table_data = pd.read_html(response.text) # this parses all the tables in webpages to a list
        # table_df_1 = table_data[0]
        # table_df_2 =table_data[1]
        # table_df_3 = table_data[2]
        # =============================================================================
        anchor_data = html_soup.find_all("a", {"class": "noBg"})     
        past_doc_list=[]
        for values in anchor_data:
            text= values.get_text()
            doc_ids=text[0:text.find(':')]
            past_doc_list.append(doc_ids)
    except Exception:
        pass   
    ### looping and finding past data
    counter=1
    Pub= 'Publication_Date'
    Dead= 'Deadline'
    Doc= 'Document'
    Auth= 'Authority_Name'   
    try:
        for value in past_doc_list :
            url='https://ted.europa.eu/udl?uri=TED:NOTICE:{0}:DATA:EN:HTML&src=0&tabId=3'.format(value)
            response = requests.get(url)
            #html_soup = BeautifulSoup(response.content, 'html.parser')
            past_table = pd.read_html(response.text) # this parses all the tables in webpages to a list
            past_table = past_table[0] 
            past_table.columns =['Symbol','Header','Values']
            key= Doc + '_id_' +str(counter)
            final_data.at[0,key]=value
            key= Pub + '_' + str(counter)
            final_data.at[0,key]=past_table.loc[past_table['Symbol'] == 'PD','Values'].values
            key= Dead + '_' + str(counter)
            final_data.at[0,key]= past_table.loc[past_table['Symbol'] == 'DT','Values'].values
            key= Doc + '_' + str(counter)
            final_data.at[0,key]= past_table.loc[past_table['Symbol'] == 'TD','Values'].values
            key= Auth + '_' + str(counter)   
            final_data.at[0,key]= past_table.loc[past_table['Symbol'] == 'AU','Values'].values
            counter = counter+1
            
    except  Exception:
        pass
    
    
    try:
        latitude,longitude=geolocate(country_iso,town)
        final_data.at[0,'latitude']=   latitude
        final_data.at[0,'longitude']=   longitude

        
    except Exception:
        pass
    return final_data








def save_csv(csv_data,scope,country_name):
    try:
        timestamp = time.time()
        filename= country_name + '_' + str(scope) + '_' + str(timestamp) + '.csv'
        csv_data.to_csv(filename, index=False, header=True)
        return 'Sucessfully Save'
    except Exception:
        return 'Failed To Save'

     

def merging_whole_df():
    filenames = [i for i in glob.glob("*.csv")]
    header_keys = []
    merged_rows = []

    for filename in filenames:
        with open(filename) as f:
            reader = csv.DictReader(f)
            merged_rows.extend(list(reader))
            header_keys.extend([key for key in reader.fieldnames if key not in header_keys])

    with open("combined.csv", "w") as f:
        w = csv.DictWriter(f, fieldnames=header_keys)
        w.writeheader()
        w.writerows(merged_rows)
        


# =============================================================================


# =============================================================================


### Main Code For Using FUnctions #####   
country_name='SK'
scope=2
final_list= []
document_id_list= get_doc_idlist(country_name, scope)
for doc_id in document_id_list:
    xml_data= get_decoded_xml(doc_id)
    data= get_xml_data(xml_data,doc_id,scope)
    final_list.append(data)
    timestamp = time.time()
    filename= country_name + '_' + str(scope) + '_' + str(timestamp) + '.csv'
    data.to_csv(filename, index=False, header=True)
    
try:
     final_dataframe=pd.concat(final_list, axis=0, ignore_index=True)
     column_list= list( final_dataframe.columns)
     for column_name in column_list:
         final_dataframe[column_name]= final_dataframe[column_name].apply(data_clean)
     
     message= save_csv(final_dataframe,scope,country_name)
     print(message)
except Exception:
    pass
    





#final_dataframe.to_csv('Slovakia_merged.csv', index=False, header=True)




