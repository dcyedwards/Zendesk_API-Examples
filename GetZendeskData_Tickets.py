# !Python3
# -------------------------------------------------------------------------------
# Script: Zendesk_API_Data
# -------------------------------------------------------------------------------
# Author: David Edwards
# ------------------------------------------------------------------------------
# Status: In development
# -------------------------------------------------------------------------------
# Date: Written on the fifth day of the sixth month in the two thousandth and
#       seventeenth year of our Lord and Saviour Jesus Christ(or just 05/06/2017)
# Modified: 07/07/2017 - To include custom fields
# ------------------------------------------------------------------------------
# Purpose:
#         Written as an alternative means to accessing Zendesk API data for ATG
#         Media's Customer Services reporting.
# ------------------------------------------------------------------------------


# Stage 1 - Preparation
# Importing necessary modules
# ----------------------------

import time                                     # Because I want to know how my programs take to run.
from datetime import timedelta                  # Need these too.
import pyodbc                                   # To establish a connection with SQL Server.
from urllib.parse import urlencode              # To parse the API end-point URL connection string.
import requests                                 # To send GET requests to Zendesk's API.
from requests.exceptions import ConnectionError

global endSplits
endSplits = []

# Stage 2 - Creating/Declaring necessary variables and stuff
# ----------------------------------------------------------
user = 'davidedwards@auctiontechnologygroup.com' + '/token' # My Zendesk username for testing purposes. This will be a user input soon.
pwd = 'kvgwnKupTpXmobFikt085fJlxZiZk2xidMLuPG6q'            # My Zendesk password which is hardcoded here for testing as my username above.
params = { 'query': 'type:ticket created>2017-01-01'}       # API search parameters: Also possible to change with a user input (Still exploring other options). You can change the search from date here.      
url = 'https://atg.zendesk.com/api/v2/search.json?' + urlencode(params) # The URL for the API (Search) end-point we're interested in.
cntlist = list(range(20000000))                             # A list with which I iterate through the API results.


# Stage 3 - Getting things moving
# -------------------------------
# Connecting to the database
# --------------------------

#conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-5JCH84R\SQLEDWARDS;DATABASE=CSR_Reporting;Trusted_Connection=yes') # My connection string at home

# ATG connection string for dummy database
conn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};Server=tcp:scratchinsights.database.windows.net,1433;Database=david;Uid=scratchadmin;Pwd=An4lyt1cs;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

writer = conn.cursor() # Opening a cursor object with which we'll iterate through records in the database.


# Stage 4 - Establishing a connection to Zendesk
# ----------------------------------------------

response = requests.get(url,auth=(user,pwd)) # Making our humble request to Zendesk. Hi Zendesk API? Could we have some data please?
if response.status_code != 200:
    print('Status:', response.status_code, 'Problem with the request. Exiting.') # Error handling in case Zendesk says no or the network goes renegade in which case, who cares anyway?
    exit()

print('Success: Connection made to API')     # Thanks Zendesk
data = response.json()                       # Decoding the json data into a Python dictionary.


# Stage 5 - Some function to get only the results we want from the Python dictionary.
# -----------------------------------------------------------------------------------

def GetZendeskData_Tickets(val):                                                            # Function I've written to loop through the keys and values of interest in the dictionary.
    global endSplits
    try:                                                                                    # Error handling because you never know
        container = []                                                                      # List which contains (and is refreshed with) the data from the dictionary
        enqlist = []
        a = data['results'][val]['id']                                                      # Fields of interest from (a) to (v)
        b = data['results'][val]['url']
        c = data['results'][val]['external_id']
        d = data['results'][val]['via']['channel']
        e = data['results'][val]['created_at']
        f = data['results'][val]['updated_at']
        g = data['results'][val]['type']
        h = data['results'][val]['subject']
        i = data['results'][val]['description']
        j = data['results'][val]['priority']
        k = data['results'][val]['status']
        l = data['results'][val]['recipient']
        m = data['results'][val]['assignee_id']
        n = data['results'][val]['organization_id']
        o = data['results'][val]['group_id']
        p = data['results'][val]['has_incidents']
        q = data['results'][val]['due_at']
        r = data['results'][val]['custom_fields'][0]['value']
        s = data['results'][val]['custom_fields'][1]['value']
        t = data['results'][val]['brand_id']
        enqlist.append(s)
        try:
            if len(enqlist) > 0:
                enqSplits = [enq.split('__') for enq in enqlist]
                division = enqSplits[0][0]
                custType = enqSplits[0][1]
                enqCat = enqSplits[0][2]
                product = enqSplits[0][3]
                enqType = enqSplits[0][4]
            else:
                division=''
                custType =''
                enqCat=''
                product=''
                enqType=''
        except AttributeError:
            division=''
            custType=''
            enqCat=''
            product=''
            enqType=''
            pass
        container.extend([a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,division,
                          custType,enqCat,product,enqType,r,t])                     # Putting fields into my list
        sql = '''INSERT INTO CSR_Tickets(Id, Url,External_Id,                  
Channel,Created_At,Updated_At,Type,Subject,Description,Priority,Status,
Recipient,Assignee_Id,Organization_Id,Group_Id,Has_Incidents,Due_At,division,Customer_Type,
Enquiry_Category,Product,Enquiry_Type,Group_Name,Brand_Id)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''                               # SQL statement to execute against the database using placeholders (?)   
        writer.execute(sql, container)                                                      # Executing SQL
        writer.commit()                                                                     # Committing (Saving) changes
        del container[:]                                                                    # Clearing contents of list for next batch 
        del enqlist[:]
    except (IndexError, KeyError, ):                                                                      # Error handling for when we exceed the index range
        return                                                                              # Continue running the programme - all is well.


# Stage 6 - Invoking our function
# -------------------------------

start_time = time.monotonic()

page = 1                                                     # Page marker                                                                      
while url:                                                   # Beginning of loop
    try:
        response = requests.get(url, auth=(user,pwd))        # Making our request to the server again just in case of a previous network failure/break or something  
        data = response.json()
        for val in cntlist:                                  # Iterating through our cntlist
            GetZendeskData_Tickets(val)                      # Invoking our function which does the heavy lifting
        url = data['next_page']                              # Zendesk API pagination
        print('Page %s data inserted into database' % page)  # Returning pages to IDLE so we know where we are
        page += 1                                            # Incrementing our page count by 1 after each successful commit
    except ConnectionError as e:
        print(e)

        

end_time = time.monotonic()


# Stage 7 - Wrapping up
# ---------------------

writer.close()                          
conn.close()
response.close()

print("Done")
print(timedelta(seconds=end_time - start_time))    
    
