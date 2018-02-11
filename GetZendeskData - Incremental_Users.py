# !Python3
# ---------------------------------------------------------------------------------------------------
# Script: GetZendeskData_Incremental_Users
# ----------------------------------------------------------------------------------------------------
# Author: David Edwards
# ----------------------------------------------------------------------------------------------------
# Status: In development
# ----------------------------------------------------------------------------------------------------
# Date: Written on the sixth day of the seventh month in the two thousandth and
#       seventeenth year of our Lord and Saviour Jesus Christ(or just 06/07/2017).
# -----------------------------------------------------------------------------------------------------
# Purpose:
#         Written as an alternative means to accessing Zendesk API data for ATG
#         Media's Customer Services reporting. This script for populating the CSR_Incremental_Users table.
#         in the database
# -----------------------------------------------------------------------------------------------------

# Stage 1 - Preparation
# Importing necessary modules
# ----------------------------

import time                                     # Because I want to know how long this takes to run.
from datetime import timedelta                  # Need these too for timing stats.
import pyodbc                                   # To establish a connection with SQL Server.
from urllib.parse import urlencode              # To parse the API end-point URL connection string.
import requests                                 # To send GET requests to Zendesk's API.
from requests.exceptions import ConnectionError # To trap connection errors


# Stage 2 - Creating/Declaring necessary variables and stuff
# ----------------------------------------------------------
user = 'davidedwards@auctiontechnologygroup.com' + '/token' # My Zendesk username for testing purposes. This will be a user input for other users.
pwd = 'kvgwnKupTpXmobFikt085fJlxZiZk2xidMLuPG6q'            # My Zendesk password which is also hardcoded here.      
urlUser = 'https://atg.zendesk.com/api/v2/incremental/users.json?start_time=1275350400'       # The URL for the API (Search) end-point we're interested in.
cntlist = list(range(2000000))                              # A list with which I iterate through the API results.

# Stage 3 - Getting things moving
# -------------------------------
# Connecting to the database
# --------------------------

# conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-5JCH84R\SQLEDWARDS;DATABASE=CSR_Reporting;Trusted_Connection=yes') # My connection string at home

# ATG connection string for dummy database
conn = pyodbc.connect('Driver={ODBC Driver 13 for SQL Server};Server=tcp:scratchinsights.database.windows.net,1433;Database=david;Uid=scratchadmin;Pwd=An4lyt1cs;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

writerUser = conn.cursor() # Opening a cursor object with which we'll iterate through records in the database.

# Stored procedures to copy across records and delete duplicates
sql_Update_Users = '''EXEC Update_CSR_Users'''                  # Update existing users with new details in CSR_Users
sql_New_User_IDs = '''EXEC Update_CSR_Users_New_IDs'''          # Add new users to CSR_Users from CSR_Incremental_Users
sql_Clear_IncUsers = '''EXEC Clear_Incremental_Users'''         # Delete records from CSR_Incremental_Users table in preparation for new records from the API
sql_Remove_Duplicates = '''EXEC Remove_Duplicates_CSR_Users'''  # Remove duplicates from CSR_Tickets (always less than 1% of all records)

# Let's execute them now
writerUser.execute(sql_Update_Users)
print('Prep phase 1 complete')
writerUser.execute(sql_New_User_IDs)
print('Prep phase 2 complete')
writerUser.execute(sql_Clear_IncUsers)
print('CSR_Incremental_Users table cleared')

# Stage 4 - Establishing a connection to Zendesk
# ----------------------------------------------

responseU = requests.get(urlUser,auth=(user,pwd)) # Making our humble request to Zendesk. Hi Zendesk API? Could we have some data please?
if responseU.status_code != 200:
    print('Status:', responseU.status_code, 'Problem with the request. Exiting.') # Error handling in case Zendesk says no or the network goes renegade in which case, who cares anyway?
    exit()

print('Success: Connection made to API')     # Once again, I thank you Zendesk
UserData = responseU.json()                  # Decoding the json data into a Python dictionary.

# Stage 5 - Function to get our results from the Python dictionary.
# -----------------------------------------------------------------

def GetZendesk_Users(val):
    try:
        userContainer = []
        a = UserData['users'][val]['id']
        b = UserData['users'][val]['email']
        c = UserData['users'][val]['name']
        d = UserData['users'][val]['active']
        e = UserData['users'][val]['alias']
        f = UserData['users'][val]['chat_only']
        g = UserData['users'][val]['created_at']
        h = UserData['users'][val]['custom_role_id']
        i = UserData['users'][val]['details']
        j = UserData['users'][val]['external_id']
        k = UserData['users'][val]['last_login_at']
        l = UserData['users'][val]['locale']
        m = UserData['users'][val]['locale_id']
        n = UserData['users'][val]['moderator']
        o = UserData['users'][val]['notes']
        p = UserData['users'][val]['only_private_comments']
        q = UserData['users'][val]['organization_id']
        r = UserData['users'][val]['default_group_id']
        s = UserData['users'][val]['phone']
        t = UserData['users'][val]['restricted_agent']
        u = UserData['users'][val]['role']
        v = UserData['users'][val]['shared']
        w = UserData['users'][val]['shared_agent']
        x = UserData['users'][val]['signature']
        y = UserData['users'][val]['suspended']
        z = UserData['users'][val]['ticket_restriction']
        aa = UserData['users'][val]['time_zone']
        ab = UserData['users'][val]['two_factor_auth_enabled']
        ac = UserData['users'][val]['updated_at']
        ad = UserData['users'][val]['url']
        ae = UserData['users'][val]['verified']
        userContainer.extend([a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,
                              x,y,z,aa,ab,ac,ad,ae])
        sqlUser = '''INSERT INTO CSR_Incremental_Users(ID,Email,Name,Active,Alias,Chat_Only,
Created_At,Custom_Role_ID,Details,External_ID,Last_Login_At,Locale,Locale_ID,Moderator,Notes,
Only_Private_Comments,Organization_ID,Default_Group_ID,Phone,Restricted_Agent,Role,Shared,Shared_Agent,Signature,
Suspended,Ticket_Restriction,Time_Zone,Two_Factor_Auth_Enabled,Updated_At,Url,Verified)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
        writerUser.execute(sqlUser, userContainer)
        writerUser.commit()
        del userContainer[:]
    except (IndexError, KeyError, ValueError):
        return


start_time = time.monotonic()

page = 1                                                     # Page marker                                                                      
while urlUser:                                               # Beginning of loop
    try:
        responseU = requests.get(urlUser, auth=(user,pwd))   # Making our request to the server again just in case of a previous network failure/break or something  
        UserData = responseU.json()
        for val in cntlist:                                  # Iterating through our cntlist
            GetZendesk_Users(val)                            # Invoking our function which does the heavy lifting
        urlUser = UserData['next_page']                      # Zendesk API pagination
        print('Page %s data inserted into database' % page)  # Returning pages to IDLE so we know where we are
        page += 1                                            # Incrementing our page count by 1 after each successful commit
    except (KeyError, IndexError):
        writerUser.execute(sql_Update_Users)
        print('End of pages reached.')
        print('Refreshing CSR_Users...')
        writerUser.execute(sql_New_User_IDs)
        writerUser.execute(sql_Remove_Duplicates)
        writerUser.commit()
        end_time = time.monotonic()
        print('CSR_Users refreshed')
        print('Done')
        print(timedelta(seconds=end_time - start_time))
        writerUser.close()                          
        conn.close()
        responseU.close()
        raise SystemExit
    except ConnectionError:
        print('Connection Error: Retrying...')

writerUser.execute(sql_Update_Users)
print('Refreshing CSR_Users...')
writerUser.execute(sql_New_User_IDs)
print('CSR_Users refreshed')
writerUser.execute(sql_Remove_Duplicates)
writerUser.commit()
print('Changes committed')
end_time = time.monotonic()
print(timedelta(seconds=end_time - start_time))
writerUser.close()                          
conn.close()
print("Done")
