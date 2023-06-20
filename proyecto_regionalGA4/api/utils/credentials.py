import json
import datetime
import os
from datetime import date
from oauth2client.service_account import ServiceAccountCredentials
#from apiclient.discovery import build
from googleapiclient.discovery import build


scopes = ['https://www.googleapis.com/auth/analytics.readonly']
wd = os.getcwd()
key_file_location_universal = 'api/database/sa_universal.json'
key_file_location_ga4 = 'api/database/sa_ga4.json'
authentication = None
USER_ACCESS = "#"
PASSWORD = "#"



def auth_analytics(method):
    """Initializes an Analytics Reporting API V4 service object.
    Returns:
    An authorized Analytics Reporting API V4 service object.
    """
    try:
        if method == "universal":
            credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location_universal, scopes)
            analytics = build('analyticsreporting', 'v4', credentials=credentials)
            return analytics
        elif method == "ga4":
            credentials = ServiceAccountCredentials.from_json_keyfile_name(key_file_location_ga4, scopes)
            analytics = build('analyticsreporting', 'v4', credentials=credentials)
            return analytics
        else:
            raise "Error in authentication method"
    except Exception as e:
        raise {f"message: Authentication {authentication}"}


def validate_credentials(user, password):
    try:
        if user == USER_ACCESS and password == PASSWORD:
            return True
        else:
            return False
    except Exception as e:
        raise ('Error in User and Password')