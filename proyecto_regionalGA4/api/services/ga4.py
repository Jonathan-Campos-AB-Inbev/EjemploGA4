import pandas as pd
import datetime
import os
from api.database.general_database import Db_datos, Db_datos_pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from api.utils.mange_enviroment_varible import get_variable
from google.oauth2 import service_account


def list_sites_process_day(auth, type_extraction, date_init, date_fin):
    sql = str('select * from vw_site_tracing_analyticsGA4;') # Es un listado de los sitios
    sites = Db_datos(query=sql)
    #sites = [{'gtm': 'GTM-RENT', 'ga4': '250169191'}] #['250169191']
    if type_extraction == 'Yesterday':
        dateIni = (datetime.date.today() + datetime.timedelta(days=-1)).isoformat()
        dateFin = (datetime.date.today() + datetime.timedelta(days=-1)).isoformat()
    elif type_extraction == 'Last7Days':
        dateIni = (datetime.date.today() + datetime.timedelta(days=-7)).isoformat()
        dateFin = (datetime.date.today() + datetime.timedelta(days=-1)).isoformat()
    elif type_extraction == 'Custom':
        dateIni = date_init
        dateFin = date_fin
    else:
        raise Exception("type_extraction must be 'Yesterday' or 'Last7Days'")

    for i in sites:
        response = extractDataGA4(i['ga4'], dateIni, dateFin)  #i['ga4'] - 250169191  219674065
        labels_ga4 = pd.DataFrame()
        if response != 'Error':
            headers = list(map(lambda x: x.name, response.metric_headers))
            headers.append('date')
            values = []
            for j in response.rows:
                rows = []
                for value in j.metric_values:
                    rows.append(value.value)
                date = j.dimension_values[0].value
                rows.append(date)
                results = zip(headers, rows)
                results = dict(results)
                labels_ga4 = labels_ga4.append(results, ignore_index=True)
        labels_ga4['dateExtraccion'] = datetime.datetime.now().strftime('%Y-%m-%d')
        labels_ga4 = process_data_structure(labels_ga4, i, type="Records generation")
        Db_datos_pd(labels_ga4, table='metricas_analytics_ga4')
    return "OK"

def extractDataGA4(property_id, dateIni, dateFin):
    try:
        """Runs a simple report on a Google Analytics 4 property."""
        # TODO(developer): Uncomment this variable and replace with your
        #  Google Analytics 4 property ID before running the sample.
        # property_id = "YOUR-GA4-PROPERTY-ID"
        # Using a default constructor instructs the client to use the credentials
        # specified in GOOGLE_APPLICATION_CREDENTIALS environment variable.
        #credential = service_account.Credentials.from_service_account_file('')
        #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "api/database/sa_universal.json"
        #client = BetaAnalyticsDataClient(credentials = credential)
        #client = vision.ImageAnnotatorClient(credentials=credential)

        client = BetaAnalyticsDataClient().from_service_account_json(get_variable("GOOGLE_APPLICATION_CREDENTIALS"))
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="engagedSessions"),
                     Metric(name="engagementRate"),
                     Metric(name="newUsers"),
                     Metric(name="screenPageViews"),
                     Metric(name="sessions"),
                     Metric(name="sessionsPerUser"),
                     Metric(name="totalUsers"),
                     Metric(name="userEngagementDuration")
                     ],
            date_ranges=[DateRange(start_date=dateIni, end_date=dateFin)],
        )
        response = client.run_report(request)
        return response
    except Exception as e:
        raise str(e)

def process_data_structure(dataframe, view_id, type=''):
    dataframe['ga4'] = view_id['ga4']
    dataframe['type'] = type
    column_names = ["engagedSessions", "engagementRate","newUsers","screenPageViews", "sessions", "sessionsPerUser", "totalUsers", "userEngagementDuration",
                        "date", "dateExtraccion", "ga4", "type"]

    dataframe = dataframe.reindex(columns=column_names)
    return dataframe