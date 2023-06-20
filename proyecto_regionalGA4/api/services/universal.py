import json
import os
import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
from api.database.general_database import Db_datos, Db_datos_pd
from api.utils.mange_enviroment_varible import get_variable

load_dotenv()
secret_id = get_variable('secret_id')


def list_sites_process_day(auth, type_extraction, date_init, date_fin):
    sql = str('select * from vw_site_tracing_analytics;')
    sites = Db_datos(query=sql)
    issue = 0
    sites_ids = list(map(lambda x: x['view_id'], sites))
    for i in sites_ids:  # Este es el loop para la descarga de todos los sitios uno a uno
        if generate_report_day(i, auth, type_extraction, date_init, date_fin) == True:
            issue += 1
    if issue == 0:
        return "finish process, done correctly"
    else:
        texto = str("There are problems [{}]").format(issue)
        return {"response": texto}

def generate_report_day(view_id, auth, type_extraction, date_init, date_fin):
    code_type_tracing = "S1"
    try:
        """"Init y end pueden tomar una rango de fechas en formato AAAA-MM-DD o la palabra yesterday"""
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
        response = get_report(auth, init=dateIni, end=dateFin, VIEW_ID=view_id) #TODO: CAMBIAR AQUI EL VIEW_ID POR -> '219674065'
        labels_organic = pd.DataFrame()
        labels_total = pd.DataFrame()
        date = ''
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            if report.get('data').get('rowCount') is not None:
                for row in report.get('data', {}).get('rows', []):
                    dimensions = row.get('dimensions', [])
                    date = dimensions[0]
                    dateRangeValues = row.get('metrics', [])
                    medium_reg = {}
                    for header, dimension in zip(dimensionHeaders, dimensions):
                        medium_reg[header[3:]] = dimension
                        for i, values in enumerate(dateRangeValues):
                            for metricHeader, value in zip(metricHeaders, values.get('values')):
                                medium_reg[metricHeader.get('name')[3:]] = value
                    labels_organic = labels_organic.append(medium_reg, ignore_index=True)
                total_values = {}
                for header, value in zip(metricHeaders, report.get('data', {}).get('totals', [])[0].get('values', {})):
                    total_values[header.get('name')[3:]] = value
                total_values['date'] = date
                labels_total = labels_total.append(total_values, ignore_index=True)
            else:
                raise Exception('There are not data to proccess')
        labels_organic = process_data_structure(labels_organic, view_id, type="organic")
        labels_total = process_data_structure(labels_total, view_id, type="total")
        updateTracing(view_id, code_type_tracing)
        Db_datos_pd(labels_organic, table = 'metricas_analytics_organic')
        Db_datos_pd(labels_total, table = 'metricas_analytics')
        return False
    except Exception as e:
        print(f"view_id {view_id} -> " + str(e))
        return True

def get_report(analytics, **kwargs):
    """Queries the Analytics Reporting API V4.
    Args:
    analytics: An authorized Analytics Reporting API V4 service object.
    Returns:
    The Analytics Reporting API V4 response.
    """
    return analytics.reports().batchGet(
        body={
            'reportRequests': [{
                'viewId': kwargs["VIEW_ID"],
                'dateRanges': [{"startDate": kwargs["init"], "endDate": kwargs["end"]}],
                'metrics': [{"expression": "ga:users"},
                            {"expression": "ga:newusers"},
                            {"expression": "ga:sessions"},
                            {"expression": "ga:pageviews"},
                            {"expression": "ga:bounces"},
                            {"expression": "ga:timeOnPage"},
                            {"expression": "ga:avgSessionDuration"},
                            {"expression": "ga:sessionDuration"},
                            ],
                'dimensions': [{'name': 'ga:date'},
                               {'name': 'ga:medium'}
                               ]
            }]
        }
    ).execute()


def process_data_structure(dataframe, view_id='', type=''):#85945663
    dataframe['view_id'] = view_id
    dataframe['type'] = "Records generation"
    dataframe = dataframe.rename(columns={'timeOnPage': 'time_on_page',
                                          'date': 'ga_date'})
    if type == "organic":
        filter = dataframe[dataframe.medium == 'organic']
        dataframe = filter
        dataframe = dataframe.drop(['medium'], axis=1)
    column_names = ["users", "newusers","sessions","pageviews", "bounces", "sessionDuration", "time_on_page", "avgSessionDuration",
                        "ga_date", "view_id", "type"]

    dataframe = dataframe.reindex(columns=column_names)
    return dataframe

def updateTracing(view_id, code_type_tracing):
    try:
        today = datetime.date.today()
        sql = str("UPDATE tracings SET "
                  "fecha_ejecucion = now() "
                  "WHERE view_id = '{}' and cod_type_tracing = '{}';"
                  ).format(view_id, code_type_tracing)
        Db_datos(query=sql)
        return "OK"
    except:
        raise "Error while update tracing"
