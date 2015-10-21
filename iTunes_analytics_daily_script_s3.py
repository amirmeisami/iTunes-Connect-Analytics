import json
import urllib
import urllib2
import cookielib
import re
import dateutil.parser
import datetime
import pandas as pd
import tinys3
from datetime import date, timedelta
import pytz
from random import randint
from time import sleep


class ITCException(Exception):
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return repr(self.value);


class MyCookieJar(cookielib.CookieJar):
    def _cookie_from_cookie_tuple(self, tup, request):
        name, value, standard, rest = tup
        version = standard.get('version', None)
        if version is not None:
            version = version.replace('"', '')
            standard["version"] = version
        return cookielib.CookieJar._cookie_from_cookie_tuple(self, tup, request)

class ITCAnalytics:
    urlITCBase = 'https://itunesconnect.apple.com%s'
    urlAnalyticsBase = 'https://analytics.itunes.apple.com/analytics/api/v1/%s'

    def __init__(self, itcLogin, itcPassword, appId, toDay, proxy=''):
        self.itcLogin = itcLogin
        self.itcPassword = itcPassword
        self.appId = str(appId)
        self.proxy = proxy
        self.toDay = toDay
        self.opener = self.createOpener()
        self.login()

    def readHtml(self, url, data=None, content_type=None):
        request = urllib2.Request(url, data, {'Content-Type': content_type}) if content_type else urllib2.Request(url, data)
        urlHandle = self.opener.open(request)
        html = urlHandle.read()
        return html

    def createOpener(self):
        handlers = []                                                       # proxy support
        if self.proxy:                                                      # proxy support
            handlers.append(urllib2.ProxyHandler({"https": self.proxy}))    # proxy support

        cj = MyCookieJar();
        cj.set_policy(cookielib.DefaultCookiePolicy(rfc2965=True))
        cjhdr = urllib2.HTTPCookieProcessor(cj)
        handlers.append(cjhdr)                                              # proxy support
        return urllib2.build_opener(*handlers)                              # proxy support

    def login(self):
        # Go to the iTunes Connect website and retrieve the
        # form action for logging into the site.
        urlWebsite = self.urlITCBase % '/WebObjects/iTunesConnect.woa'
        html = self.readHtml(urlWebsite)
        match = re.search('" action="(.*)"', html)
        urlActionLogin = self.urlITCBase % match.group(1)

        # Login to iTunes Connect web site
        webFormLoginData = urllib.urlencode({'theAccountName':self.itcLogin, 'theAccountPW':self.itcPassword, '1.Continue':'0'})
        html = self.readHtml(urlActionLogin, webFormLoginData)
        if (html.find('Your Apple ID or password was entered incorrectly.') != -1):
            raise ITCException, 'User or password incorrect.'
    
    def api_call(self, url, data=None):
        
        response = self.readHtml(url, data=json.dumps(data), content_type='application/json')
        return json.loads(response)
    
    def region(self):
        regDict = {}
        data = {
        'adamId': [self.appId],
        'frequency': 'DAY',
        'dimensionFilters': [],
        'measures': ["pageViewCount"],
        'startTime': self.toDay,
        'endTime': self.toDay,
        'group': {'metric': "pageViewCount",'dimension': "region",
                  'rank': "DESCENDING", 'limit':3}}
        
        response = self.api_call(self.urlAnalyticsBase % 'data/time-series', data=data)
        
        for metric in response['results']:
            regDict[metric['group']['key']] = metric['group']['title']
        
        regTerDict = {}
        for key in regDict:
            data = {
            'adamId': [self.appId],
            'frequency': 'DAY',
            'dimensionFilters': [{'dimensionKey': "region", 'optionKeys': [str(key)]}],
            'measures': ["pageViewCount"],
            'startTime': self.toDay,
            'endTime': self.toDay,
            'group': {'metric': "pageViewCount",'dimension': "storefront",
                      'rank': "DESCENDING", 'limit':3}}
            
            sleep(randint(2,4))
            response = self.api_call(self.urlAnalyticsBase % 'data/time-series', data=data)
            countries = []
            for metric in response['results']:
                countries.append(metric['group']['title'])
            
            regTerDict[regDict[key]]=countries 
        
        return regTerDict
        
    def metrics(self):
        metrics=["pageViewCount","installs", "sessions","activeDevices"]
        options=["iPad","iPhone","iPod"]
        op_responses = []
        responses = []
        for metric in metrics:
            data = {
            'adamId': [self.appId],
            'frequency': 'DAY',
            'dimensionFilters': [],
            'measures': [metric],
            'startTime': self.toDay,
            'endTime': self.toDay,
            'group': {'metric': metric,'dimension': "storefront",
                      'rank': "DESCENDING", 'limit':3
                              
                    }
                
            }
        
            response = self.api_call(self.urlAnalyticsBase % 'data/time-series', data=data)
            responses.append(response['results'])
            for option in options:
                data = {
                'adamId': [self.appId],
                'frequency': 'DAY',
                'dimensionFilters': [{'dimensionKey': "platform", 'optionKeys': [option]}],
                'measures': [metric],
                'startTime': self.toDay,
                'endTime': self.toDay,
                'group': {'metric': metric,'dimension': "storefront",
                          'rank': "DESCENDING", 'limit':3
                                  
                        }
                    
                }
                sleep(randint(2,4))
                op_response = self.api_call(self.urlAnalyticsBase % 'data/time-series', data=data)
                op_responses.append(op_response['results'])
        return (responses, op_responses)
        
    def metrics_data_frame(self):
        responses, op_responses = self.metrics()
        gameNames = self.app_id()
        regTer = self.region()
        DataSet = pd.DataFrame()
        DataSet['Date'] = [dateutil.parser.parse(metric['data'][0]['date']).strftime('%Y-%m-%d') for metric in responses[0]]
        DataSet['Country'] = [metric['group']['title'].encode('utf-8') for metric in responses[0]]
        DataSet['Views'] = [metric['data'][0]['pageViewCount'] for metric in responses[0]]
        DataSet['iPadViews'] = [metric['data'][0]['pageViewCount'] for metric in op_responses[0]]
        DataSet['iPhoneViews'] = [metric['data'][0]['pageViewCount'] for metric in op_responses[1]]
        DataSet['iPodViews'] = [metric['data'][0]['pageViewCount'] for metric in op_responses[2]]
        DataSet['installs'] = [metric['data'][0]['installs'] for metric in responses[1]]
        DataSet['iPadInstalls'] = [metric['data'][0]['installs'] for metric in op_responses[3]]
        DataSet['iPhoneInstalls'] = [metric['data'][0]['installs'] for metric in op_responses[4]]
        DataSet['iPodInstalls'] = [metric['data'][0]['installs'] for metric in op_responses[5]]
        DataSet['sessions'] = [metric['data'][0]['sessions'] for metric in responses[2]]
        DataSet['iPadSessions'] = [metric['data'][0]['sessions'] for metric in op_responses[6]]
        DataSet['iPhoneSessions'] = [metric['data'][0]['sessions'] for metric in op_responses[7]]
        DataSet['iPodSessions'] = [metric['data'][0]['sessions'] for metric in op_responses[8]]
        DataSet['activeDevices'] = [metric['data'][0]['activeDevices'] for metric in responses[3]]
        DataSet['iPadActiveDevices'] = [metric['data'][0]['activeDevices'] for metric in op_responses[9]]
        DataSet['iPhoneActiveDevices'] = [metric['data'][0]['activeDevices'] for metric in op_responses[10]]
        DataSet['iPodActiveDevices'] = [metric['data'][0]['activeDevices'] for metric in op_responses[11]]
        DataSet = DataSet.replace(-1,0)
        platforms = ["iPad","iPhone","iPod"]
        grouped_dataset = pd.DataFrame()
        game = []
        Date = []
        country = []
        Platforms = []
        views = []
        installs = []
        sessions = []
        actives = []
        region = []
        
        for i in xrange(0,len(DataSet)):
            for key in regTer:
                    if unicode(DataSet['Country'][i], "utf-8") in regTer[key]:
                        reg=key
            for platform in platforms:
                game.append(gameNames[responses[0][0]['adamId']])
                Date.append(DataSet['Date'][i])
                country.append(DataSet['Country'][i])
                region.append(reg)
                Platforms.append(platform)
                views.append(int(DataSet[platform +'Views'][i]))
                installs.append(int(DataSet[platform +'Installs'][i]))
                sessions.append(int(DataSet[platform +'Sessions'][i]))
                actives.append(int(DataSet[platform +'ActiveDevices'][i]))
                
                
        grouped_dataset['Game'] = game        
        grouped_dataset['Date'] = Date
        grouped_dataset['Country'] = country
        grouped_dataset['Region'] = region
        grouped_dataset['Platform'] = Platforms
        grouped_dataset['Views'] = views
        grouped_dataset['Installs'] = installs
        grouped_dataset['Sessions'] = sessions
        grouped_dataset['ActiveDevices'] = actives

        return grouped_dataset
        
    def source(self):
 
        data = {
        'adamId': [self.appId],
        'frequency': 'DAY',
        'measures': ["pageViewCount", "units", "sales", "sessions"],
        'startTime': self.toDay,
        'endTime': self.toDay
            
        }
        
        response = self.api_call(self.urlAnalyticsBase % 'data/sources/domainreferrer-list', data=data)
        return response['results']
        
    def source_data_frame(self):
        metrics = self.source()
        gameNames = self.app_id()
        DataSet = pd.DataFrame()
        DataSet['Game'] = [gameNames[metric['adamId']] for metric in metrics]
        DataSet['Date'] = [dateutil.parser.parse(metric['endTime']).strftime('%Y-%m-%d') for metric in metrics]
        DataSet['Source'] = [metric['domainReferrer'] for metric in metrics]
        DataSet['pageViewCount'] = [int(metric['data']['pageViewCount']['value']) for metric in metrics]
        DataSet.pageViewCount = DataSet.pageViewCount.astype(int)
        DataSet['sessions'] = [int(metric['data']['sessions']['value']) for metric in metrics]
        DataSet.sessions = DataSet.sessions.astype(int)
        DataSet['units'] = [int(metric['data']['units']['value']) for metric in metrics]
        DataSet.units = DataSet.units.astype(int)
        DataSet['sales'] = [int(metric['data']['sales']['value']) for metric in metrics]
        DataSet.sales = DataSet.sales.astype(int)
        

        return DataSet

    def app_id(self):
        response = self.readHtml(self.urlAnalyticsBase %'app-info/app')
        response = json.loads(response)
        Dict = {}
        for metric in response['results']:
            Dict[metric['adamId']] = metric['name']
        return Dict

eastern_tz = pytz.timezone('US/Pacific-New')
today = datetime.datetime.now(eastern_tz).date()
yesterday = today - timedelta(1)
str_yes = str(datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0).isoformat())+"Z"
analytics = ITCAnalytics('replace-me', 'replace-me', '*****', str_yes)
appNameDict = analytics.app_id()
start = yesterday
finish = today
for single_date in [start + timedelta(n) for n in xrange(0,(finish-start).days)]:
    str_yes = str(datetime.datetime(single_date.year, single_date.month, single_date.day, 0, 0, 0).isoformat())+"Z"
    metric_datasets = {}
    sources_datasets = {}
    for key in appNameDict:
        analytics = ITCAnalytics('replace-me', 'replace-me', key, str_yes)
        metric_datasets[appNameDict[key]] = analytics.metrics_data_frame()
        sleep(randint(3,8))
        sources_datasets[appNameDict[key]] = analytics.source_data_frame()    

    daily_metric_dataframe = pd.concat([metric_datasets[appNameDict[key]] for key in appNameDict])
    daily_sources_dataframe = pd.concat([sources_datasets[appNameDict[key]] for key in appNameDict])

    if len(daily_metric_dataframe > 0):
        ppath ='itunes_analytics_metrics_%s.csv' % str(single_date)
        daily_metric_dataframe.to_csv(ppath,encoding='utf-8', index=False, sep='\t')
        conn = tinys3.Connection('replace-me','replace-me',endpoint='s3-us-west-2.amazonaws.com')
        f = open(ppath,'rb')
        conn.upload('itunes_analytics_metrics_%s.csv' % str(single_date),f,'replace-me')
        
    if len(daily_sources_dataframe > 0):
        path ='itunes_analytics_sources_%s.csv' % str(single_date)
        daily_sources_dataframe.to_csv(path,encoding='utf-8',index=False, sep='\t', float_format='%.f')
        conn = tinys3.Connection('replace-me','replace-me',endpoint='s3-us-west-2.amazonaws.com')
        f = open(path,'rb')
        conn.upload('itunes_analytics_sources_%s.csv' % str(single_date),f,'~/iTunes_analytics')

