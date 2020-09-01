import sys
sys.setrecursionlimit(1000000)
from gevent import monkey
monkey.patch_all()
import gevent
from gevent.queue import Queue
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest import (KustoIngestClient,IngestionProperties,FileDescriptor,DataFormat,ReportLevel)
import requests
import json
import csv
from urllib.parse import urlparse
from branch.path_app_branch import *

def authenticate_kusto(kusto_cluster):
    tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
    KCSB = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_cluster)
    KCSB.authority_id = tenant_id
    return KustoClient(KCSB),KCSB

def query_kusto(client, database, query):
    return client.execute(database, query)

def get_page_views_1(client):
    kusto_query_1 = """
    cluster('cgadataout').database('DevRelWorkArea').vwTopicMetadata()
    | where Site =='docs.microsoft.com'
    | where IsLive
    | where Locale == "en-us"
    | extend GitHubUrl = coalesce(OriginalContentGitUrl, GitUrl)
    | where GitHubUrl startswith "https://github.com"
    | extend OriginalContentRepo = tolower(extract("https://github.com/([^/]+/[^/]+)/",1,GitHubUrl))
    | extend OriginalContentFile = tolower(extract("https://github.com/[^/]+/[^/]+/blob/[^/]+/(.+)",1,GitHubUrl))
    | extend OriginalContentBranch = extract("https://github.com/[^/]+/[^/]+/blob/([^/]+)/",1,GitHubUrl)
    | distinct OriginalContentRepo, OriginalContentBranch
    """
    kusto_database_1 = 'DevRelWorkArea'
    result_1 = query_kusto(client, kusto_database_1, kusto_query_1)
    df_1 = dataframe_from_result_table(result_1.primary_results[0])
    return df_1

def Ingest(Tag):
    ingestion_props = IngestionProperties(
        database="DevRelWorkArea",
        table="BranchCommits",
        dataFormat=DataFormat.CSV,
        ingestByTags=[Tag],
        dropByTags=[Tag],
        mappingReference="BranchCommits_CSV_Mapping",
        reportLevel=ReportLevel.FailuresAndSuccesses,
        additionalProperties={'ignoreFirstRecord': 'true'}
    )

    file_descriptor = FileDescriptor(path_file_3,3333)  # 3333 is the raw size of the data in bytes.
    ls[1].ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)

    return 1

# Query Kusto
cga_cluster = 'https://cgadataout.kusto.windows.net'
ingest_cluster = "https://ingest-cgadataout.kusto.windows.net"
cga_client = authenticate_kusto(cga_cluster)[0]
ingest_client = KustoIngestClient(authenticate_kusto(ingest_cluster)[1])
ls=[cga_client,ingest_client]
current_1 = get_page_views_1(ls[0])

File1 = open(path_file_1, "w+", newline='',encoding='utf-8')
File2 = open(path_file_2, "w+", newline='',encoding='utf-8')
File3 = open(path_file_3, "w+", newline='',encoding='utf-8')
output_url_1 = csv.writer(File1)
output_url_1.writerow(['Link','Status'])
output_url_2 = csv.writer(File2)
output_url_2.writerow(['The URL','Link','Page','Information'])
output = csv.writer(File3)
output.writerow(['RepoOwner','RepoName','Branch','CommitSha','CommitAuthorLogin','CommitAuthorId','CommitDate','Data','Tag','Month'])

headers = {
    "Authorization": "Basic MjNhZjMzZDlkNTRhZDkzMTc5MmRkYzNhOWY4MjdiZTA1ZTM3Zjg0ZA==",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    "Cookie": "_octo=GH1.1.54427208.1583984786; _ga=GA1.2.1549066493.1583984788; logged_in=yes; dotcom_user=BaymaxBai01; tz=Asia%2FShanghai; _gid=GA1.2.220945336.1589789000"
}

urls = []

for i in range(len(current_1)):
    RepoFullName = current_1.iloc[i][0]
    OriginalContentBranch = current_1.iloc[i][1]
    url = "https://api.github.com/repos/{}/commits?sha={}&since={}T00:00:00Z&until={}T23:59:59Z&page=".format(RepoFullName,OriginalContentBranch,StartTime,EndTime)
    urls.append(url)

File4 = open(path_file_4, "w+", newline='',encoding='utf-8')
output_url = csv.writer(File4)
output_url.writerow(['Total {} URL'.format(len(urls))])
for url in urls:
    output_url.writerow([url+"1"])
File4.close()

work = Queue()
for data_url in urls:
    work.put_nowait(data_url)

start = time.time()

def get_each_url_all_page(data_url):
    j = 1
    result = requests.get(data_url + str(1), headers=headers)
    html = result.text
    status = result.status_code
    output_url_1.writerow([data_url+"1",status])
    if status == 404:
        print("The URL: {}1 status:{} Not Found in {}!".format(data_url,status,StartTime))
        output_url_2.writerow(["The URL:",data_url+"1","", "Response 404 Not Found in "+StartTime])
    elif status == 403:
        print("The URL: {}1 status:{} Not Found in {}!".format(data_url,status,StartTime))
        output_url_2.writerow(["The URL:",data_url+"1","", "Response 403 Forbidden in "+StartTime])
    elif status == 200 and html == '[\n\n]\n':
        print("The URL: {}1 status:{} Response OK but has no commit data in {}!".format(data_url,status,StartTime))
        output_url_2.writerow(["The URL:",data_url+"1","", "Response 200 OK but has no commit data in "+StartTime])
    else:
        print("The URL: {}1 status:{} Response OK!".format(data_url,status))
        output_url_2.writerow(["The URL:",data_url+"1","", "Response OK"])
        while True:
            jsondata = json.loads(html)
            for row in jsondata:
                RepoOwner = urlparse(data_url).path.split('/')[2]
                RepoName = urlparse(data_url).path.split('/')[3]
                a = []
                for i in range(100):
                    if urlparse(data_url).query[i + 4] == "&":
                        break
                    a.append(urlparse(data_url).query[i + 4])
                Branch = ''.join(a)
                CommitSha = row['sha']
                CommitDate = row['commit']['author']['date']
                try:
                    CommitAuthorLogin = row['author']['login']
                except:
                    CommitAuthorLogin = ""
                try:
                    CommitAuthorId = row['author']['id']
                except:
                    CommitAuthorId = ""
                Data = ""
                Tag = RepoOwner + "/" + RepoName + "-" + Branch + "-" + StartTime
                Month = StartTime
                try:
                    output.writerow([RepoOwner, RepoName, Branch, CommitSha, CommitAuthorLogin, CommitAuthorId, CommitDate, Data, Tag, Month])
                except:
                    print(row.values())
            print("The URL: {}{} is complete!".format(data_url, j))
            output_url_2.writerow(["The URL:",data_url, j ,"is complete!"])
            j += 1
            result = requests.get(data_url + str(j), headers=headers)
            html = result.text
            if html == '[\n\n]\n':
                break

def crawler():
    while not work.empty():
        data_url = work.get_nowait()
        try:
            get_each_url_all_page(data_url)
        except:
            print("The URL: {}1 has something wrong!".format(data_url))
            output_url.writerow(["The URL:",data_url,str(1), "has something wrong!"])

tasks_list = []
for x in range(10):
    task = gevent.spawn(crawler)
    tasks_list.append(task)
gevent.joinall(tasks_list)

File1.close()
File2.close()
File3.close()

end = time.time()

print("Take:"+ str(end - start)+"s")

DROP_TABLE_IF_EXIST = """.drop extents <| .show table BranchCommits extents where tags has 'drop-by:{}'""".format(StartTime)
RESPONSE = ls[0].execute_mgmt("DevRelWorkArea", DROP_TABLE_IF_EXIST)

Ingest(StartTime)