from azure.kusto.data.request import KustoConnectionStringBuilder, KustoClient
from azure.kusto.ingest import (
    KustoIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    StreamDescriptor,
    DataFormat,
    ReportLevel,
    IngestionMappingType,
    KustoStreamingIngestClient,
)


def Ingest(Tag):
    # setting
    AUTHORITY_ID = "6babcaad-604b-40ac-a9d7-9fd97c0b779f"
    INGESTCLUSTER = "https://ingest-cgadataout.kusto.windows.net"
    KUSTOCLUSTER = "https://cgadataout.kusto.windows.net"
    DATABASE = "DevRelWorkArea"

    # Create table
    KCSB_DATA = KustoConnectionStringBuilder.with_aad_device_authentication(KUSTOCLUSTER)
    DESTINATION_TABLE = "RepoContributors"
    DESTINATION_TABLE_COLUMN_MAPPING = "RepoContributors_CSV_Mapping"

    KUSTO_CLIENT = KustoClient(KCSB_DATA)
    DROP_TABLE_IF_EXIST = ".drop table RepoContributors ifexists"
    RESPONSE = KUSTO_CLIENT.execute_mgmt(DATABASE, DROP_TABLE_IF_EXIST)

    CREATE_TABLE_COMMAND = ".create table RepoContributors (Article: string, Contributors: int64, Data: string)"
    RESPONSE = KUSTO_CLIENT.execute_mgmt(DATABASE, CREATE_TABLE_COMMAND)

    print("RepoContributors table is created")

    # Create mapping

    CREATE_MAPPING_COMMAND = """.create table RepoContributors ingestion csv mapping 'RepoContributors_CSV_Mapping' '[{"Name": "Article","datatype": "string","Ordinal": 0},{"Name": "Contributors","datatype": "int64","Ordinal": 1},{"Name": "Data","datatype": "string","Ordinal": 2}]'"""
    RESPONSE = KUSTO_CLIENT.execute_mgmt(DATABASE, CREATE_MAPPING_COMMAND)

    print("mapping is created")

    # Ingest

    # The authentication method will be taken from the chosen KustoConnectionStringBuilder.
    ingestion_props = IngestionProperties(
        database="DevRelWorkArea",
        table="RepoContributors",
        dataFormat=DataFormat.CSV,
        ingestByTags=[Tag],
        dropByTags=[Tag],
        mappingReference=DESTINATION_TABLE_COLUMN_MAPPING,
        reportLevel=ReportLevel.FailuresAndSuccesses,
        additionalProperties={'ignoreFirstRecord': 'true'}
    )

    kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(INGESTCLUSTER)
    client = KustoIngestClient(kcsb)

    # ingest from file
    file_descriptor = FileDescriptor(r"D:\test\Results\log_data_merge\merge_microsoftdocs_sql-docs-pr.txt", 3333)  # 3333 is the raw size of the data in bytes.
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
    # if status updates are required, something like this can be done

    return 1


Ingest("merge_microsoftdocs_sql-docs-pr")