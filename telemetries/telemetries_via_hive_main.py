import json
import argparse

from pyspark import SparkContext, Row
from pyspark.sql import HiveContext


def get_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from_date', required=True, help='model days back for training & evaluation')
    parser.add_argument('--to_date', required=True, help='model days back for training & evaluation')
    parser.add_argument('--target_path', required=True, help='Target path to push files on s3')
    parser.add_argument('--external_lib', required=True, action='append', help='External Lib')

    args = parser.parse_args()

    from_date = int(args.from_date)
    to_date = int(args.to_date)
    target_path = args.targetPath
    external_lib = args.externalLib

    return from_date, to_date, target_path, external_lib


record_example = {
    'carrierName': 'sprint',
    'deviceModel': 'LG-LS998',
    'deviceVendor': 'LGE',
    'deviceId': 'b46006d4-704b-4f4c-a175-326d0a979464',
    'date':'2018-02-08',
    'agentTimestamp':'2018-02-08',
    'os': 'android',
    'agentVersion': '4.369.13',
    'telemtryType': 'apps',
    'uuid':"1a65d57f-f016-11e7-b570-12619efc9baa",
    'installedApps': [
        {
            "LastUpdateTime": 1520387829239,
            "FirstInstallTime": 1520387829239,
            "VersionName": "1.18.0",
            "PackageName": "com.ubercab",
            "IsSystemApp": False,
            "Name": "Uber",
            "VersionCode": 13
        }
    ]
}


if __name__ == "__main__":
    (from_date, to_date, target_path, external_lib) = get_parameters()

    sc = SparkContext(appName="telemetries", pyFiles=external_lib)
    sql_context = HiveContext(sc)

    from telemetries.create_user_apps import load_sent_events
    from flaten_mixpanel_home_events.pyspark_schema_utils import rdd_to_df

    tele_sprint = sql_context.table("l1_sprint.telemetry_events")
    events_sprint = sql_context.table("l2_sprint.mixpanel_home")
    apps_data_rdd = tele_sprint.filter((tele_sprint.event_date >= '2018-05-01') & (tele_sprint.event_date <= '2018-05-01 ')) \
        .filter((tele_sprint.event_name == 'apps') | (tele_sprint.event_name == 'systemapps')) \
        .rdd.map(lambda x: (x.event_name , x.os, x.uuid, x.event_date, json.loads(x.json_data), x.agentversion)) \
        .filter(lambda x: x[4].get('IsTest', True) == False) \
        .map(lambda x: Row(carrierName=x[4]['CarrierName'],
                           deviceId=x[4]['DeviceId'],
                           deviceModel=x[4].get('DeviceModel',None),
                           deviceVendor=x[4].get('DeviceVendor',None),
                           agentTimestamp=x[4].get('AgentTimestamp',None),
                           agentVersion=x[5],
                           telemtryType=x[0],
                           os=x[1],
                           uuid=x[2],
                           date=x[3],
                           installedApps=x[4]['InstalledApps'])
             ).map(lambda x: x.asDict())


    df_tele = rdd_to_df(apps_data_rdd, record_example, sql_context)
    df_events = load_sent_events(events_sprint, from_date, to_date)

    df_tele.write.parquet(target_path, mode="overwrite")
    df_tele.write.partitionBy("Date").parquet("s3://eds-atlas-telaviv-nonprod/DevSandbox/tele-partitioned_by_date", mode="overwrite")
    df_tele.write.json("s3://eds-atlas-telaviv-nonprod/DevSandbox/alexK/tele-partitioned_by_date")

    sc.stop()
