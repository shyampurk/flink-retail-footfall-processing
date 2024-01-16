from pyflink.table import *
from pyflink.table.expressions import col, call
from pyflink.table.udf import udf

ZONES = ("Main_Zone","Sub1_Zone","Sub2_Zone","Sub3_Zone","Sub4_Zone")

offset = {}

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def zone_id_to_name(id):
    return ZONES[id]


if __name__ == '__main__':
    
    t_env = TableEnvironment.create(environment_settings=EnvironmentSettings.in_batch_mode())

    source_ddl = f"""
            
            CREATE TABLE footfall_count(
                createTime VARCHAR,
                zoneCount BIGINT,
                zoneId INT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'footfall-in',
                'properties.bootstrap.servers' = 'redpanda:29092',
                'properties.group.id' = 'test-group',
                'scan.bounded.mode' = 'latest-offset',
                'scan.startup.mode' = 'specific-offsets',
                'scan.startup.specific-offsets' = 'partition:0,offset:""" + str(offset["latest"]) + """',
                'format' = 'json'
            )
            """

    sink_ddl = f"""
            CREATE TABLE `footfall_agg`(
                zoneName   VARCHAR,
                totalCount DOUBLE
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'footfall'
            )
            """

    
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    t_env.register_function('zone_id_to_name', zone_id_to_name)

    t_env.from_path("footfall_count") \
        .select(call('zone_id_to_name', col('zoneId')).alias("zone"), col('zoneCount')) \
        .group_by(col('zone')) \
        .select(col('zone'), call('sum', col('zoneCount')).alias("totalCount")) \
        .execute_insert("footfall_agg").wait()
