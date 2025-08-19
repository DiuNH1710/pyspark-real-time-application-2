import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

from udfs import *

logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Data_transformation')
def data_report1(df_city_sel, df_presc_sel):
    try:
        loggers.warning("processing the data_report1 method...")

        loggers.warning("calculation total zip counts in {}".format(df_city_sel))

        df_city_split = df_city_sel.withColumn('zipcounts',column_split_count(df_city_sel.zips))
        loggers.warning("caculating distinct prescribers and total tx_cnt")

        df_presc_grp= (df_presc_sel.groupBy(df_presc_sel.presc_state, df_presc_sel.presc_city)
                       .agg(countDistinct("presc_id").alias('presc_counts'),
                        sum("tx_cnt").alias('tx_count'))
                       )
        loggers.warning("Don't report a city if no prescriber is assigned to it ... lets join df_city_sel and df_presc_grp")

        df_city_join = df_city_split.join(df_presc_grp, (df_city_sel.state_id == df_presc_grp.presc_state) & (df_city_sel.city == df_presc_grp.presc_city), 'inner')

        df_final = df_city_join.select("city", "state_name", "county_name", "population",
                                       "zipcounts", "presc_counts")

    except Exception as e:
        loggers.error("An error occured while dealing data_report1...", str(e))
        raise
    else:
        loggers.warning("Data_report1 successfully executed... go frwd")
    return df_final