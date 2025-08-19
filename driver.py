import os
import sys

import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date, print_schema, check_for_nulls
from ingest import load_files, display_df, df_count
from data_processing import *
from data_transformation import *
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

def main():
    global file_format
    try:
        logging.info('i am in the main method')
        # print(gav.header)
            # print(gav.src_olap)
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn, gav.appName)


        logging.info('Validating spark object...')
        get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            print("File is " + file)

            file_dir = gav.src_olap + '\\' + file
            print(file_dir)
            if file.endswith('.parquet'):
                file_format ='parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema= gav.inferSchema

        logging.info('reading file which is of > {} '.format(file_format))

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logging.info("displaying the dataframe {}".format(df_city))
        display_df(df_city, 'df_city')

        logging.info('validating the dataframe...')
        df_count(df_city, 'df_city')

        for file2 in os.listdir(gav.src_oltp):
            print("File is " + file2)

            file_dir = gav.src_oltp + '\\' + file2
            print(file_dir)
            if file2.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif file2.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logging.info("display the dataframe {}".format(df_fact))
        display_df(df_fact, 'df_fact')

        logging.info('validating the dataframe...')
        #validate::
        df_count(df_fact, 'df_fact')
        logging.info("implementing data_processing methods...")
        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)

        display_df(df_city_sel, 'df_city')
        display_df(df_presc_sel, 'df_fact')

        logging.info("validating schema for the dataframes...")
        print_schema(df_city_sel, 'df_city_sel')
        print_schema(df_presc_sel, 'df_presc_sel')

        logging.info('checking for null values... ')
        check_df = check_for_nulls(df_presc_sel, 'df_fact')
        display_df(check_df, 'df_fact')

        logging.info('data transformation execute...')

        df_report_1 = data_report1(df_city_sel, df_presc_sel)
        logging.info('display the df_report_1')
        display_df(df_report_1, 'data_report')



    except Exception as exp:
        logging.error("An error occured when calling main() please check the trace === %s", str(exp))
        sys.exit(1)

if __name__ == '__main__':
    main()
    logging.info("Application done")