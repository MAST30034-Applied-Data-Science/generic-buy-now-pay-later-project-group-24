from pyspark.sql import SparkSession
import os


class Utils_function:
    def __init__(self, SparkSession, ptw, truncate):
        self.spark_session:SparkSession = SparkSession
        self.ptw = ptw
        self.truncate = truncate
        self.dataframe = None
    
    def read_file(self, file_name, type, sep):
        '''
       To read different type of file use spark. And show the first metadata after read.

       Parameters
       ----------
       spark_session : DataFrame
       file_name: str
           The full name of the file to read.
       ptw :
           The relative path of the file to read, default '../data/tables/'
       type : {'parquet', 'csv'}, default 'parquet'
       truncate : int, default 80
           Parameter of `show` function spark dataframe, which control the maximum
           number of characters per row.
       sep : str, default ','
           For csv reading, control the seperate character.


       Returns
       -------
       Spark DataFrame
           A DataFrame of the read file.


       Examples
       --------
       >>> sdf = read_file(spark, 'tbl_merchants.parquet')
       |> Loading File...
       |> Loading Finished!
       -RECORD 0----------------------------------------------------------------------------------------
        name         | Felis Limited
        tags         | ((furniture, home furnishings and equipment shops, and manufacturers, except ...
        merchant_abn | 10023283211
       only showing top 1 row

       >>> sdf = read_file(spark, 'tbl_merchants.parquet', truncate=20)
       |> Loading File...
       |> Loading Finished!
       -RECORD 0----------------------------
        name         | Felis Limited
        tags         | ((furniture, home...
        merchant_abn | 10023283211
       only showing top 1 row

       >>> sdf = read_file(spark, 'tbl_consumer.csv', type='csv', sep='|')
       |> Loading File...
       |> Loading Finished!
       -RECORD 0---------------------------------
        name        | Yolanda Williams
        address     | 413 Haney Gardens Apt. 742
        state       | WA
        postcode    | 6935
        gender      | Female
        consumer_id | 1195503
       only showing top 1 row
           '''
        print('|> Loading File...')
        if type == 'csv':
            self.dataframe = self.spark_session.read.csv(f'{self.ptw}{file_name}', sep=sep, header=True)

        elif type == 'parquet':
            self.dataframe = self.spark_session.read.parquet(f'{self.ptw}{file_name}', sep=sep, header=True)
        print('|> Loading Finished!')

        # print the first row of data
        self.dataframe.show(1, vertical=True, truncate=self.truncate)
        return self.dataframe
    
    def create_folder(self, folder_path):
        '''
        Create folder.

        Parameters
        ----------
        path : str
            The relative path of the new folder. 


        Examples
        --------
        >>> create_folder('../data/temp')
        |> Create Successfully!

        >>> create_folder('../data/tables/consumer_user_details.parquet')
        |> The folder name duplicated with a file!
        |> Files already exist under the upper folder:
        ['transactions_20210228_20210827_snapshot', '.DS_Store', '.gitkeep', 'consumer_user_details.parquet', 'tbl_consumer.csv', 'tbl_merchants.parquet']

        >>> create_folder('../data/tables')
        |> The folder already exist!
        |> Files already exist under this folder:
        ['transactions_20210228_20210827_snapshot', '.DS_Store', '.gitkeep', 'consumer_user_details.parquet', 'tbl_consumer.csv', 'tbl_merchants.parquet']
            '''
        # folder should not already exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print('|> Create Successfully!')
        # if the folder already created, the print out the files under this folder
        elif os.path.isdir(folder_path):
            print(f'|> The folder already exist!\n|> Files already exist under this folder:\n   {os.listdir(folder_path)}')
            
            # the name of the new folder is the same as a file already exist under the upper folder
        elif os.path.isfile(folder_path):
            upper_path = '/'.join(folder_path.split('/')[:-1])
            print(f'|> The folder name duplicated with a file!\n|> Files already exist under the upper folder:\n   {os.listdir( upper_path )}')


    def temp_record_sdf(self, spark_df, path, overwrite = False):
        '''
        Save current progress for future steps

        Parameters
        ----------
        sdf : spark dataframe
        path : str
            Path to save data, default as `../data/temp`
        overwrite : bool
            Set if cover the origin data, default False

        Examples
        --------
        >>> temp_record_sdf(sdf, path='../data/temp')
        >>> temp_record_sdf(sdf, path='../data/temp')
        >>> temp_record_sdf(sdf, path='../data/temp', overwrite=True)
        |> Waiting for saving...
        |> Save Successfully!
        --
        |> Waiting for saving...
        |> The folder already exist! Change the attr `overwrite` to cover the origin data.
        -- 
        |> Waiting for saving...
        |> Save Successfully!

        >>> print(os.listdir( '../data' ))
        >>> print(os.path.isfile( '../data/temp.parquet' ))
        >>> temp_record_sdf(sdf, path='../data/temp.parquet')
        >>> temp_record_sdf(sdf, path='../data/temp.parquet', overwrite=True)
        ['tables', '.gitkeep', 'README.md', 'temp.parquet', 'curated']
        --
        True
        --
        |> The name duplicated with a file!
        Change the name or change the attr `overwrite` to cover the origin data.
        --
        |> Waiting for saving...
        |> Save Successfully!
            '''
        # folder should not already exist
        if not os.path.exists(path):
            print('|> Waiting for saving...')
            spark_df.write.parquet(path)
            print('|> Save Successfully!')
        
        # if the folder already created, the print out the files under this folder
        elif os.path.isdir(path):
            try:
                print('|> Waiting for saving...')
                if (overwrite):
                    spark_df.write.partitionBy('order_datetime').parquet(path, mode = 'overwrite')
                else:
                    spark_df.write.parquet(path)
                print('|> Save Successfully!')
            except Exception:
                print('|> The folder already exist! Change the attr `overwrite` to cover the origin data.')
        
        # the name of the new folder is the same as a file already exist under the upper folder
        elif os.path.isfile(path):
            if (overwrite):
                print('|> Waiting for saving...')
                spark_df.write.parquet(path, mode = 'overwrite')
                print('|> Save Successfully!')
            else:
                print(f'|> The name duplicated with a file!\n   Change the name or change the attr `overwrite` to cover the origin data.')


    def temp_record_query(sql_query:SparkSession.sql, *cols, path, overwrite = False):
        #     '''
        # Save current progress for future steps

        # Parameters
        # ----------
        # sql_query : spark sql query
        # *cols : 'ColumnsOrName'
        #     Name of columns.
        # path : str
        #     Path to save data, default as `../data/temp`
        # overwrite : bool
        #     Set if cover the origin data, default False


        # Examples
        # --------
        # >>> sql_query = sdf.orderBy('merchant_abn')
        # >>> temp_record_query(sql_query, 'name', 'tags', 'merchant_abn')
        # |> Waiting for saving...
        # |> Save Successfully!
        #     '''
        Utils_function.temp_record_sdf(sql_query.toDF(*cols), path=path, overwrite=overwrite)


if __name__ == '__main__':
    spark_session = (
        # Create a spark session (which will run spark jobs)
        SparkSession.builder.appName("Project 1")
        .config("spark.sql.repl.eagerEval.enabled", True)
        .config("spark.sql.parquet.cacheMetadata", "true")
        .config('spark.executor.memory', '10g')
        .config('spark.driver.memory', '12g')
        .config('spark.driver.maxResultsSize', '10 GiB')
        .config('spark.shuffle.file.buffer', '64k')
        # .config("spark.network.timeout", "3600s")
        # .master("local[6]")
        .getOrCreate()
        )
    utils= Utils_function(spark_session,'../generic-buy-now-pay-later-project-group-24/data/tables/', 80)
    sdf = Utils_function.read_file(utils, file_name='consumer_user_details.parquet',type='parquet', sep=',')
    Utils_function.create_folder(utils,'../generic-buy-now-pay-later-project-group-24/data/temp')
    Utils_function.temp_record_sdf(utils, sdf, path='../generic-buy-now-pay-later-project-group-24/data/temp')
    








