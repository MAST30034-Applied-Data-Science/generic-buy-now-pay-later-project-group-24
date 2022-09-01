from pyspark.sql import SparkSession
import os


class Utils_function:
    '''
    import sys
    path = os.path.join(os.path.dirname(os.getcwd()), "scripts")
    sys.path.append(path)
    from utils import Utils_function
    '''
    def __init__(self, SparkSession, ptw, truncate):
        '''
        To read different type of file use spark. And show the first metadata after read.

        Parameters
        ----------
        self : class
        SparkSession : DataFrame
        ptw :
            The relative path of the file to read, default '../data/tables/'
        truncate : int, default 80
            Parameter of `show` function spark dataframe, which control the maximum
            number of characters per row.
        Examples
        --------
        >>> utils= Utils_function(SparkSession = spark_session, ptw = '../generic-buy-now-pay-later-project-group-24/data/tables/', truncate = 80)
        >>> utils= Utils_function(SparkSession = spark_session, ptw = '../generic-buy-now-pay-later-project-group-24/data/tables/', truncate = 20)
            '''
        self.spark_session:SparkSession = SparkSession
        self.ptw = ptw
        self.truncate = truncate
        self.dataframe = None
    
    def read_file(self, file_name, type, sep):
        '''
       To read different type of file use spark. And show the first metadata after read.

       Parameters
       ----------
       self : class
       file_name: str
           The full name of the file to read.
       type : {'parquet', 'csv'}, default 'parquet'
       sep : str, default ','
           For csv reading, control the seperate character.

       Returns
       -------
       Spark DataFrame
           A DataFrame of the read file.

       Examples
       --------
       >>> sdf = Utils_function.read_file(self = utils, file_name='consumer_user_details.parquet',type='parquet', sep=',')
       |> Loading File...
       |> Loading Finished!
       -RECORD 0----------------------------------------------------------------------------------------
        name         | Felis Limited
        tags         | ((furniture, home furnishings and equipment shops, and manufacturers, except ...
        merchant_abn | 10023283211
       only showing top 1 row

       >>> sdf = Utils_function.read_file(self = utils, file_name='tbl_consumer.csv',type='csv', sep='|')
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
        folder_path : str
            The relative path of the new folder. 


        Examples
        --------
        >>> Utils_function.create_folder(self = utils, folder_path = '../data/temp')
        |> Create Successfully!

        >>> Utils_function.create_folder(self = utils, folder_path = '../data/tables/consumer_user_details.parquet')
        |> The folder name duplicated with a file!
        |> Files already exist under the upper folder:
        ['transactions_20210228_20210827_snapshot', '.DS_Store', '.gitkeep', 'consumer_user_details.parquet', 'tbl_consumer.csv', 'tbl_merchants.parquet']

        >>> Utils_function.create_folder(self = utils, folder_path = '../data/tables')
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
        >>> Utils_function.temp_record_sdf(self = utils, path='../generic-buy-now-pay-later-project-group-24/data/temp')
        >>> Utils_function.temp_record_sdf(self = utils, path='../generic-buy-now-pay-later-project-group-24/data/temp')
        >>> Utils_function.temp_record_sdf(self = utils, path='../generic-buy-now-pay-later-project-group-24/data/temp', overwrite=True)
        |> Waiting for saving...
        |> Save Successfully!
        --
        |> Waiting for saving...
        |> The folder already exist! Change the attr `overwrite` to cover the origin data.
        -- 
        |> Waiting for saving...
        |> Save Successfully!

        >>> print(os.listdir( '../generic-buy-now-pay-later-project-group-24/data' ))
        >>> print(os.path.isfile( '../generic-buy-now-pay-later-project-group-24/data/temp.parquet' ))
        >>> Utils_function.temp_record_sdf(self = utils, path='../generic-buy-now-pay-later-project-group-24/data/temp.parquet')
        >>> Utils_function.temp_record_sdf(self = utils, path='../generic-buy-now-pay-later-project-group-24/data/temp.parquet', overwrite=True)
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


    def temp_record_query(self, sql_query:SparkSession.sql, *cols, path='../generic-buy-now-pay-later-project-group-24/data/temp', overwrite = False):
        '''
        Save current progress for future steps

        Parameters
        ----------
        sql_query : spark sql query
        *cols : 'ColumnsOrName'
            Name of columns.
        path : str
            Path to save data, default as `../data/temp`
        overwrite : bool
            Set if cover the origin data, default False


        Examples
        --------
        >>> sql_query = sdf.orderBy('merchant_abn')
        >>> temp_record_query(self = utils, sql_query = sql_query, 'name', 'tags', 'merchant_abn')
        |> Waiting for saving...
        |> Save Successfully!
            '''
        Utils_function.temp_record_sdf(sql_query.toDF(*cols), path=path, overwrite=overwrite)

    








