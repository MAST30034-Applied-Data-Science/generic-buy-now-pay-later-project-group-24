from pyspark.sql import SparkSession
import os



def read_file(spark_session: SparkSession, file_name, ptw='../data/tables/', type='parquet', truncate=80, sep=','):
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

    # read file
    print('|> Loading File...')
    if type == 'csv':
        sdf = spark_session.read.csv(f'{ptw}{file_name}', sep=sep, header=True)

    elif type == 'parquet':
        sdf = spark_session.read.parquet(f'{ptw}{file_name}')
    print('|> Loading Finished!')

    # print the first row of data 
    sdf.show(1, vertical=True, truncate=truncate)
    return sdf



def create_folder(path):
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
    if not os.path.exists(path):
        os.makedirs(path)
        print('|> Create Successfully!')
    
    # if the folder aleady created, the print out the files under this folder
    elif os.path.isdir(path):
        print(f'|> The folder already exist!\n|> Files already exist under this folder:\n   {os.listdir(path)}')
    
    # the name of the new folder is the same as a file already exist under the upper folder
    elif os.path.isfile(path):
        upper_path = '/'.join(path.split('/')[:-1])
        print(f'|> The folder name duplicated with a file!\n|> Files already exist under the upper folder:\n   {os.listdir( upper_path )}')
    return 




def temp_record_sdf(sdf:SparkSession, path = '../data/temp', overwrite = False):
    '''
Save current progress for future steps

Parameters
----------
sdf : spark dataframe
path : str
    Path to save data, defualt as `../data/temp`
overwrite : bool
    Set if cover the origin data, defualt False

Examples
--------
>>> temp_record_sdf(sdf, path='../data/temp')
>>> temp_record_sdf(sdf, path='../data/temp')
>>> temp_record_sdf(sdf, path='../data/temp', overwrite=True)
|> Waitting for saving...
|> Save Successfully!
--
|> Waitting for saving...
|> The folder already exist! Change the attr `overwrite` to cover the origin data.
-- 
|> Waitting for saving...
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
|> Waitting for saving...
|> Save Successfully!
    '''


    # folder should not already exist
    if not os.path.exists(path):
        print('|> Waitting for saving...')
        sdf.write.parquet(path)
        print('|> Save Successfully!')
    
    # if the folder aleady created, the print out the files under this folder
    elif os.path.isdir(path):
        try:
            print('|> Waitting for saving...')
            if (overwrite):
                sdf.write.partitionBy('order_datetime').parquet(path, mode = 'overwrite')
            else:
                sdf.write.parquet(path)
            print('|> Save Successfully!')
        except Exception:
            print('|> The folder already exist! Change the attr `overwrite` to cover the origin data.')
    
    # the name of the new folder is the same as a file already exist under the upper folder
    elif os.path.isfile(path):
        if (overwrite):
            print('|> Waitting for saving...')
            sdf.write.parquet(path, mode = 'overwrite')
            print('|> Save Successfully!')
        else:
            print(f'|> The name duplicated with a file!\n   Change the name or change the attr `overwrite` to cover the origin data.')

    return 




def temp_record_query(sql_query:SparkSession.sql, *cols,\
    path='../data/temp', overwrite = False):
    '''
Save current progress for future steps

Parameters
----------
sql_query : spark sql query
*cols : 'ColumnsOrName'
    Name of columns.
path : str
    Path to save data, defualt as `../data/temp`
overwrite : bool
    Set if cover the origin data, defualt False


Examples
--------
>>> sql_query = sdf.orderBy('merchant_abn')
>>> temp_record_query(sql_query, 'name', 'tags', 'merchant_abn')
|> Waitting for saving...
|> Save Successfully!
    '''
    # convert to spark dataframe and save
    temp_record_sdf(sql_query.toDF(*cols), path=path, overwrite=overwrite)
    return 