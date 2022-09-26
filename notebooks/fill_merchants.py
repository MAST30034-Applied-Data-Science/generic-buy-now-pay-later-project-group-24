import numpy
import pandas
import tqdm
import collections
import warnings


if __name__ == "__main__":
    data = pandas.read_parquet('dataset.parquet')
    print(data['merchant_name'].isna().sum(), 'empty values.')
    isna = numpy.array(data['merchant_name'].isna())
    user_revdict = collections.defaultdict(list)
    user_scandict = dict()
    fill_attrs = ['merchant_name', 'tags', 'take_rate', 'type']
    dropna = data.dropna()
    for idx, entry in tqdm.tqdm(dropna.iterrows(), total=len(dropna)):
        user_revdict[entry['user_id']].append(idx)
    for k, v in user_revdict.items():
        user_scandict[k] = numpy.array(dropna.loc[v, 'user_id'], dtype=numpy.datetime64).astype(numpy.int64)
    for i in tqdm.tqdm(numpy.argwhere(isna)):
        candidates = user_scandict[data.loc[i, 'user_id'].item()]
        entries = user_revdict[data.loc[i, 'user_id'].item()]
        key = numpy.datetime64(data.loc[i, 'order_datetime'].item()).astype(numpy.int64)
        argm = numpy.argmin(candidates - key)
        for fill_attr in fill_attrs:
            data.loc[i, fill_attr] = dropna.loc[entries[argm], fill_attr]
    print(data)
    print(data['merchant_name'].isna().any())
    data.to_parquet('filled.parquet')
    # print(data)
    # for k in data.keys():
    #     print(k, data[k].dtype)
    # print(data.groupby('consumer_id').count()['merchant_abn'])
    # discrete_attrs = ['user_id', 'state', 'postcode', 'gender']
    # discrete = numpy.unique(numpy.array(data[discrete_attrs], dtype='<U'), return_inverse=True)[-1].reshape(-1, 4)
    # discrete = numpy.array(discrete, numpy.uint16)
    # continuous_attrs = ['dollar_value']
    # continuous = numpy.array(data[continuous_attrs], dtype=numpy.float32)
    # merchant = numpy.array(data['merchant_abn'], dtype=numpy.int64)
    # print(discrete.dtype, continuous.dtype)
    # isna = numpy.array(data.isna().any("columns"))
    # template_dis = discrete[~isna]
    # template_cont = continuous[~isna]
    # merchant = merchant[~isna]

    # for i in tqdm.tqdm(numpy.argwhere(isna)):
    #     # dist = numpy.linalg.norm(continuous[i] - template_cont, axis=-1)
    #     dist = numpy.sum(discrete[i] != template_dis, axis=-1)
    #     merchant[numpy.argmin(dist)]
    # print(data.keys())
