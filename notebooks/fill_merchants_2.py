import pandas
import tqdm
import collections
import numpy
import random


def main():
    print("Reading data...")
    #data = pandas.read_parquet('dataset.parquet')
    data = pandas.read_parquet('../data/curated/data')
    print("Sorting data...")
    data = data.sort_values(by=['order_datetime'])

    print("Preparing statistical model...")
    user_order_types = collections.defaultdict(list)
    user_typed_merchants = collections.defaultdict(collections.Counter)
    user_type_transfer = collections.defaultdict(float)
    merchants = {}
    fillset = ['merchant_abn', 'merchant_name', 'type', 'tags', 'take_rate']
    merchant_info = data.drop_duplicates(subset=['merchant_abn']).dropna(subset=fillset)
    for entry in merchant_info.itertuples():
        merchants[entry.merchant_abn] = entry
    start = '/s'
    for uid, ty, abn in tqdm.tqdm(zip(data['user_id'], data['type'], data['merchant_abn']), total=len(data)):
        order_types = user_order_types[uid]
        while len(order_types) < 2:
            order_types.append(start)
        user_type_transfer[uid, order_types[-2], order_types[-1], ty] += 1
        order_types.append(ty)
        user_typed_merchants[uid, ty].update([abn])
    types = list(data['type'].unique()) + [start]
    for uid, order_types in tqdm.tqdm(user_order_types.items()):
        for t1 in types:
            for t2 in types:
                s = 0.0
                for t3 in types:
                    s += user_type_transfer[uid, t1, t2, t3]
                for t3 in types:
                    user_type_transfer[uid, t1, t2, t3] /= s + 1e-8
    print("Validating model...")
    ppl = []
    for uid, orders in user_order_types.items():
        for h, m, t in zip(orders, orders[1:], orders[2:]):
            ppl.append(numpy.log(user_type_transfer[uid, h, m, t]))
        if len(ppl) > 5000:
            break
    print("Type PPL:", numpy.exp(-numpy.mean(ppl)))
    print("Merchant PPL:", numpy.mean(list(map(len, user_typed_merchants.values()))))
    print("Filling NA...")

    user_order_types = collections.defaultdict(list)
    types_concr = data['type'].dropna().unique()
    filled_stats = [0, 0]
    for i, uid, isna, ty in zip(tqdm.tqdm(data.index), data['user_id'], data['merchant_name'].isna(), data['type']):
        order_types = user_order_types[uid]
        while len(order_types) < 2:
            order_types.append(start)
        if isna:
            nty = max(types_concr, key=lambda ty: user_type_transfer[uid, order_types[-2], order_types[-1], ty])
            fill_kind = 0
            for _ in range(30):
                counter = user_typed_merchants[uid, nty]
                if len(counter):
                    (abn, _), = user_typed_merchants[uid, nty].most_common(1)
                    break
                nty = random.choice(types_concr)
                fill_kind = 1
            filled_stats[fill_kind] += 1
            merchant = merchants[abn]
            for fill_attr in fillset:
                data.loc[i, fill_attr] = getattr(merchant, fill_attr)
        order_types.append(ty)
    print("Validating filled data...")
    print("Fill kind stats:", filled_stats)
    for fill_attr in fillset:
        print(fill_attr + ":", "fail" if data[fill_attr].isna().any() else "pass")
    print("Writing results to disk...")
    data.to_parquet('filled.parquet')
    print("All done!")


if __name__ == '__main__':
    main()
