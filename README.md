# Generic Buy Now, Pay Later Project
Groups should generate their own suitable `README.md`.

Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details on how the weekly datasets will be released.


### Download Data 
*external*: `scripts/download_external.py`  --- POA Geo dataset: `data/tables/external_POA/` & `data/tables/external_postcode.csv` (fill some null value)
                                                SA2 Geo dataset: `data/tables/external_SA2/`
                                                SA2 data: `data/tables/external_SA2_data.csv`
                                                new cases per day: `data/tables/external_cases_daily_aus_NET.csv`

### Preprocessing
*Merchants*: `notebooks/merchant_prep.ipynb` --- save to `data/curated/merchants_data.parquet`
            `notebooks/Bow.ipynb` --- for analyse the merchants name
*Consumer*: `notebooks/consumer_prep2.ipynb` --- save to `data/curated/new_consumer_data.parquet/`
*External*: `notebooks/external_prep.ipynb` --- new cases per day: `data/curated/external_ncd.parquet/`
                                                retail_trade: `data/curated/external_retail_trade_origin.csv` & `data/curated/external_retail_trade_seasonal.csv`
            `notebooks/geo_mix.ipynb` --- deal with the external geo datasets

*dataset*: `notebooks/connect.ipynb` --- save to `data/curated/data`

*dataset*: `notebooks/change_type.ipynb` --- transform the format of input & save to `data/curated/dataset.parquet`
*fill_merchants*: `fill_merchants_2.ipynb` --- save to `data/curated/filled.parquet`

*final_model*: 