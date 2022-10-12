
# Generic Buy Now, Pay Later Project
Groups should generate their own suitable `README.md`.

Note to groups: Make sure to read the `README.md` located in `./data/README.md` for details on how the weekly datasets will be released.

---

## Team members
| Name | Contact | Student Id |
| ---- | ---- | ---- |
| Ziwen Xu | ziwen2@student.unimelb.edu | 1166426 |
| Jiayi Xu | jixu5@student.unimelb.edu.au | 1165986 |
| Jinyu Cai| jinyuc1@student.unimelb.edu.au | 1166389 |
| Runyu Yang| runyuy@student.unimelb.edu | 1118665 |
| Jianzhi Gao | jianzhig@student.unimelb.edu | 1166514 |

---

### Download Data 
*external*: 
- `scripts/download_external.py`  
- - POA Geo dataset: `data/tables/external_POA/` & `data/tables/external_postcode.csv` (fill some null value)
- - SA2 Geo dataset: `data/tables/external_SA2/`
- - SA2 data: `data/tables/external_SA2_data.csv`
- - new cases per day: `data/tables/external_cases_daily_aus_NET.csv`

---

### Preprocessing
*Merchants*: 
- `notebooks/merchant_prep.ipynb` 
- - save to `data/curated/merchants_data.parquet`
- `notebooks/Bow.ipynb` --- for analyse the merchants name

*Consumer*: 
- `notebooks/consumer_prep.ipynb` --- save to `data/curated/new_consumer_data.parquet/`
*External*: 
- `notebooks/external_prep.ipynb` 
- - new cases per day: `data/curated/external_ncd.parquet/`
- - retail_trade: `data/curated/external_retail_trade_origin.csv` & `data/curated/external_retail_trade_seasonal.csv`
- `notebooks/geo_match.ipynb` 
- - deal with the external geo datasets and population distribution data and save to `data/curated/geo_pos_population.parquet'`
- `notebooks/external_geo.ipynb` 
- - deal with another external geo dataset and save to `data/curated/external_pos_population/` & `data/curated/external_pos_population.parquet`

---

### Data Connection

*dataset*: 
- `notebooks/connect.ipynb` 
- - save to `data/curated/data`

---

### Statistic Model

*dataset*: 
- `notebooks/change_type.ipynb` 
- - transform the format of input & save to `data/curated/dataset.parquet`

*fill_merchants*: 
- `models/fill_merchants.ipynb` 
- - save to `data/curated/filled.parquet`

*final_model*:
- `models/consumer_fraud.ipynb`
- -  Prediction of the probability of fraud for consumers by day and save to `data/curated/consumer_fraud/`
- `models/merchants_fraud.ipynb`
- -  Prediction of the probability of fraud for merchants by day and save to `data/curated/merchants_fraud/`

---
### Rank Model

- `models/plot.ipynb`
- - Categorisation of merchants according to consumer spending habits
- `models/rank_model.ipynb`
- - The final fraud probability value is calculated, the merchants are ranked for each segment and the merchants are ranked overall and save to `data/curated/rank_final.parquet`

---
### Summary Notebooks
*Summary*
- `models/Summary.ipynb`
- - For a summary of the entire code as well as the graphical presentation and the presentation of the results.