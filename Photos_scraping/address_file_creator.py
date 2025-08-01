import pandas as pd

base_file=pd.read_csv('streets_exploded.csv', index_col=False)
dgis_stat=[0 for i in range(len(base_file))]
ya_stat=[0 for i in range(len(base_file))]
address_stat=[]

for i, row in base_file.iterrows():
    address=f'Иркутск, {row["street"]}, {row["house_intervals"]}'
    address_stat.append(address)
    
ids1=[i for i in range(len(base_file))]
result=pd.DataFrame({'id':ids1, 'address':address_stat, '2gis_stat':dgis_stat, 'ya_stat':ya_stat})
result.to_csv('addresses_stats.csv', index=False)