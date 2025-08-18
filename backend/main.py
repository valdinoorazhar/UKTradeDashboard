import pandas as pd
from obs import get_json

obs = get_json('Nov-22', '0', 'IM')

dump = []

for i in range(len(obs["observations"])):
    row_dump = []
    label_i = obs["observations"][i]["dimensions"]["CountriesAndTerritories"]["id"]
    val_i = obs["observations"][i]["observation"]
    row_dump = [label_i, val_i]
    dump.append(row_dump)

df = pd.DataFrame(dump, columns=['Country', 'Trade_Value'])
df.to_csv('output_try_0.csv')