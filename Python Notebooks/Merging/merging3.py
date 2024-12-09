import pickle
import pandas as pd

with open('array.bin','rb') as f:
    arr = pickle.load(f)

df = pd.read_csv("final_dataset_100k.csv")
df_1 = pd.read_csv("most_streamed_spotify_songs_till2022.csv")

df_new = pd.DataFrame()
df_new_1 = pd.DataFrame()

for i in arr:
    print("NEW")
    row_1 = df.iloc[i['df_index']]
    row_2 = df_1.iloc[i['df_1_index']]

    df_new = pd.concat([df_new, row_1.to_frame().T], ignore_index=True)
    df_new_1 = pd.concat([df_new_1, row_2.to_frame().T], ignore_index=True)

df_new.to_csv("new.csv",index=False)
df_new_1.to_csv("new_1.csv",index=False)