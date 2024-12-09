import pandas as pd
from rapidfuzz.fuzz import ratio
from rapidfuzz import process

df_1 = pd.read_csv("most_streamed_spotify_songs_till2022.csv")
df = pd.read_csv("./final_dataset_100k.csv")
merged = pd.read_csv("merged.csv")

df['track_artist_tuple'] = list(zip(df['track_name'], df['artists']))

def find_best_matches(df, df_1, track_threshold=66, artist_threshold=66):
    matches = []

    for idx_1, row_1 in df_1.iterrows():
        track_1 = row_1['Track']
        artist_1 = row_1['Artist']

        if (track_1, artist_1) in merged[['Track', 'Artist']].itertuples(index=False, name=None):
            # print(f"{track_1} by {artist_1} already in merged")
            continue

        for idx_df, row_df in df.iterrows():
            track_2 = row_df['track_name']
            artist_2 = [artist.strip() for artist in row_df['artists'].split(',')]

            if (len(artist_2) == 1):
              continue

            # if track_2 not in [
            #   "Mood (feat. iann dior)",
            #   "Leave The Door Open",
            #   "Crazy What Love Can Do",
            #   "One Dance",
            #   "Fair Trade (with Travis Scott)",
            #   "Jimmy Cooks (feat. 21 Savage)",
            #   "Jimmy Cooks (feat. 21 Savage)",
            #   "She Don't Give a Fo",
            #   "Cold Heart - PNAU Remix",
            #   "Till I Collapse",
            #   "Love The Way You Lie",
            #   "Godzilla (feat. Juice WRLD)",
            #   "WAIT FOR U (feat. Drake & Tems)",
            #   "New Gold (feat. Tame Impala and Bootie Brown)",
            #   "Enemy (with JID) - from the series Arcane League of Legends",
            #   "Enemy (with JID) - from the series Arcane League of Legends",
            #   "Enemy (with JID) - from the series Arcane League of Legends",
            #   "Enemy (with JID) - from the series Arcane League of Legends",
            #   "Peaches (feat. Daniel Caesar & Giveon)",
            #   "Die Hard",
            #   "Shallow",
            #   "Bamba (feat. Aitch & BIA)",
            #   "Payphone",
            #   "Volando - Remix",
            #   "LA INOCENTE",
            #   "MEMORIAS",
            #   "Miss You",
            #   "Miss You",
            #   "BABY OTAKU",
            #   "death bed (coffee for your head)",
            #   "Kesariya (From \"Brahmastra\")",
            #   "Desesperados",
            #   "Desesperados",
            #   "Calm Down (with Selena Gomez)",
            #   "Te Felicito",
            #   "I Was Never There"
            # ]:
            #   continue
            



            track_score = ratio(track_1, track_2)
            artist_score = max(ratio(artist_1, artist) for artist in artist_2)

            if track_score >= track_threshold and artist_score >= artist_threshold:
                print(artist_score, artist_1, artist_2, track_score, track_1, track_2)
                print(idx_df,idx_1)
                matches.append({
                    'df_index': idx_df,
                    'df_1_index': idx_1,
                    'track_score': track_score,
                    'artist_score': artist_score
                })
                df_matches = pd.DataFrame(matches)
                print(df_1.loc[df_matches['df_1_index']])
    
    print("done")

    return matches

import pickle

matches = find_best_matches(df, df_1)

with open('array.bin', 'wb') as file:
    pickle.dump(matches, file)

print(matches)

if matches:
    df_indexes = [match['df_index'] for match in matches]
    df_1_indexes = [match['df_1_index'] for match in matches]
    new_df = df.iloc[df_indexes]
    print("newdf",new_df)
    new_df_1 = df_1.iloc[df_1_indexes]
    print("newdf1",new_df_1)
    new_df.to_csv("/new_1.csv")
    new_df_1.to_csv("/new_2.csv")

    # df_matches = pd.DataFrame(matches)
#     # Use indices to align data
#     merged_df = pd.merge(
#         df.loc[df_matches['df_index']],
#         df_1.loc[df_matches['df_1_index']].reset_index(drop=True),
#         left_index=True,
#         right_index=True,
#         suffixes=('_df', '_df_1')
#     )
# else:
#     merged_df = pd.DataFrame()  # No matches found

# print("\nMerged DataFrame:")
# print(merged_df)
    