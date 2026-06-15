import pandas as pd
import numpy as np

df = pd.read_csv('mental_health.csv')

df['self_employed'] = np.where(df['self_employed'] == 'Yes', 'true', 'false')
df['family_history'] = np.where(df['family_history'] == 'Yes', 'true', 'false')
df['treatment'] = np.where(df['treatment'] == 'Yes', 'true', 'false')
df['Coping_Struggles'] = np.where(df['Coping_Struggles'] == 'Yes', 'true', 'false')

df['Timestamp'] = pd.to_datetime(df['Timestamp']).dt.date.astype(str)

df = df.assign(rn=list(range(1, len(df) + 1)))
df = df[:100000]
print(df.shape[0])
df.to_csv(
    'mh_transformed.csv',
    index=False,
    columns=['rn', 'Timestamp', 'Gender', 'Country', 'Occupation', 'self_employed', 'family_history', 'treatment',
             'Days_Indoors', 'Growing_Stress', 'Changes_Habits', 'Mental_Health_History', 'Mood_Swings',
             'Coping_Struggles', 'Work_Interest', 'Social_Weakness', 'mental_health_interview', 'care_options'])
