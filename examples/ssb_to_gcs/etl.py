import pandas as pd
import dataverk as dv

# TODO create dp first and then add datasets, views etc
datasets = {}

url = 'http://data.ssb.no/api/klass/v1/classifications/7/codes?from=1900-01-01&csvSeparator=;'
df = pd.read_csv(url, encoding='latin-1', sep=';', dtype={'code': object, 'parentCode':object})

df['version'] = df['validFromInRequestedRange'].apply(lambda x: 'styrk_98' if x == '1998-09-01' else 'styrk_08')
df.drop(['presentationName', 'validFrom', 'validTo', 'validFromInRequestedRange','validToInRequestedRange', 'shortName'], axis=1, inplace=True)
df.fillna('-1', inplace=True)

df_level4 = df[(df['level']==4) & (df['version']=='styrk_98')]
df_level3 = df[(df['level']==3) & (df['version']=='styrk_98')]
df_level2 = df[(df['level']==2) & (df['version']=='styrk_98')]
df_level1 = df[(df['level']==1) & (df['version']=='styrk_98')]

df_98 = pd.merge(df_level4, df_level3, left_on='parentCode', right_on='code', suffixes=('_4_98', '_3_98'))
df_98.drop(['parentCode_4_98', 'level_4_98', 'version_4_98','level_3_98','version_3_98'], axis=1, inplace=True)
df_98 = pd.merge(df_98, df_level2, left_on='parentCode_3_98', right_on='code', suffixes=('', '_2_98'))
df_98.rename(columns={'code': 'code_2_98', 'parentCode': 'parentCode_2_98','name': 'name_2_98'}, inplace=True)
df_98.drop(['level', 'version', 'parentCode_3_98'], axis=1, inplace=True)
df_98 = pd.merge(df_98, df_level1, left_on='parentCode_2_98', right_on='code', suffixes=('', '_1_98'))
df_98.drop(['parentCode_2_98', 'parentCode', 'version', 'level'], axis=1, inplace=True)
df_98.rename(columns={'code': 'code_1_98','name': 'name_1_98'}, inplace=True)

df_level4 = df[(df['level']==4) & (df['version']=='styrk_08')]
df_level3 = df[(df['level']==3) & (df['version']=='styrk_08')]
df_level2 = df[(df['level']==2) & (df['version']=='styrk_08')]
df_level1 = df[(df['level']==1) & (df['version']=='styrk_08')]

df_08 = pd.merge(df_level4, df_level3, left_on='parentCode', right_on='code', suffixes=('_4_08', '_3_08'))
df_08.drop(['parentCode_4_08', 'level_4_08', 'version_4_08','level_3_08','version_3_08'], axis=1, inplace=True)
df_08 = pd.merge(df_08, df_level2, left_on='parentCode_3_08', right_on='code', suffixes=('', '_2_08'))
df_08.rename(columns={'code': 'code_2_08', 'parentCode': 'parentCode_2_08','name': 'name_2_08'}, inplace=True)
df_08.drop(['level', 'version', 'parentCode_3_08'], axis=1, inplace=True)
df_08 = pd.merge(df_08, df_level1, left_on='parentCode_2_08', right_on='code', suffixes=('', '_1_08'))
df_08.drop(['parentCode_2_08', 'parentCode', 'version', 'level'], axis=1, inplace=True)
df_08.rename(columns={'code': 'code_1_08','name': 'name_1_08'}, inplace=True)

df_codes = df_98['code_4_98'].append(df_08['code_4_08']).drop_duplicates().reset_index(name='code')
df_merged = pd.merge(df_codes, df_98, left_on='code', right_on='code_4_98', how='left')
df_merged = pd.merge(df_merged, df_08, left_on='code', right_on='code_4_08', how='left')

datasets['styrk'] = df_merged

if __name__ == '__main__':
    dv.publish_datapackage(datasets, destination='gcs')