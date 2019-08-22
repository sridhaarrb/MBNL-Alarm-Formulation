#!/usr/bin/env python
# coding: utf-8

# # Extracting Files

# In[ ]:


import tarfile
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
days=[]
gz_files1=[]
gz_files2=[]
tar_files = ['U2000.tar.gz','Darwin.tar.gz','Samsung.tar.gz','MBNL.tar.gz','LOUK.tar.gz','Nortel.tar.gz','Ericsson.tar.gz']
text = 'Sridhar_report_'
Processing_Files = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\Processing Files'
no_of_days = int(input('Enter the number of days to be checked: '))

for i in range(1,no_of_days+1,1):
    days.append(datetime.strftime(datetime.now() - timedelta(i), '%Y-%m-%d'))

for i in days:
    gz_files1.append(text+i+'.tar.gz')
        
for i in days:
    for j in tar_files:
        gz_files2.append(text+i+'_'+j)
        
for i in gz_files1:
    print ('Taken: ',i)
    Procesing_full_path = os.path.join(Processing_Files,i)
    tf = tarfile.open(Procesing_full_path)
    tf.extractall(Processing_Files)

for i in gz_files2:
    print ('Taken: ',i)
    Procesing_full_path = os.path.join(Processing_Files,i)
    tf = tarfile.open(Procesing_full_path)
    tf.extractall(Processing_Files)


# ## Reading INSERT File

# In[ ]:


import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
Y_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
T_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
insert_path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\INSERT'
Output_Path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing'
insert_file_yesterday = 'INSERT_'+Y_date+'.xlsx'
insert_file_today = 'INSERT_'+T_date+'.xlsx'
insert_full_path = os.path.join(insert_path,insert_file_yesterday)
try:
    df_consolidated_insert = pd.read_excel('C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\INSERT\Insert_Consolidated.xlsx')
except:
    print ('No previous day INSERT file available')


# In[ ]:


from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
insert_path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\INSERT'
Output_Path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing'
Y_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')


# In[3]:


dict_previous_day={}
dict_yesterday={}
list_files = os.listdir(r"C:\Users\eubcefm\Desktop\MBNL Alarm Processing\Processing Files")
csv_files = [i for i in list_files if i.endswith('.csv')]
print (Y_date)
for i in csv_files:
    print ('Taken: ',i)
    key = i.split('.')[0][26:]
    print (key)
    if Y_date not in i:
        dict_previous_day.setdefault(key,[]).append(i)
    else:
        dict_yesterday[i.split('.')[0][26:]]=i


# In[4]:


print (dict_previous_day)
print (dict_yesterday)
Y_date


# In[5]:


columns = ['AlarmId','HistoryAction','Date','AlarmKey','Node','IPAddress','Summary','SubMethod','TicketId','Location',
    'Ack','Count','Severity','Detail1','Detail2','Detail3','Detail4','Detail5','Detail6','Detail7','Detail8',
    'Detail9','Detail10','Detail11','Detail12','B2BSite','AlarmClass','AlarmFamily','AlarmSubFamily','CellID',
    'CellSiteID','Class','NEMName','Region','SiteID','Postcode','Technology','from_unixtime(Reported)','from_unixtime(StateChange)']

output_col = ['AlarmId','HistoryAction','Date','AlarmKey','Node','IPAddress','Summary','Derived_Summary','SubMethod','Domain','Sub_Domain',
              'TicketId','Location','Ack','Count','Severity','Detail1','Detail2','Detail3','Detail4','Detail5','Detail6',
              'Detail7','Detail8','Detail9','Detail10','Detail11','Detail12','B2BSite','AlarmClass','AlarmFamily',
              'AlarmSubFamily','CellID','CellSiteID','Derived_Node','Class','NEMName','Region','SiteID','Postcode','Vendor',
              'First_Occurance', 'Last_Occurance','Clerance', 'Bounce_Count', 'Observation']

Processing_Files = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\Processing Files'
Wanted_alarm_id = []

for m in dict_yesterday:
    Alarm_id_overall = []
    dict_value = dict_yesterday.get(m)
    full_path = os.path.join(Processing_Files,dict_value)
    df_dump_final = pd.read_csv(full_path,usecols = columns)
    A=['INSERT','UPDATE','DELETE']
    df_dump_final.dropna(axis=0,inplace=True,how='all')
    df_dump_final.dropna(subset=['AlarmId'],axis=0,inplace=True)
    df_dump_final.dropna(subset=['Severity'],axis=0,inplace=True)
    df_dump_final.dropna(subset=['from_unixtime(Reported)'],axis=0,inplace=True)
    df_dump_final.replace({'CellID':['\\N','0'],'CellSiteID':['\\N','0'], 'Class':['\\N','0'], 'SiteID':['\\N','0']},np.NaN,inplace=True)
    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].fillna(0)
    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].astype('str')
    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].astype('float')
    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].astype('int')
    Q = [0,1,2,3,4,5,6,'0','1','2','3','4','5','6',1.0,2.0,3.0,4.0,5.0,6.0]
    df_dump_final['AlarmId'] = pd.to_numeric(df_dump_final['AlarmId']).astype('int64')
    df_dump_final = df_dump_final[df_dump_final['HistoryAction'].isin(A)]
    df_dump_final = df_dump_final[df_dump_final['Severity'].isin(Q)]
    df_dump_final['Summary'] = df_dump_final['Summary'].astype('str')
    df_Summary = df_dump_final[df_dump_final['Summary'].apply(lambda x: x.startswith('Summary') and len(x)>100)]
    df_dump_final.drop(index=df_Summary.index, axis=1,inplace=True)
    Alarm_id_overall = list(df_dump_final['AlarmId'].unique())
    Wanted_alarm_id.extend(Alarm_id_overall)
    print (len(Wanted_alarm_id))


# In[6]:


dict_consolidated = {}
items  = dict_previous_day.items()

for i in items:
    df_dump_insert = pd.DataFrame(columns=columns)
    for j in i[1]:
        full_path = os.path.join(Processing_Files,j)
        df_input_insert = pd.read_csv(full_path,usecols = columns)
        A=['INSERT','UPDATE','DELETE']
        df_input_insert.dropna(axis=0,inplace=True,how='all')
        df_input_insert.dropna(subset=['AlarmId'],axis=0,inplace=True)
        df_input_insert.dropna(subset=['Severity'],axis=0,inplace=True)
        Q = [0,1,2,3,4,5,6,'0','1','2','3','4','5','6',1.0,2.0,3.0,4.0,5.0,6.0]
        df_input_insert[pd.to_numeric(df_input_insert['AlarmId'], errors='coerce').notnull()]
        df_input_insert = df_input_insert[df_input_insert['HistoryAction'].isin(A)]
        df_input_insert = df_input_insert[df_input_insert['Severity'].isin(Q)]
        df_input_insert['Severity'] = pd.to_numeric(df_input_insert['Severity']).astype('int64')
        df_input_insert['AlarmId'] = pd.to_numeric(df_input_insert['AlarmId']).astype('int64')
        df_input_insert['Count'] = pd.to_numeric(df_input_insert['Count']).astype('int64')
        df_input_insert['from_unixtime(Reported)'] = pd.to_datetime(df_input_insert['from_unixtime(Reported)'])
        df_input_insert['from_unixtime(StateChange)'] = pd.to_datetime(df_input_insert['from_unixtime(StateChange)'])
        df_input_insert = df_input_insert[df_input_insert['AlarmId'].isin(Wanted_alarm_id)]
        df_input_insert.reset_index(inplace=True,drop=True)
        df_dump_insert = df_dump_insert.append(df_input_insert,ignore_index=True)
    name = 'df_'+i[0]+'_consolidated'
    dict_consolidated[name] = df_dump_insert.copy(deep=True)

df_previous_day = pd.concat(list(dict_consolidated.values()), ignore_index=True)
df_previous_day['AlarmKey'] = df_previous_day['AlarmKey'].apply(lambda x: x.strip(' '))
df_previous_day['Detail1'].replace(['\\N','0'],np.NaN,inplace=True)
df_previous_day['Node'].replace(['\\N','0'],np.NaN,inplace=True)
#df_previous_day['CellSiteID'].replace(['\\N','0'],np.NaN,inplace=True)
#df_previous_day['CellSiteID'] = df_previous_day['CellSiteID'].fillna(0).astype(np.int64, errors='ignore')
df_previous_day.replace({'CellID':['\\N','0'],'CellSiteID':['\\N','0'], 'Class':['\\N','0'], 'SiteID':['\\N','0']},np.NaN,inplace=True)
df_previous_day[['CellSiteID','Class','SiteID']] = df_previous_day[['CellSiteID','Class','SiteID']].fillna(0)
df_previous_day[['CellSiteID','Class','SiteID']] = df_previous_day[['CellSiteID','Class','SiteID']].astype('str')
df_previous_day[['CellSiteID','Class','SiteID']] = df_previous_day[['CellSiteID','Class','SiteID']].astype('float')
df_previous_day[['CellSiteID','Class','SiteID']] = df_previous_day[['CellSiteID','Class','SiteID']].astype('int')
#Detect_null = list(df_previous_day[~df_previous_day['Technology'].isin(['2G','3G','4G'])]['Technology'])
#if len(Detect_null)!=0:
#    df_previous_day['Technology'].replace(Detect_null,np.NaN,inplace=True)


# In[7]:


Vendor = {'MBNL':'Nokia', 'U2000':'Huawei','Samsung':'Samsung',
         'Nortel':'Nortel','Ericsson':'Ericsson','Darwin':'Nokia','LOUK':'Nokia'}
f_path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\05-Mar'
df_Delete = pd.DataFrame()
df_Insert_Delete = pd.DataFrame()
dict1={}
dict2={}
dict3={}


# In[8]:


get_ipython().run_cell_magic('time', '', "\nProcessing_Files = 'C:\\\\Users\\\\eubcefm\\\\Desktop\\\\MBNL Alarm Processing\\\\Processing Files'\nj=0\n\ndef Bounce(df_Temp):\n    if df_Temp['Detail1'].isnull().all():\n        #if df_Temp['Detail1'].isnull().sum()!=0:\n        #print ('In to Bounce with Detail1 blank')\n        l = list(df_Temp['Severity'])\n        common = [(l[i:i+2]) for i in range(0, len(l),1) if (l[i:i+2].count(0)==1) and (l[i])!=0]\n        return (len([i for i in common if len(i)>1]))\n    else:\n        #print ('In to Bounce with Detail1 filled')\n        Cleared_df = df_Temp[df_Temp['Severity']==0]\n        Cleared_list = list(Cleared_df['Detail1'])\n        Active_df = df_Temp[df_Temp['Severity']!=0]\n        Active_list = list(Active_df['Detail1'])\n        common = [e for e in Cleared_list if e in Active_list and (Active_list.pop(Active_list.index(e)) or True)]\n        return(len(common))\n\ndef Derived_Node(df_dump_new):\n    Index_B = 0\n    #df_dump_new['CellSiteID'].replace(['\\\\N','0',0],np.NaN,inplace=True)\n    #df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].fillna(0).astype(np.int64)\n    df_dump_new['Derived_Node'] = df_dump_new['CellSiteID']\n    Index_B = df_dump_new[df_dump_new['Derived_Node']==0].index\n    df_dump_new.loc[Index_B,'Derived_Node'] = list(df_dump_new.loc[Index_B,'Node'])\n    df_dump_new['Derived_Node'] = [str(i)+'_'+m for i in df_dump_new['Derived_Node']] \n    \n    \nfor m in dict_yesterday:\n    dict_value = dict_yesterday.get(m)\n    df_Delete = pd.DataFrame()\n    df_Insert_Delete = pd.DataFrame()\n    full_path = os.path.join(Processing_Files,dict_value)\n    df_dump_final = pd.read_csv(full_path,usecols = columns)\n    df_dump_new = pd.DataFrame(columns=columns)\n    A=['INSERT','UPDATE','DELETE']\n    df_dump_final.dropna(axis=0,inplace=True,how='all')\n    df_dump_final.dropna(subset=['AlarmId'],axis=0,inplace=True)\n    df_dump_final.dropna(subset=['Severity'],axis=0,inplace=True)\n    df_dump_final.dropna(subset=['from_unixtime(Reported)'],axis=0,inplace=True)\n    Q = [0,1,2,3,4,5,6,'0','1','2','3','4','5','6',1.0,2.0,3.0,4.0,5.0,6.0]\n    df_dump_final[pd.to_numeric(df_dump_final['AlarmId'], errors='coerce').notnull()]\n    df_dump_final = df_dump_final[df_dump_final['HistoryAction'].isin(A)]\n    df_dump_final = df_dump_final[df_dump_final['Severity'].isin(Q)]\n    df_dump_final['Severity'] = pd.to_numeric(df_dump_final['Severity']).astype('int64')\n    df_dump_final['AlarmId'] = pd.to_numeric(df_dump_final['AlarmId']).astype('int64')\n    df_dump_final['Count'] = pd.to_numeric(df_dump_final['Count']).astype('int64')\n    df_dump_final.replace({'CellID':['\\\\N','0'],'CellSiteID':['\\\\N','0'], 'Class':['\\\\N','0'], 'SiteID':['\\\\N','0']},np.NaN,inplace=True)\n    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].fillna(0)\n    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].astype('str')\n    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].astype('float')\n    df_dump_final[['CellSiteID','Class','SiteID']] = df_dump_final[['CellSiteID','Class','SiteID']].astype('int')\n    df_dump_final['from_unixtime(Reported)'] = pd.to_datetime(df_dump_final['from_unixtime(Reported)'])\n    df_dump_final['from_unixtime(StateChange)'] = pd.to_datetime(df_dump_final['from_unixtime(StateChange)'])\n    df_dump_final['AlarmKey'] = df_dump_final['AlarmKey'].apply(lambda x: x.strip(' '))\n    df_dump_final['Summary'] = df_dump_final['Summary'].astype('str')\n    df_Summary = df_dump_final[df_dump_final['Summary'].apply(lambda x: x.startswith('Summary') and len(x)>100)]\n    df_dump_final.drop(index=df_Summary.index, axis=1,inplace=True)\n    Filter = list(df_dump_final['AlarmKey'].unique())\n    Alarm_Key = [str(i.strip(' ')) for i in Filter]\n    Alarm_id_overall = list(df_dump_final['AlarmId'].unique())\n    Temp = df_previous_day[df_previous_day['AlarmId'].isin(Alarm_id_overall)==True]\n    df_consolidated = Temp.append(df_dump_final)\n    df_consolidated['Detail1'].replace(['\\\\N','0'],np.NaN,inplace=True)\n    df_consolidated['Node'].replace(['\\\\N','0'],np.NaN,inplace=True)\n    df_consolidated['AlarmKey'] = df_consolidated['AlarmKey'].apply(lambda x: x.strip(' '))\n    #df_consolidated['CellSiteID'] = df_consolidated['CellSiteID'].fillna(0).astype(np.int64, errors='ignore')\n    #df_consolidated['AlarmId'] = df_consolidated['AlarmId'].astype('str')\n    #df_consolidated = df_previous_day.loc[df_previous_day['AlarmKey'].isin(Alarm_Key)==True].append(df_dump_final)\n    df_consolidated.reset_index(inplace=True)\n    #df_dump_final.reset_index(inplace=True,drop=True)\n    #Alarm_Key = list(df_dump_final['AlarmKey'].unique())\n    #df_consolidated = df_consolidated[df_consolidated['AlarmId'].isin(Filter)]\n    n=5000\n    Index_A = 0\n    #my_list = list(df_consolidated.AlarmId.unique())\n    final = [Alarm_Key[i * n:(i + 1) * n] for i in range((len(Alarm_Key) + n - 1) // n )]\n    \n    Delete=[]\n    Insert_Delete=[]\n    print (df_dump_new.shape)\n    \n    for k in final:\n        df_dump = df_consolidated[df_consolidated['AlarmKey'].isin(k)]\n        print ('Shape of DataFrame',df_dump.shape[0])\n        \n        for i in k:\n            print ('Taken: ',i)\n            df_AlarmKey = pd.DataFrame()\n            #df_AlarmKey = df_dump[df_dump['AlarmKey'].str.contains(i,regex=False)]\n            df_AlarmKey = df_dump[df_dump['AlarmKey']==i]\n            \n            for x in list(df_AlarmKey['AlarmId'].unique()):\n                print (x)\n                #if x in list(df_dump_new['AlarmId']):\n                #   print ('***Duplicate***: ',x)\n                #   break\n                    \n                df_Temp = pd.DataFrame()\n                #df_Temp = df_AlarmKey[df_AlarmKey['AlarmId'].str.contains(x,regex=False)]\n                #df_Temp = df_AlarmKey[df_AlarmKey['AlarmId']==x].sort_values(by=['from_unixtime(Reported)','Count'])\n                #df_Temp = df_AlarmKey[df_AlarmKey['AlarmId']==x]\n                #df_Temp.sort_values(by=['from_unixtime(Reported)','Count'],inplace=True)\n                df_Temp = df_AlarmKey[df_AlarmKey['AlarmId']==x].sort_values(by=['Count'])\n                df_Temp = df_Temp.reset_index(drop=True)\n                Severity_max = max(df_Temp['Severity'].unique(),default=0)\n                Severity_Count = len(df_Temp['Severity'].unique())\n            \n    \n                if df_Temp[(df_Temp['HistoryAction']=='INSERT') & (df_Temp['Severity']!=0)].shape[0]!=0:  #INSERT >0, UPDATE, DELETE\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n                    \n                    if df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)].shape[0]!=0:\n                        Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT>0, FO!=LO'\n                    else:\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT>0, FO=LO'\n    \n                    #if (list(df_Temp['HistoryAction'])[-1]=='DELETE') and (list(df_Temp['Severity'])[-1]==0):\n                    if df_Temp[(df_Temp['HistoryAction']=='DELETE') & (df_Temp['Severity']==0)].shape[0]!=0:\n                        try:\n                            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                            df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                        except:\n                            df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                            \n                    elif list(df_Temp['HistoryAction'])[-1]=='DELETE' and list(df_Temp['Severity'])[-1]!=0:\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    else:\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                \n                    df_dump_new.loc[(df_shape-1),'Bounce_Count'] = Bounce(df_Temp)\n            \n                    \n                elif df_Temp[(df_Temp['HistoryAction']=='INSERT') & (df_Temp['Severity']==0)].shape[0]!=0:  ##INSERT =0, UPDATE, DELETE\n                    if list(df_Temp['HistoryAction']).count('UPDATE')!=0:\n                        \n                        if df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)].shape[0]!=0:\n                            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n                            df_dump_new = df_dump_new.append(Filter.iloc[0,:],ignore_index=True)\n                            df_shape = df_dump_new.shape[0]\n                            df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                            df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                        else:\n                            df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                            df_shape = df_dump_new.shape[0]\n                            df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                            df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN    \n                \n                        if (list(df_Temp['HistoryAction'])[-1]=='DELETE') and (list(df_Temp['Severity'])[-1]==0):\n                            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                            df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                        elif list(df_Temp['HistoryAction'])[-1]=='DELETE' and list(df_Temp['Severity'])[-1]!=0:\n                            df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                        else:\n                            df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    \n                        df_dump_new.loc[(df_shape-1),'Bounce_Count'] = Bounce(df_Temp)\n                \n                    else:\n                        Filter = df_Temp[(df_Temp['HistoryAction']=='INSERT') & (df_Temp['Severity']==0)]\n                        df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                        df_shape = df_dump_new.shape[0]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-DELETE, DISTR, FO=LO=CL'\n                        df_dump_new.loc[(df_shape-1),'Bounce_Count'] = Bounce(df_Temp)\n                \n\n                elif list(df_Temp['HistoryAction']).count('INSERT')==0 and Severity_Count>1 and list(df_Temp['Severity']).count(0)!=0:  # No INSERT with 3-0\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n                    df_dump_new = df_dump_new.append(Filter.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==x) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Severity'] = list(Filter_Insert['Severity'])[0]\n                    except:\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    \n                    if df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)].shape[0]!=0:\n                        Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    else:\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n        \n                    if df_Temp[(df_Temp['HistoryAction']=='DELETE') & (df_Temp['Severity']==0)].shape[0]==1:\n                        Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE 3-0'\n                    else:\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE 3-0'\n                \n                    df_dump_new.loc[(df_shape-1),'Bounce_Count'] = Bounce(df_Temp)\n        \n            \n                elif list(df_Temp['HistoryAction']).count('INSERT')==0 and Severity_Count==1 and list(df_Temp['Severity']).count(0)!=0:  # No INSERT with 0-0\n                    #Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==x) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Severity'] = list(Filter_Insert['Severity'])[0]\n                    except:\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n        \n                    if df_Temp[(df_Temp['HistoryAction']=='DELETE') & (df_Temp['Severity']==0)].shape[0]==1:\n                        try:\n                            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                            df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                            df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE 0-0'\n                        except:\n                            pass\n                    else:\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE 0-0'\n                \n                    df_dump_new.loc[(df_shape-1),'Bounce_Count'] = Bounce(df_Temp)\n       \n    \n                elif list(df_Temp['HistoryAction']).count('INSERT')==0 and Severity_Count==1 and list(df_Temp['Severity']).count(0)==0:  # No INSERT with 3-3\n                    #Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==x) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Severity'] = list(Filter_Insert['Severity'])[0]\n                    except:\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    try:\n                        Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    except:\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                \n                    if df_Temp[(df_Temp['HistoryAction']=='DELETE') & (df_Temp['Severity']==0)].shape[0]==1:\n                        Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE 0-0'\n                    else:\n                        df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                        df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE 3-3'\n                \n                    df_dump_new.loc[(df_shape-1),'Bounce_Count'] = Bounce(df_Temp)\n\n\n                else:\n                    Delete.append(i)\n                \n                j+=1\n    \n    \n    name = 'df_'+m+'_Alarms'\n    df_dump_new['Vendor'] = Vendor.get(m)\n    d1={'AlarmId':Delete,'Vendor':m}\n    d2={'AlarmId':Insert_Delete,'Vendor':m}\n    df_Delete = pd.DataFrame(data=d1)\n    df_Insert_Delete = pd.DataFrame(data=d2)\n    \n    if m in ['MBNL','Darwin']:\n        df_dump_new['Domain'] = np.where(df_dump_new['AlarmKey'].str.contains('BSC|BCF|BTS|TRX|RNC|WBTS|WCEL|OMS|MRBTS|LNBTS|eNodeB'), 'RAN', np.where(df_dump_new['AlarmKey'].str.contains('NETACT|netact|Q1A'), 'OSS',np.where(df_dump_new['AlarmKey'].str.contains('MSS|MGW|MSS|CDS'), 'CORE','')))\n        df_dump_new['Sub_Domain'] = np.where(df_dump_new['AlarmKey'].str.contains('BSC|BCF|TRX|MSS|MGW|MSS|CDS'), '2G', np.where(df_dump_new['AlarmKey'].str.contains('RNC|WBTS|WCEL|OMS'), '3G',np.where(df_dump_new['AlarmKey'].str.contains('MRBTS|LNBTS|eNodeB'), '4G','')))\n        df_dump_new['Derived_Summary'] = df_dump_new['Summary']\n        Derived_Node(df_dump_new)\n        #df_dump_new['Derived_Node'] = df_dump_new['CellSiteID']\n        #Index_A = pd.isna(df_dump_new['Derived_Node'])\n        #df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Node'])\n        #df_dump_new['Derived_Node'] = [i+'_'+m for i in df_dump_new['Derived_Node']]\n        #df_dump_new['CellSiteID'].replace(['\\\\N','0'],np.NaN,inplace=True)\n        #df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].fillna(0).astype(np.int64)\n        #df_dump_new['Derived_Node'] = df_dump_new['CellSiteID']\n        #Index_A = df_dump_new[df_dump_new['Derived_Node']==0].index\n        #df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Node'])\n        #df_dump_new['Derived_Node'] = [str(i)+'_'+m for i in df_dump_new['Derived_Node']]\n        #df_dump_new['Derived_Summary'] = df_dump_new['Summary']\n        Index_A = df_dump_new[df_dump_new['Summary'].str.contains('CELL OPERATION DEGRADED|BASE STATION NOTIFICATION|BASE STATION CONNECTIVITY LOST|BASE STATION CONNECTIVITY DEGRADED|BASE STATION OPERATION DEGRADED|CELL FAULTY|CELL NOTIFICATION|BASE STATION CONNECTIVITY LOST|BASE STATION FAULTY|TRX INITIALISATION|BASE STATION NOTIFICATION|HSUPA CONFIGURATION FAILED|BASE STATION OPERATION DEGRADED|WCDMA CELL OUT OF USE|BCF INITIALIZATION|WCDMA CELL OUT OF USE|RNW O and M SCENARIO FAILURE|BASE STATION LICENCE NOTIFICATION|RNW DATABASE OPERATION FAILURE')].index\n        df_dump_new.loc[Index_A,'Derived_Summary'] = df_dump_new.loc[Index_A,'Summary'] + ' - ' + df_dump_new.loc[Index_A,'Detail1']\n        df_dump_new['Derived_Summary'].fillna(df_dump_new['Summary'], inplace=True)\n\n\n    elif m=='LOUK':\n        df_dump_new['Domain'] = np.where(df_dump_new['AlarmKey'].str.contains('BSC|BCF|BTS|TRX|RNC|WBTS|WCEL|OMS|MRBTS|LNBTS|eNodeB'), 'RAN', np.where(df_dump_new['AlarmKey'].str.contains('NETACT|netact|Q1A'), 'OSS',np.where(df_dump_new['AlarmKey'].str.contains('MSS|MGW|MSS|CDS'), 'CORE','')))\n        df_dump_new['Sub_Domain'] = np.where(df_dump_new['AlarmKey'].str.contains('BSC|BCF|TRX|MSS|MGW|MSS|CDS'), '2G', np.where(df_dump_new['AlarmKey'].str.contains('RNC|WBTS|WCEL|OMS'), '3G',np.where(df_dump_new['AlarmKey'].str.contains('MRBTS|LNBTS|eNodeB'), '4G','')))    \n        df_dump_new['Detail3'].replace(['\\\\N','0'],np.NaN,inplace=True)\n        df_dump_new['Detail3'] = df_dump_new['Detail3'].fillna(0)\n        df_dump_new['Derived_Node'] = df_dump_new['Detail3']\n        Index_A = df_dump_new[df_dump_new['Derived_Node']==0].index\n        df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Node'])\n        df_dump_new['Derived_Node'] = [str(i)+'_'+m for i in df_dump_new['Derived_Node']]\n        #Index_A = pd.isna(df_dump_new['Derived_Node'])\n        #df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Node'])\n        #df_dump_new['Derived_Node'] = [i+'_'+m for i in df_dump_new['Derived_Node']]\n        df_dump_new['Derived_Summary'] = df_dump_new['Summary']\n        Index_A = df_dump_new[df_dump_new['Summary'].str.contains('CELL OPERATION DEGRADED|BASE STATION NOTIFICATION|BASE STATION CONNECTIVITY LOST|BASE STATION CONNECTIVITY DEGRADED|BASE STATION OPERATION DEGRADED|CELL FAULTY|CELL NOTIFICATION|BASE STATION CONNECTIVITY LOST|BASE STATION FAULTY|TRX INITIALISATION|BASE STATION NOTIFICATION|HSUPA CONFIGURATION FAILED|BASE STATION OPERATION DEGRADED|WCDMA CELL OUT OF USE|BCF INITIALIZATION|WCDMA CELL OUT OF USE|RNW O and M SCENARIO FAILURE|BASE STATION LICENCE NOTIFICATION|RNW DATABASE OPERATION FAILURE')].index\n        df_dump_new.loc[Index_A,'Derived_Summary'] = df_dump_new.loc[Index_A,'Summary'] + ' - ' + df_dump_new.loc[Index_A,'Detail1']\n        df_dump_new['Derived_Summary'].fillna(df_dump_new['Summary'], inplace=True)\n\n        \n    elif m=='Samsung':\n        df_dump_new['Domain'] = 'RAN'\n        df_dump_new['Sub_Domain'] = '4G'\n        df_dump_new['Derived_Summary'] = df_dump_new['Summary']\n        #df_dump_new['CellSiteID'].replace(['\\\\N','0'],np.NaN,inplace=True)\n        #df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].fillna(0).astype(np.int64)\n        df_dump_new['Derived_Node'] = df_dump_new['CellSiteID']\n        Index_A = df_dump_new[df_dump_new['Derived_Node']==0].index\n        df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Location'])\n        df_dump_new['Derived_Node'] = [str(i)+'_'+m for i in df_dump_new['Derived_Node']]\n        #Index_A = pd.isna(df_dump_new['Derived_Node'])\n        #df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Location'])\n        #df_dump_new['Derived_Node'] = [i+'_'+m for i in df_dump_new['Derived_Node']]\n\n    elif m=='U2000':\n        df_dump_new['Domain'] = np.where(df_dump_new['Detail1'].str.contains('BSC|BTS|GSM|LTE|RRU'), 'RAN', np.where(df_dump_new['Detail1'].str.contains('OSS'), 'OSS',np.where(df_dump_new['Summary'].str.contains('HeartBeat received'), 'OSS','')))\n        df_dump_new['Sub_Domain'] = np.where(df_dump_new['Detail1'].str.contains('BSC|GSM|GBTS'), '2G', np.where(df_dump_new['Detail1'].str.contains('LTE|MICRO BTS3900|RRU'), '4G',np.where(df_dump_new['AlarmKey'].str.contains('eNodeB'), '4G','')))\n        df_dump_new['Derived_Summary'] = df_dump_new['Summary']\n        Derived_Node(df_dump_new)\n        #df_dump_new['CellSiteID'].replace(['\\\\N','0'],np.NaN,inplace=True)\n        #df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].fillna(0).astype(np.int64)\n        #df_dump_new['Derived_Node'] = df_dump_new['CellSiteID']\n        #Index_A = df_dump_new[df_dump_new['Derived_Node']==0].index\n        #df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Node'])\n        #df_dump_new['Derived_Node'] = [str(i)+'_'+m for i in df_dump_new['Derived_Node']]\n        #Index_A = pd.isna(df_dump_new['Derived_Node'])\n        #df_dump_new.loc[Index_A,'Derived_Node'] = list(df_dump_new.loc[Index_A,'Node'])\n        #df_dump_new['Derived_Node'] = [i+'_'+m for i in df_dump_new['Derived_Node']]\n\n    elif m=='Nortel':\n        df_dump_new['Domain'] = np.where(df_dump_new['AlarmClass'].str.contains('SWITCH|SITE|PCUSN'), 'RAN', np.where(df_dump_new['AlarmClass'].str.contains('OMCR'), 'OSS',''))\n        i = df_dump_new[df_dump_new['Domain'].isin(['RAN'])].index\n        df_dump_new.loc[i,'Sub_Domain'] = '2G'\n        df_dump_new['Derived_Summary'] = df_dump_new['Summary']\n        Derived_Node(df_dump_new)\n\n\n    dict1[name] = df_dump_new.copy(deep=True)\n    dict2[name] = df_Delete.copy(deep=True)\n    dict3[name] = df_Insert_Delete.copy(deep=True)\n    \n    op = os.path.join(Output_Path,'Output_'+m+'_'+Y_date+'.xlsx')\n    writer = pd.ExcelWriter(op)\n    df_dump_new = df_dump_new[output_col]\n    df_dump_new.set_index('AlarmId',inplace=True)\n    df_dump_new.to_excel(writer,sheet_name='Alarm')\n    df_Delete.to_excel(writer,sheet_name='Delete')\n    df_Insert_Delete.to_excel(writer,sheet_name='Insert-Delete')\n    writer.save()\n\n    print ('Total AlarmId handled: ',j)\n    print ('Done with ',m)\n\ndf_processed = pd.concat(list(dict1.values()), ignore_index=True)\ndf_Delete_colsolidated = pd.concat(list(dict2.values()), ignore_index=True)\ndf_Insert_Delete_colsolidated = pd.concat(list(dict3.values()), ignore_index=True)\n\nop1 = os.path.join(Output_Path,'Output_Consolidated'+'_'+Y_date+'.xlsx')\nwriter = pd.ExcelWriter(op1)\ndf_processed = df_processed[output_col]\ndf_processed.set_index('AlarmId',inplace=True)\ndf_processed.to_excel(writer,sheet_name='Alarm')\ndf_Delete_colsolidated.to_excel(writer,sheet_name='Delete')\ndf_Insert_Delete_colsolidated.to_excel(writer,sheet_name='Insert-Delete')\nwriter.save()\nprint ('*********Done with Script*********')")


# In[ ]:


df_Temp


# In[ ]:


A[['AlarmId','AlarmKey','Count','Severity','from_unixtime(Reported)']]


# In[ ]:


A.AlarmKey.unique()


# #####################################################################################################################

# In[ ]:


get_ipython().run_cell_magic('time', '', "for m in dict_yesterday:\n    \n    dict_value = dict_yesterday.get(m)\n    full_path = os.path.join(Processing_Files,dict_value)\n    df_dump_final = pd.read_csv(full_path,usecols = columns)\n    df_dump_new = pd.DataFrame(columns=columns)\n    A=['INSERT','UPDATE','DELETE']\n    df_dump_final.dropna(axis=0,inplace=True,how='all')\n    df_dump_final.dropna(subset=['AlarmId'],axis=0,inplace=True)\n    df_dump_final.dropna(subset=['Severity'],axis=0,inplace=True)\n    Q = [0,1,2,3,4,5,6,'0','1','2','3','4','5','6',1.0,2.0,3.0,4.0,5.0,6.0]\n    df_dump_final[pd.to_numeric(df_dump_final['AlarmId'], errors='coerce').notnull()]\n    df_dump_final = df_dump_final[df_dump_final['HistoryAction'].isin(A)]\n    df_dump_final = df_dump_final[df_dump_final['Severity'].isin(Q)]\n    df_dump_final['Severity'] = pd.to_numeric(df_dump_final['Severity']).astype('int64')\n    df_dump_final['AlarmId'] = pd.to_numeric(df_dump_final['AlarmId']).astype('int64')\n    df_dump_final['Count'] = pd.to_numeric(df_dump_final['Count']).astype('int64')\n    df_dump_final.reset_index(inplace=True,drop=True)\n    Filter = list(df_dump_final['AlarmId'].unique())\n    df_consolidated = df_previous_day[df_previous_day['AlarmId'].isin(Filter)].append(df_dump_final)\n    df_consolidated.reset_index(inplace=True)\n    \n    df_consolidated = df_consolidated[df_consolidated['AlarmId'].isin(Filter)]\n    n=5000\n    my_list = list(df_consolidated.AlarmId.unique())\n    final = [my_list[i * n:(i + 1) * n] for i in range((len(my_list) + n - 1) // n )]\n    \n    Delete=[]\n    Insert_Delete=[]\n\n    for k in final:\n        df_dump = df_consolidated[df_consolidated['AlarmId'].isin(k)]\n        print ('Shape of DataFrame',df_dump.shape[0])\n        for i in list(df_dump['AlarmId'].unique()):\n                \n            print ('Taken: ',i)\n            df_Temp = df_dump[df_dump['AlarmId']==i].sort_values(by=['from_unixtime(Reported)'])\n            #df_Temp['Rank'] = df_Temp['HistoryAction'].map(custom_sort)\n            #df_Temp = df_Temp.sort_values(by=['Rank']).reset_index(drop=True)\n            df_Temp = df_dump[df_dump['AlarmId']==i].reset_index(drop=True)\n            Severity_max = max(df_Temp['Severity'].unique())\n            Severity_Count = len(df_Temp['Severity'].unique())\n    \n    \n            if (df_Temp.shape[0])==1 and df_Temp['HistoryAction'][0]=='INSERT' and df_Temp['Severity'][0]!=0:\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                df_dump_new.loc[(df_shape-1),'First_Occurance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n                df_dump_new.loc[(df_shape-1),'Last_Occurance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n                df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = 1\n                df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT, SHAPE=1, SEVERITY!=0, BC=1'\n                \n            elif (df_Temp.shape[0])==1 and df_Temp['HistoryAction'][0]=='INSERT' and df_Temp['Severity'][0]==0:\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                df_dump_new.loc[(df_shape-1),'Clerance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n                df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = 0\n                df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT, SHAPE=1, SEVERITY==0, BC=0'\n\n            elif ((df_Temp.shape[0])>2 and df_Temp['HistoryAction'][0]=='INSERT' and list(df_Temp['HistoryAction'])[-1]=='DELETE'):\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                Filter = df_Temp[(df_Temp['Severity']==Severity_max)]\n                df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                try:\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                except:\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                try:\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                except:\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                if list(df_Temp['Severity']).count(0)!=0:\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = list(Filter['Count'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-DELETE(3-0), SHAPE>2, BC//2'\n                else:\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count']))[-1]\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-DELETE(3-3), SHAPE>2, BC'\n                    \n        \n            elif ((df_Temp.shape[0])>2 and df_Temp['HistoryAction'][0]=='INSERT' and list(df_Temp['HistoryAction'])[-1]=='UPDATE'):\n                print ('In to INSERT-UPDATE')\n                print ('Severity_max & Type: ',Severity_max,type(Severity_max))\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                Filter = df_Temp[(df_Temp['Severity']==Severity_max)]\n                df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                try:\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                except:\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n            \n                df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                if list(df_Temp['Severity']).count(0)!=0:\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = list(Filter['Count'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-UPDATE(3-0), SHAPE>2'\n                else:\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count']))[-1]\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-UPDATE(3-3), SHAPE>2, BC'\n                \n    \n            elif ((df_Temp.shape[0])==2 and df_Temp['HistoryAction'][0]=='INSERT' and list(df_Temp['HistoryAction'])[-1]=='UPDATE'):\n                if Severity_Count>1:                       #3-0\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = 1\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-UPDATE, SHAPE=2, 3-0, BC=1'\n                    \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)==0:             #3-3\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = 0\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-UPDATE, SHAPE=2, 3-3, BC=0'\n                    \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)!=0:             #3-0\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = 1\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-UPDATE, SHAPE=2, 3-0, BC=1'\n                    \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)!=0:             #0-0\n                    df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                    df_shape = df_dump_new.shape[0]\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = 0\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT-UPDATE, SHAPE=2, 0-0, BC=0'\n                    \n                \n            elif ((df_Temp.shape[0])>2 and df_Temp['HistoryAction'][0]=='UPDATE' and list(df_Temp['HistoryAction'])[-1]=='UPDATE'):\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                if Severity_Count>1 and list(df_Temp['Severity']).count(0)!=0:  #3-0\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    #Bouncing_count = (list(Filter['Count'])[-1]) - (list(Filter['Count'])[0])\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE>2, 3-0, BC//2'\n                    \n                    \n                elif Severity_Count>1 and list(df_Temp['Severity']).count(0)==0:  #3-4\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    #Bouncing_count = len((list(Filter['Reported'].unique())))\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE>2, 3-4, BC'\n                    \n                    \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)==0: #3-3\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    #Bouncing_count = len((list(Filter['Reported'].unique())))\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE>2, 3-3, BC'\n                    \n                    \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)!=0: #0-0\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE>2, 0-0, BC'\n                    \n                    \n            elif ((df_Temp.shape[0])==2 and df_Temp['HistoryAction'][0]=='UPDATE' and list(df_Temp['HistoryAction'])[-1]=='UPDATE'):\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                if Severity_Count>1 and list(df_Temp['Severity']).count(0)!=0: #3-0\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE=2, 3-0, BC//2'\n                    \n                        \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)==0: #3-3\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    #Bouncing_count = (list(Filter['Count'])[-1]) - (list(Filter['Count'])[0])\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE=2, 3-3, BC'\n                    \n                        \n                elif Severity_Count==1 and list(df_Temp['Severity']).count(0)!=0: #0-0\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE, SHAPE=2, 0-0, BC'\n                    \n                    \n            elif ((df_Temp.shape[0])>2 and df_Temp['HistoryAction'][0]=='UPDATE' and list(df_Temp['HistoryAction'])[-1]=='DELETE'):\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                if Severity_Count>1:\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    Filter = df_Temp[(df_Temp['Severity']==0)]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    #Filter = list(df_Temp['Severity'])\n                    #Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE, SHAPE>2, 3-0, BC//2'\n                    \n                        \n                elif Severity_Count==1 and Severity_max==0:  #0-0\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']==0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE, SHAPE>2, 0-0, BC'\n                    \n                elif Severity_Count==1 and Severity_max!=0:  #3-3\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE(IMPROPER DELETE), SHAPE>2, 3-3, BC'\n                    \n            \n            elif ((df_Temp.shape[0])==2 and df_Temp['HistoryAction'][0]=='UPDATE' and list(df_Temp['HistoryAction'])[-1]=='DELETE'):\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                if (list(df_Temp.Severity)[-1])==0:\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']==0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE, SHAPE=2, BC//2'\n                else:\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE(IMPROPER DELETE), SHAPE=2, BC//2'\n                    \n    \n            elif ((df_Temp.shape[0])==1 and (df_Temp['HistoryAction'][0]=='UPDATE')):\n                df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n                df_shape = df_dump_new.shape[0]\n                if (df_Temp.Severity[0])!=0:\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    Filter = df_Temp[(df_Temp['Severity']!=0)]\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[0]\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE, SHAPE=1, 3, BC//2'\n                    \n                else:\n                    try:\n                        Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==i) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n                        df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                        df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n                    except:\n                        pass\n                    df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n                    df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n                    Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE')]\n                    Bouncing_count = (list(Filter['Count'])[-1])\n                    df_dump_new.loc[(df_shape-1),'Bouncing_Count'] = Bouncing_count//2\n                    df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE, SHAPE=1, 0, BC//2'\n                \n        \n            elif ((df_Temp.shape[0])==2 and df_Temp['HistoryAction'][0]=='INSERT' and list(df_Temp['HistoryAction'])[-1]=='DELETE'):\n                Insert_Delete.append(i)\n        \n        \n            else:\n                Delete.append(i)\n    \n    name = 'df_'+m+'_Alarms'\n    dict1[name] = df_dump_new.copy(deep=True)\n    d1={'AlarmId':Delete,'Vendor':m}\n    d2={'AlarmId':Insert_Delete,'Vendor':m}\n    \n    if df_Delete.shape[0]==0:\n        df_Delete = pd.DataFrame(data=d1)\n    else:\n        df_Temp_Delete=pd.DataFrame(data=d1)\n        df_Delete = df_Delete.append(df_Temp_Delete,ignore_index=True)\n    \n    if df_Insert_Delete.shape[0]==0:\n        df_Insert_Delete = pd.DataFrame(data=d2)\n    else:\n        df_Temp_Insert_Delete=pd.DataFrame(data=d2)\n        df_Insert_Delete = df_Insert_Delete.append(df_Temp_Insert_Delete,ignore_index=True)\n    \n    print ('Done with ',m)\n\ndf_processed = pd.concat(list(dict1.values()), ignore_index=True)\n    \n\nwriter = pd.ExcelWriter(r'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\Output_U2000_1503.xlsx')\ndf_processed.to_excel(writer,sheet_name='Alarm')\ndf_Delete.to_excel(writer,sheet_name='Delete')\ndf_Insert_Delete.to_excel(writer,sheet_name='Insert-Delete')\nwriter.save()\nprint ('*********Done with Script*********')")


# ### Update using Alarm Key

# In[ ]:


get_ipython().run_cell_magic('time', '', "import pandas as pd\nimport numpy as np\nimport os\n\nProcessing_Files = 'C:\\\\Users\\\\eubcefm\\\\Desktop\\\\MBNL Alarm Processing\\\\Processing Files'\ndict_value = 'Test.csv'\ncolumns = ['AlarmId','HistoryAction','Date','AlarmKey','Node','IPAddress','Summary','SubMethod','TicketId','Ack','Count','Severity','Detail1','Detail2','Detail3','Detail4','Detail5','Detail6','Detail7','Detail8','Detail9','Detail10','Detail11','Detail12','B2BSite','CellID','CellSiteID','Class','NEMName','Region','SiteID','SubClass','Technology','from_unixtime(Reported)','from_unixtime(StateChange)']\nfull_path = os.path.join(Processing_Files,dict_value)\ndf_dump_final = pd.read_csv(full_path,usecols = columns)\ndf_dump_new = pd.DataFrame(columns=columns)\nA=['INSERT','UPDATE','DELETE']\ndf_dump_final.dropna(axis=0,inplace=True,how='all')\ndf_dump_final.dropna(subset=['AlarmId'],axis=0,inplace=True)\ndf_dump_final.dropna(subset=['Severity'],axis=0,inplace=True)\ndf_dump_final.dropna(subset=['from_unixtime(Reported)'],axis=0,inplace=True)\nQ = [0,1,2,3,4,5,6,'0','1','2','3','4','5','6',1.0,2.0,3.0,4.0,5.0,6.0]\ndf_dump_final[pd.to_numeric(df_dump_final['AlarmId'], errors='coerce').notnull()]\ndf_dump_final = df_dump_final[df_dump_final['HistoryAction'].isin(A)]\ndf_dump_final = df_dump_final[df_dump_final['Severity'].isin(Q)]\ndf_dump_final['Severity'] = pd.to_numeric(df_dump_final['Severity']).astype('int64')\ndf_dump_final['AlarmId'] = pd.to_numeric(df_dump_final['AlarmId']).astype('int64')\ndf_dump_final['Count'] = pd.to_numeric(df_dump_final['Count']).astype('int64')\ndf_dump_final['from_unixtime(Reported)'] = pd.to_datetime(df_dump_final['from_unixtime(Reported)'])\ndf_dump_final['from_unixtime(StateChange)'] = pd.to_datetime(df_dump_final['from_unixtime(StateChange)'])\ndf_dump_final.reset_index(inplace=True,drop=True)\nAlarm_Key = list(df_dump_final['AlarmKey'].unique())\nDelete=[]\nInsert_Delete=[]\n\n#for i in ['2518RNC-217/FUUT-OMU-0:910002518_']:\nfor i in Alarm_Key:\n#i = '2518RNC-217/FUUT-OMU-0:910002518_'\n    print ('Taken: ',i)\n    df_Temp = df_dump_final[df_dump_final['AlarmKey']==i].sort_values(by=['from_unixtime(Reported)'])\n    df_Temp = df_Temp.reset_index(drop=True)\n    #Severity_max = max(df_Temp['Severity'].unique())\n    #Severity_Count = len(df_Temp['Severity'].unique())\n    \n    \n    if df_Temp[(df_Temp['HistoryAction']=='INSERT') & (df_Temp['Severity']!=0)].shape[0]!=0:\n        Filter = df_Temp[(df_Temp['HistoryAction']=='INSERT') & (df_Temp['Severity']!=0)]\n        print ('List of AlarmId',list(Filter['AlarmId']))\n        Alarm_id = list(Filter['AlarmId'])[-1]\n        df_Temp = df_Temp[(df_Temp['AlarmId']==Alarm_id)].reset_index(drop=True)\n        df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n        df_shape = df_dump_new.shape[0]\n        df_dump_new.loc[(df_shape-1),'First_Occurance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n        try:\n            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n            df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n            df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT, FO!=LO'\n        except:\n            df_dump_new.loc[(df_shape-1),'Last_Occurance'] = df_Temp.loc[0,'from_unixtime(Reported)']\n            df_dump_new.loc[(df_shape-1),'Observation'] = 'INSERT, FO=LO'\n    \n        if (list(df_Temp['HistoryAction'])[-1]=='DELETE') and (list(df_Temp['Severity'])[-1]==0):\n            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n            df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n        elif list(df_Temp['HistoryAction'])[-1]=='DELETE' and list(df_Temp['Severity'])[-1]!=0:\n            df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n        else:\n            df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n\n    elif list(df_Temp['HistoryAction']).count('INSERT')==0:  # No INSERT in History Action\n        Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n        Alarm_id = list(Filter['AlarmId'])[-1]\n        print ('List of AlarmId',list(Filter['AlarmId']))\n        df_Temp = df_Temp[(df_Temp['AlarmId']==Alarm_id)].reset_index(drop=True)\n        df_dump_new = df_dump_new.append(df_Temp.iloc[0,:],ignore_index=True)\n        df_shape = df_dump_new.shape[0]\n        try:\n            Filter_Insert = df_consolidated_insert[(df_consolidated_insert['AlarmId']==Alarm_id) & (df_consolidated_insert['HistoryAction']=='INSERT')]\n            df_dump_new.loc[(df_shape-1),'First_Occurance'] = list(Filter_Insert['from_unixtime(Reported)'])[0]\n        except:\n            df_dump_new.loc[(df_shape-1),'First_Occurance'] = np.NaN\n        try:\n            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']!=0)]\n            df_dump_new.loc[(df_shape-1),'Last_Occurance'] = list(Filter['from_unixtime(Reported)'])[-1]\n        except:\n            df_dump_new.loc[(df_shape-1),'Last_Occurance'] = np.NaN\n        \n        if df_Temp[(df_Temp['HistoryAction']=='DELETE') & (df_Temp['Severity']==0)].shape[0]==1:\n            Filter = df_Temp[(df_Temp['HistoryAction']=='UPDATE') & (df_Temp['Severity']==0)]\n            df_dump_new.loc[(df_shape-1),'Clerance'] = list(Filter['from_unixtime(Reported)'])[-1]\n            df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-DELETE(0)'\n        else:\n            df_dump_new.loc[(df_shape-1),'Clerance'] = np.NaN\n            df_dump_new.loc[(df_shape-1),'Observation'] = 'UPDATE-UPDATE'")


# In[ ]:


import pandas as pd
df_processed = pd.DataFrame(data={'A':[1,2,3,4,5],'B':[6,7,8,9,10]})
print (df_processed)


# In[ ]:


import os
Output_Path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\'
m='MBNL'
op = os.path.join(Output_Path,m+'.xlsx')
writer = pd.ExcelWriter(op)
df_processed.to_excel(writer,sheet_name='Alarm')
writer.save()
print ('*********Done with Script*********')


# In[ ]:


op


# ### Updating Consolidated INSERT Records

# In[ ]:


import pandas as pd
import os
from datetime import datetime, timedelta
Y_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
T_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
insert_path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\INSERT'
insert_file = 'INSERT_'+Y_date+'.xlsx'
full_path = os.path.join(insert_path,insert_file)
#df_consolidated_insert = pd.read_excel(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\INSERT\Insert_Consolidated.xlsx',index_col=0)
df_consolidated_insert = pd.read_excel(full_path,index_col=0)
df_processed = pd.read_excel(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\Output.xlsx',index_col=0)
Remove = df_processed[df_processed['Clerance'].notnull()]
To_be_removed = list(Remove.index)
Retain = df_processed[df_processed['First_Occurance'].notnull() & df_processed['Clerance'].isnull()]
df_consolidated_insert.drop(index=To_be_removed,axis=0,errors='ignore',inplace=True)
Retain.drop(['Vendor', 'First_Occurance', 'Last_Occurance', 'Clerance', 'Bouncing_Count','Observation'],axis=1,inplace=True)
df_consolidated_insert = pd.concat([df_consolidated_insert,Retain])
df_consolidated_insert.reset_index(inplace=True)
df_consolidated_insert.drop_duplicates(subset='AlarmId',keep='first',inplace=True)
df_consolidated_insert.reset_index()
#df_consolidated_insert.set_index('AlarmId',inplace=True)


# In[ ]:


output_file = 'INSERT_'+T_date+'.xlsx'
full_path = os.path.join(insert_path,output_file)
writer = pd.ExcelWriter(full_path)
df_consolidated_insert.to_excel(writer)
writer.save()


# In[ ]:


datetime.strftime(datetime.now(), '%Y-%m-%d')


# In[ ]:





# ### Consildating Dump

# In[ ]:


#df_consolidated[df_consolidated['AlarmId']==8154702719]
Bouncing_count


# In[ ]:


writer = pd.ExcelWriter(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\df_consolidated.xlsx')
df_consolidated.to_excel(writer,sheet_name='Alarm')
#df_Delete.to_excel(writer,sheet_name='Delete')
#df_Insert_Delete.to_excel(writer,sheet_name='Insert-Delete')
writer.save()


# In[ ]:


#df_consolidated['df_MBNL_consolidated'].Vendor.unique()
writer = pd.ExcelWriter(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\Output_Combined.xlsx')
df_consolidated['df_MBNL_consolidated'].to_excel(writer)
writer.save()


# ### Consolidating INSERT Records

# In[ ]:


import pandas as pd
import os
from datetime import datetime, timedelta
Y_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
list_files = os.listdir(r"C:\Users\eubcefm\Desktop\MBNL Alarm Processing\Processing Files")

csv_files = [i for i in list_files if i.endswith('.csv')]
columns = ['AlarmId','HistoryAction','Date','AlarmKey','Node','IPAddress','Summary','SubMethod','TicketId','Ack','Count','Severity','Detail1','Detail2','Detail3','Detail4','Detail5','Detail6','Detail7','Detail8','Detail9','Detail10','Detail11','Detail12','B2BSite','CellID','CellSiteID','Class','NEMName','Region','SiteID','SubClass','Technology','from_unixtime(Reported)','from_unixtime(StateChange)']
Remove = []

insert_path = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\INSERT'
Processing_Files = 'C:\\Users\\eubcefm\\Desktop\\MBNL Alarm Processing\\Processing Files'
df_dump_insert = pd.DataFrame(columns=columns)
df_input_exisiting = pd.read_excel(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\INSERT\Insert_Consolidated.xlsx')


for j in csv_files:
    print ('Taken: ',j)
    full_path = os.path.join(Processing_Files,j)
    df_input_insert = pd.read_csv(full_path,usecols = columns)
    A=['INSERT','DELETE']
    df_input_insert.dropna(axis=0,inplace=True,how='all')
    df_input_insert.dropna(subset=['AlarmId'],axis=0,inplace=True)
    df_input_insert.dropna(subset=['Severity'],axis=0,inplace=True)
    Q = [0,1,2,3,4,5,6,'0','1','2','3','4','5','6',1.0,2.0,3.0,4.0,5.0,6.0]
    df_input_insert[pd.to_numeric(df_input_insert['AlarmId'], errors='coerce').notnull()]
    df_input_insert = df_input_insert[df_input_insert['HistoryAction'].isin(A)]
    df_input_insert = df_input_insert[df_input_insert['Severity'].isin(Q)]
    df_input_insert['Severity'] = pd.to_numeric(df_input_insert['Severity']).astype('int64')
    df_input_insert['AlarmId'] = pd.to_numeric(df_input_insert['AlarmId']).astype('int64')
    df_input_insert['Count'] = pd.to_numeric(df_input_insert['Count']).astype('int64')
    df_input_insert['from_unixtime(Reported)'] = pd.to_datetime(df_input_insert['from_unixtime(Reported)'])
    df_input_insert['from_unixtime(StateChange)'] = pd.to_datetime(df_input_insert['from_unixtime(StateChange)'])
    df_input_insert.drop_duplicates(subset=['AlarmId'], keep=False, inplace=True)
    To_be_removed = df_input_insert[df_input_insert['HistoryAction']=='DELETE']
    Remove.extend(list(To_be_removed['AlarmId']))
    df_input_insert = df_input_insert[df_input_insert['HistoryAction']!='DELETE']
    df_input_insert = df_input_insert[df_input_insert['Severity']!=0]
    df_input_insert.reset_index(inplace=True,drop=True)
    df_dump_insert = df_dump_insert.append(df_input_insert[columns],ignore_index=True)
    print ('Delete to be removed: ',len(list(To_be_removed['AlarmId'])))
    print ('Newly added INERT: ',len(list(df_input_insert['AlarmId'])))
    

df_dump_insert = df_input_exisiting.append(df_dump_insert[columns],ignore_index=True)
df_dump_insert = df_dump_insert[~df_dump_insert['AlarmId'].isin(Remove)]

file_name = 'Insert_'+Y_date+'.xlsx'
path = os.path.join(insert_path,file_name)
writer = pd.ExcelWriter(path)
df_dump_insert.to_excel(writer)
writer.save()
print ('*********Done with Script*********')


# In[ ]:


import numpy as np
import pandas as pd
df_dump_new = pd.read_excel(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\End Files\21st Apr\Output_Consolidated_2019-04-21.xlsx')
#req = ['CELL OPERATION DEGRADED','BASE STATION NOTIFICATION','BASE STATION CONNECTIVITY LOST','BASE STATION CONNECTIVITY DEGRADED','BASE STATION OPERATION DEGRADED','CELL FAULTY','CELL NOTIFICATION','BASE STATION CONNECTIVITY LOST','BASE STATION FAULTY','TRX INITIALISATION','BASE STATION NOTIFICATION','HSUPA CONFIGURATION FAILED','BASE STATION OPERATION DEGRADED','WCDMA CELL OUT OF USE','BCF INITIALIZATION','WCDMA CELL OUT OF USE','RNW O and M SCENARIO FAILURE','BASE STATION LICENCE NOTIFICATION','RNW DATABASE OPERATION FAILURE']
df_dump_new['Derived_Summary'] = df_dump_new['Summary']
#Index_A = df_dump_new[df_dump_new['Summary'].isin(req)]
#Index_A = pd.isna(df_dump_new['Derived_Node'])
Index_A = df_dump_new[df_dump_new['Summary'].str.contains('CELL OPERATION DEGRADED|BASE STATION NOTIFICATION|BASE STATION CONNECTIVITY LOST|BASE STATION CONNECTIVITY DEGRADED|BASE STATION OPERATION DEGRADED|CELL FAULTY|CELL NOTIFICATION|BASE STATION CONNECTIVITY LOST|BASE STATION FAULTY|TRX INITIALISATION|BASE STATION NOTIFICATION|HSUPA CONFIGURATION FAILED|BASE STATION OPERATION DEGRADED|WCDMA CELL OUT OF USE|BCF INITIALIZATION|WCDMA CELL OUT OF USE|RNW O and M SCENARIO FAILURE|BASE STATION LICENCE NOTIFICATION|RNW DATABASE OPERATION FAILURE')].index
df_dump_new.loc[Index_A,'Derived_Summary'] = df_dump_new.loc[Index_A,'Summary'] + ' - ' + df_dump_new.loc[Index_A,'Detail1']

#file_name = 'Test.xlsx'
#path = os.path.join(insert_path,file_name)
writer = pd.ExcelWriter(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\End Files\21st Apr\Test.xlsx')
df_dump_new.to_excel(writer)
writer.save()


# In[ ]:


import pandas as pd
import numpy as np
df_dump_new = pd.read_excel(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\Output_Consolidated_2019-04-28.xlsx')


# In[ ]:


df_dump_new


# In[ ]:


df_dump_new['Derived_Summary'].fillna(df_dump_new['Summary'], inplace=True)


# In[ ]:


df_dum_new['Revised_Summary'] = np.where(df_dump_new['Derived_Summary'].str.contains(np.NaN), '2G', np.where(df_dump_new['Detail1'].str.contains('LTE|MICRO BTS3900|RRU'), '4G',np.where(df_dump_new['AlarmKey'].str.contains('eNodeB'), '4G','')))


# In[ ]:


#df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].astype('str')
#df_dump_new['CellID'] = df_dump_new['CellID'].astype('str')
#df_dump_new['Class'] = df_dump_new['Class'].astype('str')
#df_dump_new['SiteID'] = df_dump_new['SiteID'].astype('str')
df_dump_new[['CellSiteID','Class','SiteID']] = df_dump_new[['CellSiteID','Class','SiteID']].astype('str')


# In[ ]:


#df_dump_new[['CellSiteID']].select_dtypes(exclude=['int','float64']).astype('int')
#df_dump_new[['CellSiteID']].select_dtypes(exclude=[np.int64,'float64'])
#df_dump_new[['CellSiteID']].select_dtypes(include=['object'])
#df_dump_new['CellID'] = df_dump_new['CellID'].astype('float')
#df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].astype('float')
#df_dump_new['Class'] = df_dump_new['Class'].astype('float')
#df_dump_new['SiteID'] = df_dump_new['SiteID'].astype('float')
#df_dump_new['CellSiteID'].unique()
df_dump_new[['CellSiteID','Class','SiteID']] = df_dump_new[['CellSiteID','Class','SiteID']].astype('float')
df_dump_new[['CellSiteID','Class','SiteID']] = df_dump_new[['CellSiteID','Class','SiteID']].astype('int')
#df_dump_new[['CellSiteID','Class','SiteID']].replace(0, np.NaN,inplace=True)


# In[ ]:


#df_dump_new['CellID'] = df_dump_new['CellID'].astype('int')
#df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].astype('int')
#df_dump_new['Class'] = df_dump_new['Class'].astype('int')
#df_dump_new['SiteID'] = df_dump_new['SiteID'].astype('int')
#df_dump_new[['CellID','CellSiteID','Class','SiteID']].replace(0, np.NaN ,inplace=True)
#type(df_dump_new['CellSiteID'].unique()[0])
#df_dump_new[['CellSiteID']].select_dtypes(exclude=[np.int64,np.float64]).loc[0:,]


# In[ ]:


#df_dump_new[['CellID','CellSiteID','Class','SiteID']].tail()
#df_dump_new[['CellSiteID','Class','SiteID']].replace(0, np.NaN,inplace=True)
#df_dump_new['CellSiteID'].unique()
#print (df_dump_new[['CellSiteID']].select_dtypes(exclude=['int','float']).loc[0:1,'CellSiteID'])
#print (type(df_dump_new[['CellSiteID']].select_dtypes(exclude=['int','float']).loc[0:1,'CellSiteID']))
#df_dump_new[['CellSiteID']].select_dtypes(exclude=['int','float'])
#df_dump_new[['CellSiteID']].select_dtypes(exclude=['int','float'])
#df_dump_new['CellSiteID'] = df_dump_new['CellSiteID'].fillna(0).astype('str').astype(np.int64)
#for i in df_dump_new['CellSiteID'].tail():
#    print (i, type(i))


# In[ ]:


df_dump_new[['CellID','CellSiteID','Class','SiteID']].tail(20)


# In[ ]:


writer = pd.ExcelWriter(r'C:\Users\eubcefm\Desktop\MBNL Alarm Processing\Test.xlsx')
df_dump_new.to_excel(writer,sheet_name='Alarm')
#df_processed[output_col].to_excel(writer,sheet_name='Alarm')
writer.save()


# In[ ]:


import pandas as pd
import numpy as np
d={'A':[0,1,2,3,4.0,5.0,6.0,'7','8','9','\\N','0']}
df=pd.DataFrame(data=d)


# In[ ]:


df


# In[ ]:


import pandas as pd
df_AlarmKey = pd.read_excel(r'C:\Users\eubcefm\Desktop\Test.xlsx')
#df_Temp = df_AlarmKey[df_AlarmKey['AlarmId']==x].sort_values(by=['from_unixtime(Reported)','Count'])


# In[ ]:


df_AlarmKey


# In[ ]:


#df_Temp = df_AlarmKey[df_AlarmKey['AlarmId']==8594064301].sort_values(by=['from_unixtime(Reported)','Count'])
df_AlarmKey[df_AlarmKey['AlarmId']==8594064301].sort_values(by=['from_unixtime(Reported)','Count'],inplace=True)


# In[ ]:


df_AlarmKey


# In[ ]:


df['A'] = df['A'].astype('float')


# In[ ]:


df['A'] = df['A'].astype('int')


# In[ ]:




