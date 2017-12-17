import json
import pandas as pd
import sys
import numpy as np
if __name__=="__main__":
    #Taking input file as argument
    inputfile=sys.argv[1]
    with open(inputfile) as input_file:
        json_data=[json.loads(line) for line in input_file]
    #Fetching only photo id and label id and converting it into csv file
    result=[]
    for item in json_data:
        my_dict={}
        photoid=item.get('photo_id')
        my_dict['photoid']=photoid+'.jpg'
        my_dict['label']=item.get('label')
        if(photoid[0]!='-'):
            result.append(my_dict)
    df=pd.DataFrame(result)
    df=df.reindex(columns=['photoid','label'])
    print('Statistics for each label before encoding')
    value,freq=np.unique(df.ix[:,1],return_counts=True)
    print(value)
    print(freq)
    #Encoding categorical data
    from sklearn.preprocessing import LabelEncoder
    labelencoder=LabelEncoder()
    df.ix[:,1]=labelencoder.fit_transform(df.ix[:,1])
    print('Statistics for each label after encoding')
    value,freq=np.unique(df.ix[:,1],return_counts=True)
    print(value)
    print(freq)
    df.to_csv('out.csv',index=False)