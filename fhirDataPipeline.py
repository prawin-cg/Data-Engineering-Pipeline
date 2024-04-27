# importing necessary libraries

import os
import pandas as pd
import numpy as np
import json
import configparser
from tqdm import tqdm
from customFunctions import normalizeListValues
import datetime
import sys

# reading config values
config=configparser.ConfigParser()
config.read('config.ini')
sourceFolder=config['FOLDERS']['sourceFolder']
errorFolder=config['FOLDERS']['errorFolder']
processedFolder=config['FOLDERS']['processedFolder']
outputFolder=config['FOLDERS']['outputFolder']
outputFile=config['FILES']['outputFile']

print("*"*10,"FHIR DATA PIPELINE STARTED","*"*10)
# collecting the source files to read and process
sourceFiles=os.listdir(sourceFolder)
if len(sourceFiles)==0:
    print("There are not enough source files to process")
    print("*"*10,"FHIR DATA PIPELINE COMPLETED","*"*10)
    sys.exit()

sourceFiles=[sourceFolder+file for file in sourceFiles]

print(f"Total Files to be processed: {len(sourceFiles)}")

startTime=datetime.datetime.now()
# initializing the list to store the data processed from files
attributeData=[]
valueData=[]
resourceIdData=[]
fullUrlData=[]
requestData=[]

outputDataFrames=[]
for fileIter in tqdm(range(len(sourceFiles)),desc='Processing Files'):
    file=sourceFiles[fileIter]

    try:
        with open(file,'rb') as f:
            data=f.read()
            data=json.loads(data)

        for entry in data['entry']:

            # using panda's json_normalize function we can expand the json to a pandas DataFrame (tabular)
            df=pd.json_normalize(entry['resource'])
            attribute=[]
            value=[]

            for col in df.columns.values:
                if isinstance(df[col][0],list):

                    # calling the custom normalize function if the value is a list data type to further breakdown
                    attribute, value=normalizeListValues(inpVal=df[col][0], header=col, attribute=attribute, value=value)

                else:
                    attribute.append(col)
                    value.append(df[col][0])

            attributeData.extend(attribute)
            valueData.extend(value)
            resourceIdData.extend(list(np.repeat(df['id'][0],len(attribute))))
            fullUrlData.extend(list(np.repeat(entry['fullUrl'],len(attribute))))
            requestData.extend(list(np.repeat(entry['request'],len(attribute))))

        # creating a DataFrame with the collected data
        outputDF=pd.DataFrame({'RESOURCEID':resourceIdData,'FULLURL':fullUrlData,'REQUEST':requestData,'ATTRIBUTE':attributeData,'VALUE':valueData})
        outputDataFrames.append(outputDF)
        
        #   moving to processed folder as the pipeline process is completed
        newPath=processedFolder+file.split('/')[-1]
        os.rename(file,newPath)

    except Exception as e:
        print(f"Exception occurred while processing the File {file} ")
        print(f"Exception Details: {e}")
        newPath=errorFolder+file.split('/')[-1]
        os.rename(file,newPath)

# writing the processed (tabular) data to a csv file
if len(outputDataFrames)==0:
    print("There are not enough processed data to produce output table")
    sys.exit()
    print("*"*10,"FHIR DATA PIPELINE COMPLETED","*"*10)
finalOutput=pd.concat(outputDataFrames)
finalOutput.to_csv(outputFolder+outputFile,index=False)

endTime=datetime.datetime.now()

print(f"Total Files successfully processed: {len(outputDataFrames)}")
print(f"Total Files errored with exception: {len(sourceFiles)-len(outputDataFrames)}")
print(f"Shape of the Final Table: {finalOutput.shape}")
print(f"Total size of the output file: {os.path.getsize(outputFolder+outputFile)}")
print(f"Total Time Taken: {endTime-startTime}")


print("*"*10,"FHIR DATA PIPELINE COMPLETED","*"*10)