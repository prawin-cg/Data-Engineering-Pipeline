import pandas as pd
import json
import numpy as np

# function to normalize the list values to attribute - value 
def normalizeListValues(inpVal:list, header:str, attribute:list=[], value:list=[]):
    normDF=pd.json_normalize(inpVal)
    if len(normDF.columns.values)==0:
        for i in range(len(inpVal)):
            attribute.append(header+'.'+str(i))
            value.append(inpVal[i])
    for idx in normDF.index.values:
        for col in normDF.columns.values:
            if not isinstance(normDF[col][idx],list) and not pd.isna(normDF[col][idx]):
                attribute.append(header+'.'+str(idx)+'.'+col)
                value.append(normDF[col][idx])
            elif isinstance(normDF[col][idx],list):
                # calling the function again to normalize the list further (recursive)
                attribute, value=normalizeListValues(inpVal=normDF[col][idx], header=header+'.'+str(idx)+'.'+col, attribute=attribute, value=value)
    return attribute, value

