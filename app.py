from flask import Flask, request, render_template, send_from_directory
from flask_cors import CORS
import pandas as pd
import json
import os
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
from werkzeug.utils import secure_filename
import mimetypes
import secrets 
import string 
from datetime import date
import datetime
import threading
import multiprocessing as mp
import logging
import time
from itertools import combinations
import numpy as np

#from itertools import combinations
#import numpy as np
#import matplotlib.pyplot as mp
#import seaborn as sb


app = Flask(__name__)
CORS(app)
app.config['UPLOAD_EXTENSIONS'] = ['.csv', '.xlsx', '.xls']
app.config['MAX_CONTENT_LENGTH'] = 10000* 1024 * 1024 
    
@app.route("/")
def helloWorld():
  return "Hello, cross-origin-world!"

@app.route('/api/configureSource', methods=['POST'])
def configureSource(): 
    uploaded_files = request.files.getlist("file[]")
    reference_files = request.files.getlist("reffile[]")
    content =json.loads(request.form.get('data'))                     
    sourcedetails= content['source']
    referencedetails= content['reference']
    settings= content['settings']

    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA= listSourceNames()
    if sourceDataName in listA:
        content['errorMsg']= 'The source name is already exist'
        content['errorflag']= 'True'
        content['errorCode']= '101'
        return json.dumps(content)
    else:
            #sourceDataDescription= sourcedetails['sourceDataDescription']
            sourceFilename = sourcedetails['sourceFileName']
            newSourceId='S'+ str(int(getSourceMaxId())+1)
            sourceFilename=newSourceId + sourceFilename
            for file in uploaded_files:                
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                        dli=[]
                        
                        file.save(sourceFilename)
                        path=sourceFilename
                        if(file_ext=='.csv'):
                            df = pd.read_csv(path)
                        elif(file_ext=='.xls'):
                            df = pd.read_excel(path)
                        elif(file_ext=='.xlsx'): 
                            df = pd.read_excel(path)
                        else:
                            df = pd.read_csv(path)

                        df=df.head()
                        resDf = df.where(pd.notnull(df), 'None')
                        data_dict = resDf.to_dict('index')

                        dli= list(df.columns.values)
                        sourcedetails['templateSourcePath']= path
                        sourcedetails['availableColumns'] = dli
                        df_meta = pd.DataFrame(df.dtypes).reset_index()
                        df_meta.columns = ["column_name", "dtype"]
                        df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                        df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                        df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                        catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
                        sourcedetails['categorialColumns'] = catFeat
            content['source']=  sourcedetails
            content['sourcePreview']=data_dict
            #reference related details           
            N = 7  
            i=1
            for file in reference_files:
                strRef=newSourceId+'-Ref' +str(i)+'-'
                refId='Ref'+str(i)
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                        dli=[]
                        res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) 
                                                        for i in range(N))
                        path= strRef + str(res)+file_ext 
                        file.save(path)         
                        if(file_ext=='.csv'):
                            df = pd.read_csv(path)
                        elif(file_ext=='.xls'):
                            df = pd.read_excel(path)
                        elif(file_ext=='.xlsx'): 
                            df = pd.read_excel(path)
                        else:
                            df = pd.read_csv(path)
                        dli= list(df.columns.values) 
                        dli2=[]
                        Dict = {}
                        dli2 = [strRef + s for s in dli]
                        referencedetails[i-1]['referenceId']=refId
                        referencedetails[i-1]['availableRefColumns'] =dli2
                        referencedetails[i-1]['referencePath']     =  path                                                                   
                        i=i+1
            data = json.load(open("db.json","r"))
            AnalysisList=  data['Analysis']
            
            content['sourceId']= newSourceId
            content['rules']=[]
            AnalysisList.append(content)
            data['Analysis'] = AnalysisList
            json.dump(data, open("db.json","w"))
            content['errorMsg']= ''
            content['errorflag']= 'False'
            return json.dumps(content)

def GetAEntityDB(sourceId):
    analysisList={}
    data={}
    with open('db.json', 'r') as openfile:
            json_object = json.load(openfile)
    
    data=json_object  
    for obj in data['Analysis']:
        if obj["sourceId"]==sourceId:  
            analysisList=obj
            break     
    data['Analysis'] = analysisList
    jsonString = json.dumps(data)       
    return jsonString


@app.route('/api/configureSource', methods=['PUT'])
def EditconfigureSource(): 
    uploaded_files = request.files.getlist("file[]")
    reference_files = request.files.getlist("reffile[]")
    content =json.loads(request.form.get('data'))                     
    sourcedetails= content['source']
    sourceId=content['sourceId']
    referencedetails= content['reference']
    settings= content['settings']


    datafromdb=  json.loads(removeAEntityDB(sourceId))
   
    allDBList= datafromdb['Analysis'] 
    
    tobeedited= json.loads(GetAEntityDB(sourceId))
    datatobeedited= tobeedited['Analysis'] 
    rules=datatobeedited['rules']
    #source related details
    sourceDataName= sourcedetails['sourceDataName']
    listA= listSourceNames()
    #sourceDataDescription= sourcedetails['sourceDataDescription']
    sourceFilename = sourcedetails['sourceFileName']
    newSourceId=sourceId
    datasource= datatobeedited['source']
    datareference= datatobeedited['reference']
    print(datasource)
    sourceFilename= newSourceId+ ""+ sourceFilename
    j=1
    for file in uploaded_files:  
                         
            filename = secure_filename(file.filename)
            print(filename)
            if filename != '':
                file_ext = os.path.splitext(filename)[1]
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    j=j+1
                    dli=[]
                    file.save(sourceFilename)
                    path=sourceFilename
                    if(file_ext=='.csv'):
                        df = pd.read_csv(path)
                    elif(file_ext=='.xls'):
                        df = pd.read_excel(path)
                    elif(file_ext=='.xlsx'): 
                        df = pd.read_excel(path)
                    else:
                        df = pd.read_csv(path)
                    df=df.head()
                    resDf = df.where(pd.notnull(df), 'None')
                    data_dict = resDf.to_dict('index')
                    dli= list(df.columns.values)
                    sourcedetails['templateSourcePath']= path
                    sourcedetails['availableColumns'] = dli
                    df_meta = pd.DataFrame(df.dtypes).reset_index()
                    df_meta.columns = ["column_name", "dtype"]
                    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
                    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
                    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")
                    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
                    sourcedetails['categorialColumns'] = catFeat  
                    content['sourcePreview']=data_dict    
    
    if(j==1) :
        sourcedetails['templateSourcePath']= datasource['templateSourcePath']
        sourcedetails['availableColumns'] = datasource['availableColumns']  
        sourcedetails['categorialColumns'] = datasource['categorialColumns'] 
    content['source']=  sourcedetails
         
    #reference related details           
    N = 7  
    i=1
    for file in reference_files:
                strRef=newSourceId+'-Ref' +str(i)+'-'
                refId='Ref'+str(i)
                
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                        dli=[]
                        res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) 
                                                        for i in range(N))
                        path= strRef + str(res)+file_ext 
                        file.save(path)         
                        if(file_ext=='.csv'):
                            df = pd.read_csv(path)
                        elif(file_ext=='.xls'):
                            df = pd.read_excel(path)
                        elif(file_ext=='.xlsx'): 
                            df = pd.read_excel(path)
                        else:
                            df = pd.read_csv(path)
                        dli= list(df.columns.values) 
                        dli2=[]
                        Dict = {}
                        dli2 = [strRef + s for s in dli]
                        referencedetails[i-1]['referenceId']=refId
                        referencedetails[i-1]['availableRefColumns'] =dli2
                        referencedetails[i-1]['referencePath']     =  path                                                                   
                        i=i+1
    if(i==1) :
        k=1
        for itlm in referencedetails:
            referencedetails[k-1]['referencePath']= datareference[k-1]['referencePath']
            referencedetails[k-1]['availableRefColumns'] = datareference[k-1]['availableRefColumns'] 
            k=k+1
    content['reference']=referencedetails
    content['rules']=rules
    allDBList.append(content)

    data={}
    data['Analysis'] = allDBList
    json.dump(data, open("db.json","w")) 
    return json.dumps(content)                                              

@app.route('/getSourceMaxId', methods=['GET'])
def getSourceMaxId():       
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)
        
        data = json_object
        dictionary ={}   
        IDList=[]
        IDList.append('0')
        for obj in data['Analysis']:            
                    IDList.append((obj["sourceId"])[1:]) 
        dictionary["IdsList"] = IDList
        print(str(IDList))
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))

def removeAEntityDB(sourceId):
        analysisList=[]
        data={}
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)
        
        data = json_object 
        for obj in data['Analysis']:
            if obj["sourceId"]!=sourceId:  
                analysisList.append(obj)     
        data['Analysis'] = analysisList
        jsonString = json.dumps(data)       
        return jsonString

@app.route('/listSourceNames', methods=['GET'])
def listSourceNames():       
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)
        
        data = json_object
        IDList=[]
        IDList.append('')
        for obj in data['Analysis']: 
            sourceobj= obj['source']           
            IDList.append((sourceobj["sourceDataName"])) 
        return (IDList)

@app.route('/api/CreateRuleSet', methods=['POST'])
def CreateRuleset():  
    data={} 
    content= request.get_json() 
    sourceId=content['sourceId']
    newRules = content['ruleset']
    data=  json.loads(removeAEntityDB(sourceId))
   
    AnalysisList= data['Analysis'] 
    
    rulesObject={}  
    datarules= json.loads(GetAEntityDB(sourceId))
    datarulesList= datarules['Analysis']  
    existingrulesList=[]
    existingrulesList=datarulesList['rules']  
    rulesObject['rulesetId']=sourceId + 'R' + str(int(getRMaxId(sourceId))+1) 
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['refSelectedColumns'] = content['refSelectedColumns']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    content['rulesetId']=rulesObject['rulesetId']
    existingrulesList.append(rulesObject)

    datarulesList['rules'] =existingrulesList

    AnalysisList.append(datarulesList)
    data['Analysis'] = AnalysisList
    json.dump(data, open("db.json","w"))
    return json.dumps(content)

@app.route('/api/CreateRuleSet', methods=['PUT'])
def editRuleset():  
    data={} 
    content= request.get_json() 
    sourceId=content['sourceId']
    newRules = content['ruleset']
    rulesetId= content['rulesetId']

    data=  json.loads(removeAEntityDB(sourceId))   
    AnalysisList= data['Analysis'] 
    
    rulesObject={}  
    datarules= json.loads(GetAEntityDB(sourceId))
    datarulesList= datarules['Analysis']  
    existingrulesListedited=[] 
    for obj1 in datarulesList['rules']:
        if obj1["rulesetId"]!=rulesetId:
            existingrulesListedited.append(obj1)
    rulesObject['selectedColumns'] = content['selectedColumns']
    rulesObject['refSelectedColumns'] = content['refSelectedColumns']
    #rulesObject['Referencepath'] = content['Referencepath']
    rulesObject['startDate'] = content['startDate']
    rulesObject['endDate'] = content['endDate']                                                                                                                         
    rulesObject['rulesetName']= content['rulesetName']
    rulesObject['ruleset']=newRules
    rulesObject['rulesetId']=rulesetId
    existingrulesListedited.append(rulesObject)

    datarulesList['rules'] =existingrulesListedited

    AnalysisList.append(datarulesList)
    data['Analysis'] = AnalysisList
    json.dump(data, open("db.json","w"))
    return json.dumps(content)

    
@app.route('/api/listSource', methods=['GET'])
def listSource():       
        with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)
        
        data = json_object       
        jsonString = json.dumps(data['Analysis'])
        return (jsonString)


@app.route('/api/rules', methods=['POST']) #GET requests will be blocked
def rulesWithRef():
    content = request.get_json()     
    sourcepath= content['sourcepath']
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)
    rules=[]    
    selectedColumns= content['selectedColumns']
    df_meta = pd.DataFrame(df.dtypes).reset_index()
    df_meta.columns = ["column_name", "dtype"]
    df_meta_inferred = pd.DataFrame(df.infer_objects().dtypes).reset_index()
    df_meta_inferred.columns = ["column_name", "inferred_dtype"]
    df_meta = df_meta.merge(df_meta_inferred, on="column_name", how="left")

    conFeat = df_meta[df_meta.dtype != "object"].column_name.tolist()
    catFeat = df_meta[df_meta.dtype == "object"].column_name.tolist()
    refColumns= content['refSelectedColumns']
    for k in selectedColumns:                                                                                                 
        Dict = {}
        RulesDict = {}
        rules_list=[]
        if is_string_dtype(df[k]): 
            RulesDict['rule'] ='DataType'        
            isAlphacount = df[k].str.isalpha().sum()            
            isAlphaNumcount= df[k].str.isalnum().sum() - isAlphacount
            if(isAlphacount>isAlphaNumcount)  :  
                RulesDict['value'] =  'Text' 
            else:
                RulesDict['value'] =  'Alphanumeric'
            RulesDict['dimension'] =  'Validity'  
            RulesDict['operator']  = 'Shouldbe'
            RulesDict['format']  = ''               
            rules_list.append(RulesDict)
            minLength= df[k].str.len().min()
            maxLength=df[k].str.len().max()
            MinRulesDict1 = {}
            MinRulesDict1['rule'] ='MaxLength'                    
            MinRulesDict1['value'] = str(maxLength)
            MinRulesDict1['dimension'] =  'Validity'
            MinRulesDict1['operator']  = 'euqualto'
            MinRulesDict1['format']  = ''
            rules_list.append(MinRulesDict1)
            
            MaxRulesDict1 = {}
            MaxRulesDict1['rule'] ='MinLength'                    
            MaxRulesDict1['value'] = str(minLength)
            MaxRulesDict1['dimension'] =  'Validity'
            MaxRulesDict1['operator']  = 'euqualto'
            MaxRulesDict1['format']  = ''
            rules_list.append(MaxRulesDict1)
            if (minLength)== (maxLength):
                RulesDict1 = {}
                RulesDict1['rule'] ='Length'                    
                RulesDict1['value'] = str(maxLength)
                RulesDict1['dimension'] =  'Validity'
                RulesDict1['operator']  = 'euqualto'
                RulesDict1['format']  = ''
                rules_list.append(RulesDict1)
        if is_numeric_dtype(df[k]):  
            RulesDict['rule'] ='DataType'           
            RulesDict['value'] =  'Numeric'    
            RulesDict['dimension'] =  'Validity' 
            RulesDict['operator']  = 'Shouldbe'
            RulesDict['format']  = ''        
            rules_list.append(RulesDict)
            print('unique entries' +k )
            print(df[k].nunique())
            if df[k].nunique()!=1:
                valueofFormula= getCorelationrelationships(df,k)
                print(valueofFormula)
                RulesDictf={}
                if len(valueofFormula['value']) != 0:
                    RulesDictf['rule'] ='Formula'           
                    RulesDictf['value'] =  valueofFormula['value']    
                    RulesDictf['dimension'] =  'Accuracy' 
                    RulesDictf['operator']  = 'euqualto'
                    RulesDictf['format']  = ''        
                    rules_list.append(RulesDictf)
        if any(k in s for s in refColumns):
            RulesDict2 = {}
            RulesDict2['rule'] ='Reference' 
            matchers = []
            matchers.append(k)
            matching = [i for i in refColumns if k in i]           
            RulesDict2['value'] =  list(matching)
            RulesDict2['dimension'] =  'Integrity'  
            RulesDict2['operator']  = 'Shouldbe'
            RulesDict2['format']  = '' 
            rules_list.append(RulesDict2)   
            print(str(matching))          
            
        Dict['column'] =k
        if k in conFeat:
            df_describe_continuous = processContinuous([k], df)
            Dict['statistics'] =df_describe_continuous
        if k in catFeat:
            df_describe_categorical = processCategorical([k], df)
            Dict['statistics'] =df_describe_categorical
        Dict['rules'] = rules_list
        rules.append(Dict)
    json_data = json.dumps(rules)  
    return json_data 

@app.route('/getRMaxId', methods=['GET'])
def getRMaxId(sourceId): 
        with open('db.json', 'r') as openfile:
            json_object = json.load(openfile)
        data = json_object     
        IDList=[]
        IDList.append('0')
        for obj in data['Analysis']:  
                if obj["sourceId"]==sourceId:  
                   for obj1 in obj["rules"]:
                        IDList.append(obj1["rulesetId"][(len(sourceId)+1):] )         
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))


@app.route('/api/LaunchAnalysis', methods=['POST']) #GET requests will be blocked
def LaunchAnalysis():

    content = request.get_json() 
    rules=[]    
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    datarules= json.loads(GetAEntityDB(sourceId))
    AnalysisObj= datarules['Analysis'] 
    sourcepath= AnalysisObj['sourcepath']
    for obj1 in AnalysisObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
            rulesfromdb=obj1
     
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   
    
    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby(['AIRLINE'])
    for AIRLINE, frame in dfs:    
        Dict = {}    
        Dict['airline'] =AIRLINE   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            #print(frame[k].isnull().sum())
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list        
        Dict['completness'] = Dictconstraints
        value=[100] 
        vnulls_list=[]
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatch'] =str(cnttotal-cntalnum)
                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "")
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatch'] =str(cnttotal-cntalnum)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(ru['value'])).sum())/(frame[col].count()))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatch'] = str((frame[col].str.len() !=int(ru['value'])).sum())
                        vnulls_list.append(DNull)  
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = res
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints      
        rules.append(Dict)  
    json_data = json.dumps(rules) 
    #saveAnalysisResult(AnalysisObj, rulesfromdb, json_data,AnalysisId,rulesetId)
    return json_data

@app.route('/api/LaunchAnalysisbyKeyold', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbykeyold():

    content = request.get_json() 
    rules=[]    
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   

    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            #print(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = resDf.to_dict('index')
            count=count+1 
            resultId= sourceId +rulesetId +'RS'+str(count)
            DNull['Outlier']=resultId
            saveResultsets(data_dict,resultId)


            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['OutLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        resDf1 = val.where(pd.notnull(val), 'None')
        
        data_dict = resDf1.to_dict('index')

        count=count+1 
        resultId= sourceId +rulesetId +'RS'+str(count)
        DNull['outlier']=resultId
        saveResultsets(data_dict,resultId)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
                   
        
        rules.append(Dict)  
    json_data = json.dumps(rules) 

    return json_data

@app.route('/api/LaunchAnalysisbyKey', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbykey():

    content = request.get_json() 
    rules=[]    
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   
    resultsetDictlist=[]
    

    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            #print(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
        
            resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = resDf.to_dict('index')
            count=count+1 
            resultId= sourceId +rulesetId +'RS'+str(count)
            DNull['Outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
           


            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['OutLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = resDf1.to_dict('index')

        count=count+1 
        resultId= sourceId +rulesetId +'RS'+str(count)
        DNull['outlier']=resultId
       
        
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)

        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            #DNull['Outlier']=r3.to_dict()
                            
                            count=count+1 
                            resultId= sourceId +rulesetId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = resDf2.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)
                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "")
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= sourceId +rulesetId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = resDf3.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)


                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= sourceId +rulesetId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = resDf5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['OutLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= sourceId +rulesetId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = resDf6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)


                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                    
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict)  
        saveResultsets(resultsetDictlist,(sourceId +rulesetId))
    json_data = json.dumps(rules) 

    return json_data

def processContinuous(conFeat, data):
    conHead = ['Count', 'Miss %', 'Card.', 'Min', '1st Qrt.', 'Mean', 'Median', '3rd Qrt', 'Max', 'Std. Dev.',
               'Outliers']

    conOutDF = pd.DataFrame(index=conFeat, columns=conHead)
    conOutDF.index.name = 'feature_name'
    columns = data[conFeat]
    
    # COUNT
    count = columns.count()
    conOutDF[conHead[0]] = count

    # MISS % - no continuous features have missing data
    percents = [''] * len(conFeat)
    outliers = [''] * len(conFeat)
    for col in columns:
        percents[conFeat.index(col)] = round(columns[col].isnull().sum() / len(columns[col]) * 100, 2)
        anomaly_cut_off = columns[col].std() * 3
        mean = columns[col].mean()
        outliers[conFeat.index(col)] = len(columns[columns[col] < (mean - anomaly_cut_off)]) \
                                       + len(columns[columns[col] > (mean + anomaly_cut_off)])

    conOutDF[conHead[1]] = percents

    # CARDINALITY
    conOutDF[conHead[2]] = columns.nunique()

    # MINIMUM
    conOutDF[conHead[3]] = columns.min()

    # 1ST QUARTILE
    conOutDF[conHead[4]] = columns.quantile(0.25)

    # MEAN
    conOutDF[conHead[5]] = round(columns.mean(), 2)

    # MEDIAN
    conOutDF[conHead[6]] = columns.median()

    # 3rd QUARTILE
    conOutDF[conHead[7]] = columns.quantile(0.75)

    # MAX
    conOutDF[conHead[8]] = columns.max()
    conOutDF['Data Type']='Numeric'
    conOutDF['suggested_dtype'] = 'Numeric'
    # STANDARD DEVIATION
    conOutDF[conHead[9]] = round(columns.std(), 2)

    # IQR
    conOutDF[conHead[10]] = outliers
   
    return  conOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

def processCategorical(catFeat, data):
    catHead = ['Count', 'Miss #', 'Miss %', 'Card.', 'Len Min', 'Len Max', 'Data Type',
               'Alphabetic', 'Numeric', 'Alphanumeric']

    catOutDF = pd.DataFrame(index=catFeat, columns=catHead)
    catOutDFMeta = None
    catOutDFPattern = None
    catOutDF.index.name = 'feature_name'
    columns = data[catFeat]
    total_rows = len(data)
    # COUNT
    count = columns.count()
    catOutDF[catHead[0]] = count

    # CARDINALITY
    catOutDF[catHead[3]] = columns.nunique()

    # preparing arrays for storing data
    amt = len(catFeat)
    missNumbers = [''] * amt
    missPercents = [''] * amt
    
    lenMin = [''] * amt
    lenMax = [''] * amt
    dataType = [''] * amt
    dataType2 = [''] * amt
    isAlpha = [''] * amt
    isNum = [''] * amt
    isAlNum = [''] * amt
    isSpace = [''] * amt
    for col in columns:
        values = columns[col].value_counts()
        col_meta = pd.DataFrame(values).reset_index()
        index = catFeat.index(col)

        # MISS %
        missNumbers[index] = columns[col].isnull().sum()
        percent = (missNumbers[index] / total_rows) * 100
        missPercents[index] = round(percent, 2)
        
        # MODES
        #mode = values.index[0]
        #mode2 = values.index[1]
        #modes[index] = mode
        #modes2[index] = mode2

        # MODE FREQ
        #modeCount = values.loc[mode]
        #modeCount2 = values.loc[mode2]
        #modeFreqs[index] = modeCount
        #modeFreqs2[index] = modeCount2

        # MODE %
        miss = missPercents[index]
        #modePer = (modeCount / (count[index] * ((100 - miss) / 100))) * 100
        #modePercents[index] = round(modePer, 2)

        #modePer2 = (modeCount2 / (count[index] * ((100 - miss) / 100))) * 100
        #modePercents2[index] = round(modePer2, 2)

        # VALUE LENGTH & TYPE
        col_meta = pd.DataFrame(values).reset_index().rename(columns={'index': 'value', col: 'count'})
        col_meta.insert(0, "feature_name", col)
        col_meta['length'] = col_meta.value.apply(lambda x: len(x))
        col_meta['type'] = col_meta.value.apply(lambda x: type(x))
        col_meta['isAlpha'] = col_meta.value.str.isalpha()
        col_meta['isNum'] = col_meta.value.str.isnumeric()
        col_meta['isAlNum'] = col_meta.value.str.isalnum()
        if index == 0:
            catOutDFMeta = col_meta
        else:
            catOutDFMeta = pd.concat([catOutDFMeta, col_meta], ignore_index=True)
        lenMin[index] = col_meta['length'].min()
        lenMax[index] = col_meta['length'].max()
        dataType[index] = col_meta['type'].value_counts().index[0]
        try:
            dataType2[index] = col_meta['type'].value_counts().index[1]
        except Exception as e:
            dataType2[index] = None

        isAlpha[index] = columns[col].str.isalpha().sum()
        isNum[index] = columns[col].str.isnumeric().sum()
        isAlNum[index] = columns[col].str.isalnum().sum() - isAlpha[index]

    catOutDF[catHead[1]] = missNumbers
    catOutDF[catHead[2]] = missPercents
    catOutDF[catHead[4]] = lenMin
    catOutDF[catHead[5]] = lenMax
    catOutDF[catHead[6]] = "object"
    catOutDF[catHead[7]] = isAlpha
    catOutDF[catHead[8]] = isNum
    catOutDF[catHead[9]] = isAlNum
    catOutDF['suggested_dtype'] = catOutDF.filter(items=['Alphabetic', 'Numeric', 'Alphanumeric'], axis=1) \
        .idxmax(axis=1)
   
    return  catOutDF.to_dict('records')#.to_json(orient='records')[1:-1].replace('},{', '} {')

@app.route('/api/getAllAnalysis', methods=['GET'])
def getAllAnalysis(): 
    analysisList=[]
    data={}
    
    with open('db.json', 'r') as openfile:
                json_object = json.load(openfile)
        
    data = json_object 
    for obj in data['Analysis']:
        tempDict={}
        tempDict['sourceId']= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        temprules=[]
        for obj1 in obj["rules"]:            
            temprulesDict={}
            temprulesDict['sourceId']= obj['sourceId']
            temprulesDict["rulesetId"]=obj1["rulesetId"]
            temprulesDict["selectedColumns"]=    obj1["selectedColumns"]  
            #temprulesDict["Referencepath"]=    obj1["Referencepath"]
            temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]  
            temprulesDict['startDate'] = obj1['startDate']
            temprulesDict['endDate'] = obj1['endDate']
            temprulesDict["ruleset"]= obj1["ruleset"]
            temprulesDict["rulesetName"]= obj1["rulesetName"]
            temprulesDict["columns"]= sourceData["availableColumns"]  
            startDate_obj =  datetime.datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
          
            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'
            
            temprules.append(temprulesDict)
        tempDict['rules']=temprules
        analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data)       
    return jsonString


    
@app.route('/api/DelayAnalysis', methods=['POST']) #GET requests will be blocked
def DelayAnalysis():

    content = request.get_json() 
    rules=[]    
    rules1=[] 
    AnalysisId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    listofNumdelays=[]
    listofTotaldelays=[]
    cdecolumns=[] 
    datarules= json.loads(GetAEntityDB(AnalysisId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"] 
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   
    cdecolumns = ["AIRLINE","FLIGHT_NUMBER","TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY","ARRIVAL_DELAY","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY"]
    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby(['AIRLINE'])
    for AIRLINE, frame in dfs:    
        Dict = {}    
        Dict['airline'] =AIRLINE   
        Dict1 = {}    
        Dict1['airline'] =AIRLINE 
        Dictconstraints = {}   
        Dictconstraints1 = {}          
        Dictconstraints['NoOfDelays'] = str((frame['ARRIVAL_DELAY'] >= 15).sum() )
        Dictconstraints1['TotalDelays'] = str(frame.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
        listofNumdelays.append(Dictconstraints['NoOfDelays']) 
        listofTotaldelays.append(Dictconstraints1['TotalDelays']) 
        framesum= frame.groupby(['ORIGIN_AIRPORT']) 
        dictList=[] 
        dictList1=[] 
        for ORIGIN_AIRPORT, framesub in framesum:    
            Dictdet = {}    
            Dictdet['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
            Dictdet1 = {}    
            Dictdet1['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT
            Dictdet['NoOfDelays'] = str((framesub['ARRIVAL_DELAY'] >= 15).sum() )
            Dictdet1['TotalDelays'] = str(framesub.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
            dictList.append(Dictdet)
            dictList1.append(Dictdet1)
        Dictconstraints['details']=dictList 
        Dictconstraints1['details']=dictList1
        Dict['value'] = Dictconstraints
        Dict1['value'] = Dictconstraints1
        rules.append(Dict)  
        rules1.append(Dict1)
    listofNumdelays = [int(float(s)) for s in listofNumdelays]
    graphDim={}
    graphDim['Title'] = "Airline vs #ofDelays"
    graphDim['X-Axis'] = "Airline"
    graphDim['Y-Axis'] = "#ofDelays"
    graphDim['Y-Axismin'] = str(min(listofNumdelays)-10 )
    graphDim['Y-Axismax'] = str(max(listofNumdelays)+10 )
    graphDim['Y-AxisInterval'] = '20'
    graphDim['data']= rules
    if (min(listofNumdelays)-10 <0):
        graphDim['Y-Axismin'] = 0

    listofTotaldelays = [ int(float(s))  for s in listofTotaldelays] 
    graphDim1={}
    graphDim1['Title'] = "Airline vs TotalTimeDelays"
    graphDim1['X-Axis'] = "Airline"
    graphDim1['Y-Axis'] = "TotalTimeDelays"
    graphDim1['Y-Axismin'] = str(min(listofTotaldelays)-10 )
    graphDim1['Y-Axismax'] = str(max(listofTotaldelays)+10 )
    graphDim1['Y-AxisInterval'] = '200'
    graphDim1['data']= rules1
    if (min(listofTotaldelays)-10 <0):
        graphDim1['Y-Axismin'] = 0
    val=max(listofTotaldelays)
    graph=[] 
    graph.append(graphDim)
    graph.append(graphDim1)
    
    json_data = json.dumps(graph) 
    return json_data


@app.route('/api/DelayAnalysisbyAirPortnew', methods=['POST']) #GET requests will be blocked
def DelayAnalysisbyAirPortnew():

    content = request.get_json() 
    rules=[]    
    rules1=[] 
    AnalysisId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    listofNumdelays=[]
    listofTotaldelays=[]
    cdecolumns=[] 
    datarules= json.loads(GetAEntityDB(AnalysisId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"] 
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   
    cdecolumns = ["AIRLINE","FLIGHT_NUMBER","TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY","ARRIVAL_DELAY","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY"]
    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby(['ORIGIN_AIRPORT'])
    for ORIGIN_AIRPORT, frame in dfs:    
        Dict = {}    
        Dict['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT   
        Dict1 = {}    
        Dict1['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT 
        Dictconstraints = {}   
        Dictconstraints1 = {}          
        Dictconstraints['NoOfDelays'] = str((frame['ARRIVAL_DELAY'] >= 15).sum() )
        Dictconstraints1['TotalDelays'] = str(frame.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
        listofNumdelays.append(Dictconstraints['NoOfDelays']) 
        listofTotaldelays.append(Dictconstraints1['TotalDelays']) 
        framesum= frame.groupby(['AIRLINE']) 
        dictList=[] 
        dictList1=[] 
        for AIRLINE, framesub in framesum:    
            Dictdet = {}    
            Dictdet['AIRLINE'] =AIRLINE
            Dictdet1 = {}    
            Dictdet1['AIRLINE'] =AIRLINE
            Dictdet['NoOfDelays'] = str((framesub['ARRIVAL_DELAY'] >= 15).sum() )
            Dictdet1['TotalDelays'] = str(framesub.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
            dictList.append(Dictdet)
            dictList1.append(Dictdet1)
        Dictconstraints['details']=dictList 
        Dictconstraints1['details']=dictList1
        Dict['value'] = Dictconstraints
        Dict1['value'] = Dictconstraints1
        rules.append(Dict)  
        rules1.append(Dict1)
    listofNumdelays = [int(float(s)) for s in listofNumdelays]
    graphDim={}
    graphDim['Title'] = "Airport vs #ofDelays"
    graphDim['X-Axis'] = "Airport"
    graphDim['Y-Axis'] = "#ofDelays"
    graphDim['Y-Axismin'] = str(min(listofNumdelays)-10 )
    graphDim['Y-Axismax'] = str(max(listofNumdelays)+10 )
    graphDim['Y-AxisInterval'] = '20'
    if (min(listofNumdelays)-10 <0):
        graphDim['Y-Axismin'] = 0
    graphDim['data']= rules

    listofTotaldelays = [ int(float(s))  for s in listofTotaldelays] 
    graphDim1={}
    graphDim1['Title'] = "Airport vs TotalTimeDelays"
    graphDim1['X-Axis'] = "Airport"
    graphDim1['Y-Axis'] = "TotalTimeDelays"
    graphDim1['Y-Axismin'] = str(min(listofTotaldelays)-10 )
    graphDim1['Y-Axismax'] = str(max(listofTotaldelays)+10 )
    graphDim1['Y-AxisInterval'] = '200'
    if (min(listofTotaldelays)-10 <0):
        graphDim1['Y-Axismin'] = 0
    graphDim1['data']= rules1
    val=max(listofTotaldelays)
    graph=[] 
    graph.append(graphDim)
    graph.append(graphDim1)
    
    json_data = json.dumps(graph) 
    return json_data




@app.route('/api/DelayAnalysisByAirport', methods=['POST']) #GET requests will be blocked
def DelayAnalysisByAirport():

    content = request.get_json() 
    rules=[]    
    AnalysisId = content['sourceId']
    rulesetId = content['rulesetId']
    sourcepath=''
    cdecolumns=[] 
    datarules= json.loads(GetAEntityDB(AnalysisId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']
    sourcepath= AnalysisObj['templateSourcePath']
    for obj1 in sourceObj["rules"]:             
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"] 
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   
    cdecolumns = ["AIRLINE","FLIGHT_NUMBER","TAIL_NUMBER","ORIGIN_AIRPORT","DESTINATION_AIRPORT","DEPARTURE_DELAY","ARRIVAL_DELAY","AIR_SYSTEM_DELAY","SECURITY_DELAY","AIRLINE_DELAY","LATE_AIRCRAFT_DELAY","WEATHER_DELAY"]
    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby(['ORIGIN_AIRPORT'])
    for ORIGIN_AIRPORT, frame in dfs:    
        Dict = {}    
        Dict['ORIGIN_AIRPORT'] =ORIGIN_AIRPORT   
        Dictconstraints = {}          
        Dictconstraints['NoOfDelays'] = str((frame['ARRIVAL_DELAY'] >= 15).sum() )
        Dictconstraints['TotalDelays'] = str(frame.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
           
        framesum= frame.groupby(['AIRLINE']) 
        dictList=[] 
        for AIRLINE, framesub in framesum:    
            Dictdet = {}    
            Dictdet['AIRLINE'] =AIRLINE
            Dictdet['NoOfDelays'] = str((framesub['ARRIVAL_DELAY'] >= 15).sum() )
            if (framesub['ARRIVAL_DELAY'] >= 15).sum() >0:
                Dictdet['TotalDelays'] = str(framesub.query("ARRIVAL_DELAY >= 15")['ARRIVAL_DELAY'].sum()) #str((frame['ARRIVAL_DELAY']).sum())
                dictList.append(Dictdet)
        Dictconstraints['details']=dictList 
        Dict['value'] = Dictconstraints
        rules.append(Dict)  
    json_data = json.dumps(rules) 
    return json_data

@app.route('/getLaunchMaxId', methods=['GET'])
def getLaunchMaxId(sourceId):       
        
        with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)
            
        data = json_object        
        IDList=[]
        IDList.append('0')
        for obj in data['LaunchAnalysis']:  
                if obj["sourceId"]==sourceId:  
                   for obj1 in obj["uploads"]:
                        IDList.append(obj1["launchId"][(len(sourceId)+1):] )         
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))


@app.route('/getUploadMaxId', methods=['GET'])
def getUploadMaxId(sourceId):       
        with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)
            
        data = json_object         
        IDList=[]
        IDList.append('0')
        for obj in data['LaunchAnalysis']:  
                if obj["sourceId"]==sourceId:  
                   for obj1 in obj["uploads"]:
                        IDList.append(obj1["uploadId"][(len(sourceId)+1):] )         
        dli2 = [int(s) for s in IDList]
        return (str(max(dli2)))


def GetALaunchEntityDB(sourceId,uploadId):
    analysisList={}
    entiresourceList={}
    data={}
    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)
            
    data = json_object    
    result={}
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId: 
            entiresourceList=obj
            for obj1 in obj["uploads"]: 
                if obj1["uploadId"]==uploadId:
                    analysisList=obj1
                    break     
    result['Analysis'] = analysisList
    result['EntireSourceList'] = entiresourceList
    jsonString = json.dumps(result)       
    return jsonString

def GetAEntireLaunchEntityDB(sourceId):
    analysisList={}
    entiresourceList=[]
    entiresourceObj={}
    entiresourceObj['uploads']=[]
    data={}
    with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)
            
    data = json_object     
    result={}
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId: 
            entiresourceObj=obj
        else:
            entiresourceList.append(obj)
    result['EntireSourceObj'] = entiresourceObj
    result['EntireSourceList'] = entiresourceList
    jsonString = json.dumps(result)       
    return jsonString


def removeALaunchEntityDB(sourceId):
    analysisList=[]
    data={}
    
    with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)
            
    data = json_object  
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]!=sourceId: 
             analysisList.append(obj)     
    result['Analysis'] = analysisList
    jsonString = json.dumps(result)      
    return jsonString

def GetDBentitiesforLaunch(sourceId,uploadId):
    exactuploadList={}
    SourceremovedList=[]
    exactSourceList={}
    data={}
    with open('ldb.json', 'r') as openfile:
                    json_object = json.load(openfile)
            
    data = json_object  
    result={}
    IDList=[]
    IDList.append('0')
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId: 
            exactSourceList=obj
            for obj1 in obj["uploads"]:
                IDList.append(obj1["launchId"][(len(sourceId)+1):] ) 
                if obj1["uploadId"]==uploadId:
                    exactuploadList=obj1
                    
        else:   
            SourceremovedList.append(obj)

            
    result['exactuploadList'] = exactuploadList
    result['SourceremovedList'] = SourceremovedList
    result['exactSourceList'] = exactSourceList
    dli2 = [int(s) for s in IDList]
    result['launchMaxId'] =  (str(max(dli2)))
    jsonString = json.dumps(result)
    return jsonString

def saveResultsets(data,name):
    #jsonString = json.dumps(data)
    filename= name + '.json'
    
    json.dump(data, open(filename,"w"))
    return True

@app.route('/api/getLaunchResult', methods=['GET'])
def getLaunchResult():
    sid = request.args.get('id')
    ssid =  sid.split('RS', 2)[0] 
    filename= ssid + '.json'
    with open(filename, 'r') as openfile:
        json_object = json.load(openfile)
            
    data = json_object 
    
    res={}
    for obj in data:        
        if obj['resultset'] == sid:
            res['result'] = obj['results']
            break
    jsonString = json.dumps(res)     
    return jsonString


@app.route('/api/getLaunchResultold', methods=['GET'])
def getLaunchResultold():
    ssid = request.args.get('id')
    filename= ssid + '.json'
    res={}
    with open(filename, 'r') as openfile:
        json_object = json.load(openfile)
            
    data = json_object 
    res['result'] =data
    
    jsonString = json.dumps(res)     
    return jsonString   

@app.route('/api/LaunchAnalysisbyParam', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbyParam():
    content = request.get_json() 
    rules=[]    
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    uploadId= content['uploadId']
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']
    '''
    DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=DBList['Analysis'] 

    LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['Analysis'] 
    EntireSourceList=LaunchEntityRaw['EntireSourceList'] 
    sourcepath= LaunchEntity['sourcePath']
    '''
    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList'] 

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList'] 
    EntireSourceList=LaunchEntityRaw['exactSourceList'] 
    sourcepath= LaunchEntity['sourcePath']
    launchMaxId= LaunchEntity['launchMaxId']
    existingLaunchSourceList=[] 
    for obj1 in EntireSourceList["uploads"]: 
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1) 
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   

    df1 = pd.DataFrame(df, columns=cdecolumns)   
    dfs= df1.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = resDf.to_dict('index')
            count=count+1 
            resultId= launchNewId +'RS'+str(count)
            DNull['Outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['OutLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = resDf1.to_dict('index')


        count=count+1 
        resultId= launchNewId +'RS'+str(count)
        DNull['Outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = resDf2.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "")
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = resDf3.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = resDf5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                     
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['OutLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = resDf6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                    
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict) 
        saveResultsets(resultsetDictlist,(launchNewId))  
    json_data = json.dumps(rules) 
    resultobject={}
    resultobject['launchId']=launchNewId
    resultobject['results']=rules
    LaunchEntity['launchAnalysis'] = resultobject
    LaunchEntity['launchId']=launchNewId

    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity
   
    jsonString = json.dumps(data)
    json.dump(data, open("ldb.json","w"))
    return json.dumps(content)

@app.route('/api/LaunchAnalysisbyKeyfromDb', methods=['POST']) #GET requests will be blocked
def LaunchAnalysisbyKeyfromDb():

    content = request.get_json() 
    rules=[]    
    sourceId = content['sourceId']
    rulesetId = content['rulesetId']
    KeyName = content['keyname']
    uploadId=content['uploadId']
    
    analysisList={}
    entiresourceList={}
    data={}
    
    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)
            
    data = json_object 
    newuploadId=''
    jsonString=''
    result={}
    isavailableflag=False
    for obj in data['LaunchAnalysis']:
        if obj["sourceId"]==sourceId: 
            entiresourceList=obj
            for obj1 in obj["uploads"]: 
                if obj1["uploadId"]==uploadId:
                    analysisList=obj1
                    resultAlist = analysisList['launchAnalysis']
                    resultsAllList= analysisList['AnalysisResultList']
                    
                    if not resultsAllList:
                        print('1') 
                        #resultfinal=LaunchAnalysisFnCall(sourceId,rulesetId,KeyName,obj1["uploadId"])
                        content['errorMsg']= 'The processing is going on.Please try again after some time'
                        content['errorflag']= 'True'
                        content['errorCode']= '104'
                        return json.dumps(content)
                        #jsonString = json.dumps(resultfinal)
                    else:     
                        print('2') 
                        ''' 
                        if(resultAlist['keyName']== KeyName):            
                            resultfinal= resultAlist['results']
                            jsonString = json.dumps(resultfinal) 
                            isavailableflag =True
                        else:
                        '''   
                        for resultitem in resultsAllList:
                                if(resultitem['keyName']== KeyName): 
                                    resultfinal= resultitem['results']
                                    jsonString = json.dumps(resultfinal) 
                                    isavailableflag =True
                                    break
                        if isavailableflag==False:
                                content['errorMsg']= 'The processing is going on.Please try again after some time'
                                content['errorflag']= 'True'
                                content['errorCode']= '104'
                                return json.dumps(content)
                    break    
            if isavailableflag==False:
                                content['errorMsg']= 'The processing is going on.Please try again after some time'
                                content['errorflag']= 'True'
                                content['errorCode']= '104'
                                return json.dumps(content)     
    return jsonString

@app.route('/api/uploadSourceold', methods=['POST'])
def uploadSourceold(): 
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))                     
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']
    
    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    keyNametoLaunch= sourceCatColumns[0]   
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourceFilename=newuploadId + str(uploadDate)+ sourceFilename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']  
    uploadsObject= EntireSourceObj['uploads']
    
    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
                        
    for file in uploaded_files:    
        filename = secure_filename(file.filename)
        if filename != '':
                file_ext = os.path.splitext(filename)[1]
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    if(file_ext=='.csv'):
                        df = pd.read_csv(sourcePath)
                    elif(file_ext=='.xls'):
                        df = pd.read_excel(sourcePath)
                    elif(file_ext=='.xlsx'): 
                        df = pd.read_excel(sourcePath)
                    else:
                        df = pd.read_csv(sourcePath)
                    dli= list(df.columns.values)
                    sourcedetails['availableColumns'] = dli   
                    dli=dli.sort() 
                    sourceavailableColumns=sourcedetails['availableColumns']
                    sourceavailableColumns=sourceavailableColumns.sort() 
                    if dli != sourceavailableColumns: 
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourceFilename
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]
    '''for objcol in sourceCatColumns:         
        resultobject={}
        resultobject['launchId']=''
        resultobject['keyName']=objcol
        resultobject['results']=[]               
        resultList.append(resultobject)
    '''
    resultContent['AnalysisResultList']=[]
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey  
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    
    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data)
    json.dump(data, open("ldb.json","w")) 
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {} 
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail
    

    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=keyNametoLaunch

    print(keyNametoLaunch)




    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]    
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
              
        print('Completed the main thread function')
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict)

@app.route('/api/uploadSourcenew', methods=['POST'])
def uploadSourcenew(): 
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))                     
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']
    
    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    keyNametoLaunch= sourceCatColumns[0]   
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + str(uploadDate)+ sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']  
    uploadsObject= EntireSourceObj['uploads']
    
    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
                        
    for file in uploaded_files:    
        filename = secure_filename(file.filename)
        if filename != '':
                print(sourceFilename)
                file_ext = os.path.splitext(filename)[1]
                sourceFilename=sourceFilename+file_ext
                print(file_ext)
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    if(file_ext=='.csv'):
                        df = pd.read_csv(sourcePath)
                    elif(file_ext=='.xls'):
                        df = pd.read_excel(sourcePath)
                    elif(file_ext=='.xlsx'): 
                        df = pd.read_excel(sourcePath)
                    else:
                        df = pd.read_csv(sourcePath)
                    dli= list(df.columns.values)
                    sourcedetails['availableColumns'] = dli   
                    dli=dli.sort() 
                    sourceavailableColumns=sourcedetails['availableColumns']
                    sourceavailableColumns=sourceavailableColumns.sort() 
                    if dli != sourceavailableColumns: 
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourceFilename
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]
    
    resultContent['AnalysisResultList']=[]
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey  
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    
    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data)
    json.dump(data, open("ldb.json","w"))
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {} 
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail
    

    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=sourceCatColumns

  


    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[]    
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        LaunchAnalysisthread(sourceId,rulesetId,KeyName,uploadId)      
        print('Completed the main thread function')
   
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()

    return json.dumps(tempDict)


def LaunchAnalysisFnCall(sourceId,rulesetId,KeyName,uploadId):
    print('starting long running task')
    rules=[]    
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList'] 

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList'] 
    EntireSourceList=LaunchEntityRaw['exactSourceList'] 
    sourcepath= LaunchEntity['sourcePath']
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[] 
    for obj1 in EntireSourceList["uploads"]: 
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1) 
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   

    df1 = pd.DataFrame(df, columns=cdecolumns)   
    df2 = df1.where(pd.notnull(df1), 'None')
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = val1.to_dict('index')
            count=count+1 
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = val.to_dict('index')


        count=count+1 
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "")
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                    
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                    
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict) 
        saveResultsets(resultsetDictlist,(launchNewId))  
    #json_data = json.dumps(rules) 
    resultobject={}
    resultobject['launchId']=launchNewId
    resultobject['keyName']=KeyName
    resultobject['results']=rules
    #LaunchEntity['launchAnalysis'] = resultobject
    LaunchEntity['launchId']=launchNewId
    AnalysisResultList.append(resultobject)
    LaunchEntity['AnalysisResultList']=AnalysisResultList
    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity
    
    jsonString = json.dumps(data)
    json.dump(data, open("ldb.json","w"))

    return rules

def LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId):
    print('starting long running task')
    rules=[]    
    keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList'] 

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList'] 
    EntireSourceList=LaunchEntityRaw['exactSourceList'] 
    sourcepath= LaunchEntity['sourcePath']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[] 
    for obj1 in EntireSourceList["uploads"]: 
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1) 
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   

    df1 = pd.DataFrame(df, columns=cdecolumns)   
    df2 = df1.where(pd.notnull(df1), 'None')
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = val1.to_dict('index')
            count=count+1 
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = val.to_dict('index')


        count=count+1 
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "",regex=False)
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                    
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict) 
        saveResultsets(resultsetDictlist,(launchNewId))  
   
    json_data = json.dumps(rules) 
    resultobject={}
    resultobject['launchId']=launchNewId    
    resultobject['keyName']=keyv
    resultobject['results']=rules
    LaunchEntity['launchAnalysis'] = resultobject
    LaunchEntity['launchId']=launchNewId
    print('before' + keyv )
    print(AnalysisResultList)
    AnalysisResultList.append(resultobject)
    LaunchEntity['AnalysisResultList']=AnalysisResultList

    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity
    jsonString = json.dumps(data)
    json.dump(data, open("ldb.json","w")) 
    print('Completed for' + keyv )
    return 'Completed'

@app.route('/registrazione', methods=['GET', 'POST']) 
def start_task():
    content1 = request.get_json() 
    rules=[]    
    sourceId = content1['sourceId']
    print(sourceId)
    def long_running_task(**kwargs):
        content = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", content)
        rules=[]    
        sourceId = content['sourceId']
        rulesetId = content['rulesetId']
        KeyName = content['keyname']
        uploadId=content['uploadId']
        saveResultsets(content,'testResultset')
              
        print('Completed the main thread function')
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': content1})
    thread.start()
    thread.join()
    return 'started'

@app.route('/api/getAllSources', methods=['GET'])
def getAllSources(): 
    analysisList=[]
    data={}
    with open('db.json', 'r') as openfile:
        json_object = json.load(openfile)
            
    data = json_object 

    
    with open('ldb.json', 'r') as openfile:
        json_object = json.load(openfile)
            
    dataldb = json_object 
    LDBData =dataldb['LaunchAnalysis']

    for obj in data['Analysis']:
        tempDict={}
        tempDict['sourceId']= obj['sourceId']
        sourceId= obj['sourceId']
        tempDict['source']= obj['source']
        sourceData= obj['source']
        tempDict['reference']= obj['reference']
        tempDict['settings']=obj['settings']
        temprules=[]
        uploadSourceList=[]
        recentsourceUpload={}
        for objldb in LDBData:
           
            if objldb["sourceId"]==sourceId:
                
                for objldb1 in objldb["uploads"]:
                    uploadSourceDetail= {} 
                    uploadSourceDetail['uploadId'] =objldb1["uploadId"]
                    uploadSourceDetail['isMultiSource'] =objldb1["isMultiSource"]
                    if(objldb1["isMultiSource"]=="Yes"):
                        multiSourcePathObj= objldb1['multiSourcePath']
                        uploadSourceDetail['multiSourceKey'] =multiSourcePathObj["multiSourceKey"]
                    uploadSourceDetail['uploadDate'] =objldb1["uploadDate"]
                    uploadSourceDetail['uploadTime'] =objldb1["uploadTime"]
                    uploadSourceDetail['sourceFileName']=objldb1["sourceFileName"]
                    uploadSourceList.append(uploadSourceDetail)
                    recentsourceUpload=uploadSourceDetail   
        for obj1 in obj["rules"]:            
            temprulesDict={}
            temprulesDict['sourceId']= obj['sourceId']
            temprulesDict["rulesetId"]=obj1["rulesetId"]
            temprulesDict["selectedColumns"]=    obj1["selectedColumns"]  
            #temprulesDict["Referencepath"]=    obj1["Referencepath"]
            temprulesDict["refSelectedColumns"]=    obj1["refSelectedColumns"]  
            temprulesDict['startDate'] = obj1['startDate']
            temprulesDict['endDate'] = obj1['endDate']
            temprulesDict["ruleset"]= obj1["ruleset"]
            temprulesDict["rulesetName"]= obj1["rulesetName"]
            temprulesDict["columns"]= sourceData["availableColumns"]  
            
            startDate_obj =  datetime.datetime.strptime(obj1['startDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
            endDate_obj = datetime.datetime.strptime(obj1['endDate'],"%Y-%m-%dT%H:%M:%S.%fZ")
          
            if startDate_obj.date() <= date.today() <= endDate_obj.date():
                temprulesDict["Rulestatus"]= "Active"
            elif startDate_obj.date() >= date.today():
                temprulesDict["Rulestatus"]= "Inactive"
            else:
                temprulesDict["Rulestatus"]= 'Expired'
            
            temprules.append(temprulesDict)
        tempDict['rules']=temprules
        tempDict["UploadsHistory"]= uploadSourceList
        tempDict['recentsourceUpload'] = recentsourceUpload
        analysisList.append(tempDict)
    data['Analysis'] = analysisList
    jsonString = json.dumps(data)       
    return jsonString

def LaunchAnalysisthread(sourceId,rulesetId,KeyNameList,uploadId):
    print('starting long running task')
    rules=[]    
    #keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList'] 

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList'] 
    EntireSourceList=LaunchEntityRaw['exactSourceList'] 
    sourcepath= LaunchEntity['sourcePath']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[] 
    for obj1 in EntireSourceList["uploads"]: 
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1) 
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   

    df2 = pd.DataFrame(df, columns=cdecolumns)   
    #df2 = df1.where(pd.notnull(df1), 'None')
    
    
    q = mp.Queue()
    jobs = []
    i=0
    
    for keyItem in KeyNameList:
        i=i+1
        launchNewId=sourceId + 'L' + str(int(launchMaxId)+i)
        p = mp.Process(target=LaunchAnalysisbyKeyNames, args=(df2,keyItem,launchNewId,ruleset,cdecolumns,q))
        jobs.append(p)
        p.start()
    #for p in jobs:
    #    p.join()
    #    print('joining results')
    print('checking results')

    results = [q.get() for j in jobs]
    print(results)
    AnalysisResultList.append(results)
    LaunchEntity['AnalysisResultList']=results

    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity
    
    jsonString = json.dumps(data)
     
    json.dump(data, open("ldb.json","w"))
    return 'Completed'


def LaunchAnalysisbyKeyNames(df2,KeyName,launchNewId,ruleset,cdecolumns,q):
    print('starting long running task')
    rules=[]    
    keyv=KeyName
    print(KeyName)
    
    print(launchNewId)
    rulesObject={}  
    resultsetDictlist=[]
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = val1.to_dict('index')
            count=count+1 
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = val.to_dict('index')


        count=count+1 
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        framenew = frame.where(pd.notnull(frame), 0)
        frame = frame.where(pd.notnull(frame), 'None')
        
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "",regex=False)
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                    
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                elif ru['rule'] =='Formula':
                    formula= ru['value']
                    print('formula')
                    print(KeyName)
                    fList=[]
                    oList=[] 
                    for valcol in formula:
                        fList.append(valcol['cde'])
                        if(valcol['operator']!='NULL'):
                            oList.append(valcol['operator'])
                    
                    if oList[0]=='-':
                        col=col
                        col1=fList[0]
                        col2=fList[1]
                        num_rows= len(framenew[(framenew[col1] - framenew[col2] == framenew[col] ) ])
                    elif oList[0]=='+':
                        col=col
                        col1=fList[0]
                        col2=fList[1]
                        num_rows= len(framenew[(framenew[col1] + framenew[col2] == framenew[col] ) ])
                    elif oList[0]=='*':
                        col=col
                        col1=fList[0]
                        col2=fList[1]
                        num_rows= len(framenew[(framenew[col1] * framenew[col2] == framenew[col] ) ])
                    elif oList[0]=='/':
                        col=col
                        col1=fList[0]
                        col2=fList[1]
                        num_rows= len(framenew[(framenew[col1] / framenew[col2] == framenew[col] ) ])
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=num_rows  
                    print(num_rows)                    
                    reftotalCount=len(framenew[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r7= framenew[(framenew[col1] - framenew[col2] != framenew[col] ) ]    
                    print(len(r7))                
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    data_dict = r7.to_dict('index')
        
                    resultsetdict7={}
                    resultsetdict7['resultset'] = resultId
                    resultsetdict7['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict7)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Accuracy'] = DictInconstraints
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict) 
    print('calling the saveresultset to s3')
    saveResultsets(resultsetDictlist,(launchNewId))  
   
    resultobject={}
    resultobject['launchId']=launchNewId    
    resultobject['keyName']=keyv
    resultobject['results']=rules
    print(resultobject)
    q.put(resultobject)

def LaunchAnalysisbyKeyNamesNew(df2,KeyName,launchNewId,ruleset,cdecolumns):
    print('starting long running task')
    rules=[]    
    keyv=KeyName
    print(KeyName)
    
    print(launchNewId)
    rulesObject={}  
    resultsetDictlist=[]
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = val1.to_dict('index')
            count=count+1 
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = val.to_dict('index')


        count=count+1 
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        frame = frame.where(pd.notnull(frame), 'None')
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "",regex=False)
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                     
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                  
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict) 
    print('calling the saveresultset to s3')
    saveResultsets(resultsetDictlist,(launchNewId))  
   
    resultobject={}
    resultobject['launchId']=launchNewId    
    resultobject['keyName']=keyv
    resultobject['results']=rules
    print(resultobject)
    return resultobject

  

def LaunchAnalysisbyKeyNamesthread(df2,KeyName,launchNewId,ruleset,cdecolumns,q):
    print('starting long running task')
    rules=[]    
    keyv=KeyName
    print(KeyName)
    
    print(launchNewId)
    rulesObject={}  
    resultsetDictlist=[]
    dfs= df2.groupby([KeyName])
    count=0
    for KeyName, frame in dfs:    
        Dict = {}    
        Dict[keyv] =KeyName   
        Dictconstraints = {}          
        Dictconstraints['value'] = 100-round(frame.isnull().stack().mean()*100,2)       
        #rulesList.append(Dict)
        nan_values = frame.isna()
        nan_columns = nan_values.any()
        columns_with_nan = frame.columns[nan_columns].tolist()
        nulls_list=[]
        for k in columns_with_nan:       
            DNull = {}            
            DNull['column'] =k
            DNull['nullcount'] =str(frame[k].isnull().sum())
            val1= frame[frame[k].isnull()]
            #resDf = val1.where(pd.notnull(val1), 'None')
            
            data_dict = val1.to_dict('index')
            count=count+1 
            resultId= launchNewId +'RS'+str(count)
            DNull['outlier']=resultId
            resultsetdict={}
            resultsetdict['resultset'] = resultId
            resultsetdict['results'] = data_dict
            #saveResultsets(data_dict,resultId)
            resultsetDictlist.append(resultsetdict)
            nulls_list.append(DNull)
        Dictconstraints['details']=nulls_list   
       

        Dict['completness'] = Dictconstraints
        
        dupCount=0
        duptotalCount=0
        dupPerc=0
        colList=cdecolumns#['AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT']
        farmenew= frame#[colList]       
         
        dupPerc= 100 -((farmenew.duplicated().mean())*100)
        nulls_list=[]
        DNull = {}            
        DNull['column'] =str(colList)
        DNull['outLiers'] = str(farmenew.duplicated().sum())        
        val= farmenew[farmenew.duplicated(subset=colList, keep=False)]
        #resDf1 = val.where(pd.notnull(val1), 'None')
            
        data_dict = val.to_dict('index')


        count=count+1 
        resultId= launchNewId +'RS'+str(count)
        DNull['outlier']=resultId
        resultsetdict1={}
        resultsetdict1['resultset'] = resultId
        resultsetdict1['results'] = data_dict
        #saveResultsets(data_dict,resultId)
        resultsetDictlist.append(resultsetdict1)
        nulls_list.append(DNull)
        DictUnconstraints = {}          
        DictUnconstraints['value'] = str(round(dupPerc, 2))
        DictUnconstraints['details']=nulls_list
        Dict['Uniqueness'] = DictUnconstraints
        value=[100] 
        vnulls_list=[]
        
        for r in ruleset:
            col=r['column']
            for ru in r['rules']:
                if ru['rule']=='DataType':
                    if ru['value']=='alphabets':
                        cntalnum=frame[col].str.isalnum().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append( percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='alphabets'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r3= frame[~frame[col].str.isalnum()] 
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r3.to_dict(),resultId)
                            #resDf2 = r3.where(pd.notnull(val1), 'None')
                            data_dict = r3.to_dict('index')
        
                            resultsetdict2={}
                            resultsetdict2['resultset'] = resultId
                            resultsetdict2['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict2)

                            vnulls_list.append(DNull)
                    elif ru['value']=='Numeric':
                        frame[col]=frame[col].astype(str) 
                        frame[col]=frame[col].str.replace(".", "",regex=False)
                        cntalnum=frame[col].str.isdigit().sum()
                        cnttotal=frame[col].count()
                        percen=((cntalnum)/(cnttotal))*100
                        value.append(percen)
                        if(cnttotal!=cntalnum):
                            DNull = {}            
                            DNull['column'] =col
                            DNull['ruleType']='DataType'
                            DNull['rulevalue']='Numeric'
                            DNull['ruleMismatchcount'] =str(cnttotal-cntalnum)
                            r4= frame[~frame[col].str.isdigit()]
                            count=count+1 
                            resultId= launchNewId +'RS'+str(count)
                            DNull['outlier']=resultId
                            #saveResultsets(r4.to_dict(),resultId)

                            #resDf3 = r4.where(pd.notnull(val1), 'None')
                            data_dict = r4.to_dict('index')
        
                            resultsetdict3={}
                            resultsetdict3['resultset'] = resultId
                            resultsetdict3['results'] = data_dict
                            #saveResultsets(data_dict,resultId)
                            resultsetDictlist.append(resultsetdict3)
                            vnulls_list.append(DNull)
                elif ru['rule']=='Length':
                    if (int(float(ru['value']))==frame[col].str.len().min()) and  (int(float(ru['value']))==frame[col].str.len().max()):
                        value.append(100)
                    else:
                        value.append(100-((((frame[col].str.len() !=int(float(ru['value']))).sum())/(len(frame[col])))*100) )
                        DNull = {}            
                        DNull['column'] =col                        
                        DNull['ruleType']='Length'
                        DNull['rulevalue']=ru['value']
                        DNull['ruleMismatchcount'] = str((frame[col].str.len() !=int(float(ru['value']))).sum())
                        r5= frame[(frame[col].str.len() !=int(float(ru['value'])))]                        
                        count=count+1 
                        resultId= launchNewId +'RS'+str(count)
                        DNull['outlier']=resultId
                        #saveResultsets(r5.to_dict(),resultId)

                        #resDf5 = r5.where(pd.notnull(val1), 'None')
                        data_dict = r5.to_dict('index')
        
                        resultsetdict5={}
                        resultsetdict5['resultset'] = resultId
                        resultsetdict5['results'] = data_dict
                        #saveResultsets(data_dict,resultId)
                        resultsetDictlist.append(resultsetdict5)

                        vnulls_list.append(DNull)  
                elif ru['rule']=='ReferenceCDE':
                    colname= ru['value'].split('-', 2)[2] 
                    referenceIdRule = ru['value'].split('-', 2)[1] 
                    referepath=""
                    for referobj in ReferenceObj:
                        if referobj['referenceId']==referenceIdRule:
                            referepath=referobj['referencePath']
                    
                    refpath=referepath
                    #refpath='s3://dquploads/Ref1-8D3NZAE.csv'
                    
                    file_ext= os.path.splitext(os.path.basename(refpath))[1]
                    print(refpath)
                    if(file_ext=='.csv'):
                        dfref = pd.read_csv(refpath)
                    elif(file_ext=='.xls'):
                        dfref = pd.read_excel(refpath)
                    elif(file_ext=='.xlsx'): 
                        dfref = pd.read_excel(refpath)
                    else:
                        dfref = pd.read_csv(refpath)  
                    refCount=0
                    reftotalCount=0
                    refPerc=0
                    refCount=(frame[col].isin(dfref[colname])).sum()                    
                    reftotalCount=len(frame[col])                    
                    refPerc= (refCount / reftotalCount)*100
                    
                    nulls_list=[]
                    DNull = {}            
                    DNull['column'] =col 
                    DNull['outLiers'] = str(reftotalCount- refCount)
                    r6= frame[~(frame[col].isin(dfref[colname]))]                    
                    count=count+1 
                    resultId= launchNewId +'RS'+str(count)
                    DNull['outlier']=resultId
                    #saveResultsets(r6.to_dict(),resultId)
                    #resDf6 = r6.where(pd.notnull(val1), 'None')
                    data_dict = r6.to_dict('index')
        
                    resultsetdict6={}
                    resultsetdict6['resultset'] = resultId
                    resultsetdict6['results'] = data_dict
                    #saveResultsets(data_dict,resultId)
                    resultsetDictlist.append(resultsetdict6)
                    nulls_list.append(DNull)
                    DictInconstraints = {}          
                    DictInconstraints['value'] = str(round(refPerc, 2))
                    DictInconstraints['details']=nulls_list
                    Dict['Integrity'] = DictInconstraints
                  
        res= sum(value)/len(value)
        Dictvconstraints = {}          
        Dictvconstraints['value'] = (round(res, 2))
        Dictvconstraints['details']=vnulls_list
        Dict['Validity'] = Dictvconstraints
        rules.append(Dict) 
        saveResultsets(resultsetDictlist,(launchNewId))  
        break
    resultobject={}
    resultobject['launchId']=launchNewId    
    resultobject['keyName']=keyv
    resultobject['results']=rules
    print(resultobject)
    q.put(resultobject)


@app.route('/api/uploadSource', methods=['POST'])
def uploadSource(): 
    uploaded_files = request.files.getlist("file[]")
    content =json.loads(request.form.get('data'))                     
    sourceId= content['sourceId']
    rulesetId= content['rulesetId']
    isMultiSource= content['isMultiSource']
    multiSourceKey= content['multiSourceKey']
    uploadDate= content['uploadDate']
    uploadReason = content['uploadReason']
    uploadTime= content['uploadTime']
    sourcedetails= content['sourceObj']
    
    settings= content['settings']
    sourceFilename = sourcedetails['sourceFileName']
    sourceCatColumns = sourcedetails['categorialColumns']
    sourcePath=''
    keyNametoLaunch= sourceCatColumns[0]   
    newuploadId=sourceId+'U'+ str(int(getUploadMaxId(sourceId))+1)
    sourcename = os.path.splitext(sourceFilename)[0]
    print(sourcename)
    sourceFilename=newuploadId + sourcename


    LaunchEntityRaw= json.loads(GetAEntireLaunchEntityDB(sourceId))

    EntireSourceObj= LaunchEntityRaw['EntireSourceObj']
    EntireSourceList= LaunchEntityRaw['EntireSourceList']  
    uploadsObject= EntireSourceObj['uploads']
    
    print(uploadReason)
    if(uploadReason==''):
        if(isMultiSource=="Yes"):
            if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                        multisrObj= item['multiSourcePath']
                        if multisrObj['multiSourceKey']==multiSourceKey:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file for this multisource key is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
        else:
                if(settings['frequency']=="Daily"):
                    for item in uploadsObject:
                            payloadDate =  datetime.datetime.strptime(uploadDate,"%Y-%m-%dT%H:%M:%S.%fZ")
                            actualDate = datetime.datetime.strptime(item['uploadDate'],"%Y-%m-%dT%H:%M:%S.%fZ")

                            if payloadDate.date()==actualDate.date():
                                content['errorMsg']= 'The file is already uploaded for the current date'
                                content['errorflag']= 'True'
                                content['errorCode']= '102'
                                return json.dumps(content)
                        
    for file in uploaded_files:    
        filename = secure_filename(file.filename)
        if filename != '':
                print(sourceFilename)
                file_ext = os.path.splitext(filename)[1]
                sourceFilename=sourceFilename+file_ext
                print(file_ext)
                if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                    dli=[]
                    sourcePath=sourceFilename
                    file.save(sourcePath)
                    if(file_ext=='.csv'):
                        df = pd.read_csv(sourcePath)
                    elif(file_ext=='.xls'):
                        df = pd.read_excel(sourcePath)
                    elif(file_ext=='.xlsx'): 
                        df = pd.read_excel(sourcePath)
                    else:
                        df = pd.read_csv(sourcePath)
                    dli= list(df.columns.values)
                    sourcedetails['availableColumns'] = dli   
                    dli=dli.sort() 
                    sourceavailableColumns=sourcedetails['availableColumns']
                    sourceavailableColumns=sourceavailableColumns.sort() 
                    if dli != sourceavailableColumns: 
                        content['errorMsg']= 'The file headers does not match with the configured source'
                        content['errorflag']= 'True'
                        content['errorCode']= '103'
                        return json.dumps(content)
    resultContent={}

    resultContent['launchId']=newuploadId
    resultContent['uploadId']=newuploadId
    resultContent['rulesetId']=rulesetId
    resultContent['sourceFileName']=sourcePath
    resultContent['sourcePath']=sourcePath
    resultContent['uploadDate']=uploadDate
    resultContent['uploadTime']=uploadTime
    resultContent['isexpecteddate']='Yes'
    resultContent['reference']=[]
    resultContent['launchAnalysis']={}
    resultList=[]
    
    resultContent['AnalysisResultList']=[]
    resultContent['isMultiSource']=isMultiSource
    resultContent['multiSource']=[]
    #multiSourcePaths=[]
    if(isMultiSource=="Yes"):
        mpath={}
        mpath['sourcePath']=  sourcePath
        mpath['multiSourceKey']=multiSourceKey  
        #multiSourcePaths.append(mpath)
        resultContent['multiSourcePath']=mpath
    content['uploadId']=newuploadId
    content['sourceFilename']=sourceFilename
    uploadsObject.append(resultContent)
    EntireSourceObj['uploads']=uploadsObject
    EntireSourceObj['sourceId']=sourceId
    EntireSourceList.append(EntireSourceObj)
    data={}
    data['LaunchAnalysis']=EntireSourceList
    jsonString= json.dumps(data)
    json.dump(data, open("ldb.json","w"))
    content1={}
    content1['errorMsg']= ''
    content1['errorflag']= 'False'
    content1['responseObj']= jsonString

    tempDict={}
    tempDict['sourceId']= sourceId
    tempDict['uploadId'] =newuploadId
    tempDict['source']= sourcedetails
    uploadSourceDetail= {} 
    uploadSourceDetail['uploadId'] =newuploadId
    uploadSourceDetail['isMultiSource'] =isMultiSource
    uploadSourceDetail['uploadDate'] =uploadDate
    uploadSourceDetail['uploadTime'] =uploadTime
    uploadSourceDetail['sourceFileName']=sourceFilename
    tempDict['rules']=[]
    tempDict["UploadsHistory"]= []
    tempDict['recentsourceUpload'] = uploadSourceDetail
    

    inputcontent1={}
    inputcontent1['sourceId']=sourceId
    inputcontent1['uploadId']=newuploadId
    inputcontent1['rulesetId']=rulesetId
    inputcontent1['keyname']=sourceCatColumns

  
      

    def long_running_task(**kwargs):
        inputcontent = kwargs.get('post_data', {})
        print("Starting long task")
        print("Your params:", inputcontent)
        rules=[] 
        time.sleep(1)   
        sourceId = inputcontent['sourceId']
        rulesetId = inputcontent['rulesetId']
        KeyName = inputcontent['keyname']
        uploadId=inputcontent['uploadId']
        #LaunchAnalysisbyParamfromFnCall(sourceId,rulesetId,KeyName,uploadId)
        LaunchAnalysisthread(sourceId,rulesetId,KeyName,uploadId)      
        print('Completed the main thread function')
    tempDict['stuats'] ='started'
    thread = threading.Thread(target=long_running_task, kwargs={
                    'post_data': inputcontent1})
    thread.start()
    
    return json.dumps(tempDict)


def LaunchAnalysisthreadNew(sourceId,rulesetId,KeyNameList,uploadId):
    print('starting long running task')
    rules=[]    
    #keyv=KeyName
    sourcepath=''
    cdecolumns=[]
    # JSON file 
    rulesObject={}  
    resultsetDictlist=[]
    datarules= json.loads(GetAEntityDB(sourceId))
    sourceObj= datarules['Analysis'] 
    AnalysisObj=sourceObj['source']

    LaunchEntityRaw= json.loads(GetDBentitiesforLaunch(sourceId,uploadId))
    #DBList= json.loads(removeALaunchEntityDB(sourceId))
    DBListEntity=LaunchEntityRaw['SourceremovedList'] 

    #LaunchEntityRaw= json.loads(GetALaunchEntityDB(sourceId,uploadId))

    LaunchEntity=LaunchEntityRaw['exactuploadList'] 
    EntireSourceList=LaunchEntityRaw['exactSourceList'] 
    sourcepath= LaunchEntity['sourcePath']
    multiSourceKey=""
    if(LaunchEntity['isMultiSource']=='Yes'):
        multiSources= LaunchEntity['multiSourcePath']
        sourcepath=multiSources['sourcePath']
        multiSourceKey=multiSources['multiSourceKey']
    else:
        sourcepath= LaunchEntity['sourcePath']
        multiSourceKey=""
    AnalysisResultList= LaunchEntity['AnalysisResultList']
    launchMaxId= LaunchEntityRaw['launchMaxId']
    existingLaunchSourceList=[] 
    for obj1 in EntireSourceList["uploads"]: 
        if obj1["uploadId"]!=uploadId:
            existingLaunchSourceList.append(obj1)

    launchNewId= sourceId + 'L' + str(int(launchMaxId)+1) 
    ReferenceObj=sourceObj['reference']
    for obj1 in sourceObj["rules"]:            
        if obj1["rulesetId"]==rulesetId:
            cdecolumns=    obj1["selectedColumns"]    
            ruleset= obj1["ruleset"]
    file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
    if(file_ext=='.csv'):
        df = pd.read_csv(sourcepath)
    elif(file_ext=='.xls'):
        df = pd.read_excel(sourcepath)
    elif(file_ext=='.xlsx'): 
        df = pd.read_excel(sourcepath)
    else:
        df = pd.read_csv(sourcepath)   

    df2 = pd.DataFrame(df, columns=cdecolumns)   
    #df2 = df1.where(pd.notnull(df1), 'None')
    i=0
    results=[]
    for keyItem in KeyNameList:
        i=i+1
        launchNewId=sourceId + 'L' + str(int(launchMaxId)+i)
        resobjlist= LaunchAnalysisbyKeyNamesNew(df2,keyItem,launchNewId,ruleset,cdecolumns)
        results.append(resobjlist)
   
    Name= uploadId +'Results'
    LaunchEntity['AnalysisResultList']=Name
    
    saveResultsets(results,Name)
    existingLaunchSourceList.append(LaunchEntity)
    EntireSourceList["uploads"]=existingLaunchSourceList
    data={}
    DBListEntity.append(EntireSourceList)
    data['LaunchAnalysis'] = DBListEntity
    jsonString = json.dumps(data)
    json.dump(data, open("ldb.json","w"))
    return 'Completed'

@app.route('/getSourcePreview', methods=['GET'])
def getSourcePreview(): 
        sourceId = request.args.get('sourceId')      
        with open('db.json', 'r') as openfile:
            json_object = json.load(openfile)
            
        data = json_object       
       
        for obj in data['Analysis']:  
                if obj["sourceId"]==sourceId:  
                   AnalysisObj=obj['source']
                   break   
        sourcepath= AnalysisObj['templateSourcePath'] 
        #path= 's3://dquploads/Source.csv' 
        file_ext= os.path.splitext(os.path.basename(sourcepath))[1]
        if(file_ext=='.csv'):
            df = pd.read_csv(sourcepath)
        elif(file_ext=='.xls'):
            df = pd.read_excel(sourcepath)
        elif(file_ext=='.xlsx'): 
            df = pd.read_excel(sourcepath)
        else:
            df = pd.read_csv(sourcepath)
        df=df.head()
        resDf = df.where(pd.notnull(df), 'None')
        data_dict = resDf.to_dict('index')
        content={}
        content['sourcePreview']=data_dict
        jsonString = json.dumps(content)     
        return jsonString


@app.route('/api/getPreview', methods=['POST'])
def getPreview(): 
    uploaded_files = request.files.getlist("file[]")
    for file in uploaded_files:                
                filename = secure_filename(file.filename)
                if filename != '':
                    file_ext = os.path.splitext(filename)[1]
                    if file_ext in app.config['UPLOAD_EXTENSIONS'] :
                        dli=[]
                        
                        if(file_ext=='.csv'):
                            df = pd.read_csv(file)
                        elif(file_ext=='.xls'):
                            df = pd.read_excel(file)
                        elif(file_ext=='.xlsx'): 
                            df = pd.read_excel(file)
                        else:
                            df = pd.read_csv(file)

                        df=df.head()
                        resDf = df.where(pd.notnull(df), 'None')
                        data_dict = resDf.to_dict('index')
                        content={}
                        content['sourcePreview']=data_dict
                        jsonString = json.dumps(content)     
                        return jsonString

@app.route('/corelationrelationships', methods=['POST'])
def corelationrelationships():  
    n=50
    df = pd.read_csv("flights_extra.csv")
    df= df.head(int(len(df)*(n/100)))
    df_clean = df.select_dtypes(exclude=['object'])
    print(df_clean)
    df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]
    print(df_clean)
    corr_mat = df_clean.corr(method='spearman', min_periods=1000)
    upper_corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(np.bool)) 
    response_vars_list = list(upper_corr_mat.columns)
    print(response_vars_list)
    unique_corr_pairs = upper_corr_mat.unstack().dropna() 
    print(unique_corr_pairs)
    sorted_mat   = unique_corr_pairs.sort_values(ascending=False) 
    df_corr_sort = pd.DataFrame(data = sorted_mat, index = sorted_mat.index, columns = ['Corr_Coeff'])
    resp_var=  'ARRIVAL_DELAY'
    df_respvar1 = df_corr_sort.loc[resp_var].sort_values(by = ['Corr_Coeff'], ascending=False)
    dependent_features = list(df_respvar1.index)
    
    nr_combinations = 2 # Specify the length of combinations to be generated from the list
    dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
    df_relationships = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
    df_relationshipsnew = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
    #for resp_var in response_vars_list:
    print(dep_vars_pairwise_combinations)
    formulaList=[]
    if 1==1:  
        rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
        # resp_var_list, dep_var1_list, dep_var2_list,rel_status_list = [], [], [], []
        for pair in dep_vars_pairwise_combinations:
            df_temp = df_clean[[resp_var, pair[0], pair[1]]]
            df_len_threshold = int(len(df_temp)/1.6) # Threshold for valid entries set at 60% of the dataframse size
            print('\nResponse_var:',resp_var,';\tDependent_var1:',pair[0],';\tDependent_var2:',pair[1])
            # Check if the dependant variables have a subtractive relationship for every entry in the dataframe
            rel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] - df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
            if (rel_true_array.sum() > df_len_threshold):
                print('Nr. of TRUE entries:',rel_true_array.sum(),'; \tDataFrame Size:',len(df_temp))
                rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
                rel_dict['response_var'].append(resp_var)
                rel_dict['dependent_var1'].append(pair[0])
                rel_dict['dependent_var2'].append(pair[1])
                rel_dict['relationship'].append('SUBTRACTION')
                rel_dict['entries'].append(rel_true_array.sum())
                rel_dict['percentOfEntries'].append(100*np.round(rel_true_array.sum()/len(df_temp), 2))
                #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                formulaDict={}
                formulaDict['cde']=pair[0]
                formulaDict['operator']='NULL'
                formulaList.append(formulaDict)

                formulaDict1={}
                formulaDict1['cde']=pair[1]
                formulaDict1['operator']='-'
                formulaList.append(formulaDict1)
                print('SUBTRACTION Relationship Identified in ~', 100*np.round(rel_true_array.sum()/len(df_temp), 2) ,'% of the entries')
                      
                df_relationships = df_relationships.append(pd.DataFrame(data = rel_dict), ignore_index=True)
    print(df_relationships) 
    f= df_relationships.to_dict('records')          
    d={}          
    d['relationship'] =f
    d['value']=formulaList
    return d

 
def getCorelationrelationships(df,resp_var):  
    d={}  
    formulaList=[]      
    d['value']=formulaList
    try:
        n=30
        df=df.head(int(len(df)*(n/100)))
        df_clean = df.select_dtypes(exclude=['object'])
        
        df_clean = df_clean.loc[:,df_clean.apply(pd.Series.nunique) != 1]
        
        corr_mat = df_clean.corr(method='spearman', min_periods=1000)
        upper_corr_mat = corr_mat.where(np.triu(np.ones(corr_mat.shape), k=1).astype(np.bool)) 
        response_vars_list = list(upper_corr_mat.columns)
        unique_corr_pairs = upper_corr_mat.unstack().dropna() 
        
        sorted_mat   = unique_corr_pairs.sort_values(ascending=False) 
        df_corr_sort = pd.DataFrame(data = sorted_mat, index = sorted_mat.index, columns = ['Corr_Coeff'])
        #resp_var=  'DEPARTURE_DELAY'
        df_respvar1 = df_corr_sort.loc[resp_var].sort_values(by = ['Corr_Coeff'], ascending=False)
        dependent_features = list(df_respvar1.index)
        
        nr_combinations = 2 # Specify the length of combinations to be generated from the list
        dep_vars_pairwise_combinations = list(combinations(dependent_features, nr_combinations))
        df_relationships = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
        df_relationshipsnew = pd.DataFrame(columns = ['response_var','dependent_var1','dependent_var2','relationship'])
        #for resp_var in response_vars_list:
        print(dep_vars_pairwise_combinations)
        formulaList=[]
        if 1==1:  
            rel_dict = {'response_var':[],'dependent_var1':[],'dependent_var2':[],'relationship':[],'entries':[],'percentOfEntries':[]}
            # resp_var_list, dep_var1_list, dep_var2_list,rel_status_list = [], [], [], []
            for pair in dep_vars_pairwise_combinations:
                df_temp = df_clean[[resp_var, pair[0], pair[1]]]
                df_len_threshold = int(len(df_temp)/1.6) # Threshold for valid entries set at 60% of the dataframse size
                print('\nResponse_var:',resp_var,';\tDependent_var1:',pair[0],';\tDependent_var2:',pair[1])
                # Check if the dependant variables have a subtractive relationship for every entry in the dataframe
                rel_true_array = np.array([True if (df_temp.loc[i,resp_var] == df_temp.loc[i,pair[0]] - df_temp.loc[i,pair[1]]).any() else False for i in range(len(df_temp))])
                if (rel_true_array.sum() > df_len_threshold):
                
                    #df_relationshipsnew = df_relationshipsnew.append(pd.DataFrame(data = rel_dict), ignore_index=True)
                    formulaDict={}
                    formulaDict['cde']=pair[0]
                    formulaDict['operator']='NULL'
                    formulaList.append(formulaDict)

                    formulaDict1={}
                    formulaDict1['cde']=pair[1]
                    formulaDict1['operator']='-'
                    formulaList.append(formulaDict1)
                
                
        d={}        
        d['value']=formulaList
        return d
    except Exception as e:	    
        return d