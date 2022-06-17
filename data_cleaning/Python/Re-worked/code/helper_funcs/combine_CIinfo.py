def combine_CIinfo(path, respdf, cidf = pd.DataFrame(), ci_cols_to_include = [], interaction_type_list = 1):
    
    #read in content info file
    if(cidf.empty == True):
        cidf = pd.read_csv(path + 'contentItemInfo.tsv', sep = '\t')
    
    #turn all initial letters in column headers to lowercse
    cidf.columns = [col[0].lower()+col[1:] for col in cidf.columns]
    
    cidf = cidf.drop_duplicates()
    
    cidf = cidf[cidf['interactionTypeId'].isin(interaction_type_list)]
    
    columnsToAdd = ['contentItemName', 'contentItemId']
    columnsToAdd = columnsToAdd + ci_cols_to_include
    
    columnsdf = cidf[columnsToAdd]
    
    outputdf = pd.merge(respdf, columnsdf)
    
    return outputdf