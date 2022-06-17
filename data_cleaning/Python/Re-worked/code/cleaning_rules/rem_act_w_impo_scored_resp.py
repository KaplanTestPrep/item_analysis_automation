import libs

def rem_act_w_impo_scored_resp(respExcl = libs.pd.DataFrame()):
    # Excluding activities that have weird response records - scored as correct without a response
    #we have already corrected resposes with status 'responded', score =1, response = null
    act_to_exclude = respExcl[((respExcl['response_status']!='responded') | (respExcl['response_status'].isnull())) & (respExcl['score']==1) & ((respExcl['raw_response'].isnull()) | (respExcl['raw_response'].notnull()))]['activity_id'].unique()

    if(len(act_to_exclude)>0):
        respExcl = respExcl[~respExcl['activity_id'].isin(act_to_exclude)]
    
    return respExcl