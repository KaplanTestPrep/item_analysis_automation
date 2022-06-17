import libs

def rem_act_w_impo_timing(respExcl = libs.pd.DataFrame()):
    act_to_exclude = respExcl[respExcl['m_sec_used']<0]['activity_id'].unique()

    if(len(act_to_exclude)>0):
        respExcl = respExcl[~respExcl['activity_id'].isin(act_to_exclude)]
    
    return respExcl