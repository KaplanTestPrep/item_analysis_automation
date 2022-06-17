import libs

def rem_act_w_dup_CIs(respExcl = libs.pd.DataFrame()):
    act_to_exclude = respExcl[respExcl['content_item_id']!=-1].copy()
    #getting counts of multi-appeared items with the help of display_seq
    #get items appeared more than once using display_seq
    #filter them from act_to_exclude df and filter for activity_id column
    act_to_exclude = act_to_exclude.set_index(['content_item_name']).loc[act_to_exclude.groupby(by=['activity_id', 'content_item_name'])['display_seq'].agg('nunique')>1]['activity_id'].unique()
    
    if(len(act_to_exclude) > 0):
        respExcl = respExcl[~respExcl['activity_id'].isin(act_to_exclude)]
    
    return respExcl