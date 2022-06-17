import libs

#making below code thinking that content_item_id is unique
def rem_CI_old_keys(respExcl = libs.pd.DataFrame(), CI_old_keys = libs.pd.DataFrame()):
    if (respExcl.empty == False and CI_old_keys.empty == False):
        
        for row, val in enumerate(CI_old_keys['contentItemName']):
            respExcl = libs.recode_as_omitted(respExcl,
                                        omit_condition = ((respExcl['content_item_id']==CI_old_keys['content_item_id'].loc[row]) & 
                                                          (respExcl['raw_response'].fillna('None')==CI_old_keys['raw_response'].fillna('None').loc[row])))
    else:
        print('One of respExcl or CI_old_keys df is empty')
    
    return respExcl