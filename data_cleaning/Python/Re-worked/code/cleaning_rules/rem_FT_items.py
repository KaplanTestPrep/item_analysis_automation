import libs

def rem_FT_items(respExcl = libs.pd.DataFrame(), field_test_items = libs.pd.DataFrame()):
    if (field_test_items.empty == False):
        respExcl['FT'] = respExcl['content_item_name'].isin(field_test_items['content_item_name'])
        respExcl = respExcl[respExcl['FT']==False]
        
    else:
        print('No Field test items df provided\n')
    
    return respExcl