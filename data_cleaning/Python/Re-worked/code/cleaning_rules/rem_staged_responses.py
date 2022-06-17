import libs

def rem_staged_responses(respExcl = libs.pd.DataFrame()):
    respExcl = respExcl[respExcl['content_item_id']!=-1] # these are staged records and do not represent a question that was viewed
    return respExcl