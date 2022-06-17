import libs

#all calculated columns making will be present here
def make_calc_columns(respExcl = libs.pd.DataFrame()):
    #making actual num ques column
    respExcl['actual_num_ques'] = respExcl.groupby(by = ['activity_id', 'activity_name'])['content_item_name'].transform('nunique')
    respExcl['attempted'] = libs.np.where(respExcl['raw_response'].isnull(), False, respExcl['raw_response']!=0)

    #while in recoding we manipulate response & score, so we duplicate them and use the duplicates for manipulation keeping
    #original cols as is
    respExcl['orig_response'] = respExcl['raw_response']
    respExcl['orig_score'] = respExcl['score']