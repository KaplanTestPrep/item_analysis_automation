import libs

def response_df_cleaning(response_df, get_cor_ans = True, ci_old_version = True):
####Working on response_df
    response_df['item_submitted_timestamp'] = libs.pd.to_datetime(response_df['item_submitted_timestamp'], errors = 'coerce')
    response_df.rename(columns = {'section_title':'section_name',
                             'milliseconds_used' : 'm_sec_used',
                             'is_scored' : 'scored',
                             'scored_response' : 'score',
                             'item_status': 'response_status',
                             'item_section_position' : 'display_seq'}, inplace = True)

    response_df.drop(columns = ['item_position', 'field_test',
                           'milliseconds_review_total', 'milliseconds_review_explanation'], inplace = True)
    
    response_df['source_system'] = libs.np.where(libs.pd.isnull(response_df['source_system']), response_df['source_system'], response_df['source_system'].astype('str').str.lower())

    response_df['history_db_id'] = libs.np.where(libs.pd.isnull(response_df['history_db_id']), response_df['history_db_id'], response_df['history_db_id'].astype('str').str.lower())

    #making student_id truly str
    response_df['student_id'] = libs.np.where(libs.pd.isnull(response_df['student_id']), response_df['student_id'], response_df['student_id'].astype('str').str.lower())

    #making activity_id truly string
    response_df['activity_id'] = libs.np.where(libs.pd.isnull(response_df['activity_id']), response_df['activity_id'], response_df['activity_id'].astype('str').str.lower())

    #making content_item_name lower case in response_df to merge with content_item_info
    response_df['content_item_name'] = libs.np.where(libs.pd.isnull(response_df['content_item_name']), response_df['content_item_name'], response_df['content_item_name'].astype('str').str.lower())

    #making cor_ans df from response_df file
    if(get_cor_ans):
        item_ver = response_df[((response_df['score']==1) | (response_df['score']>1)) & (response_df['raw_response']!=0) & (response_df['raw_response'].notnull())][['content_item_id', 'content_item_name', 'raw_response', 'item_submitted_timestamp']].drop_duplicates().sort_values(by=['content_item_name'], ignore_index = True)
        cor_ans = item_ver[item_ver.groupby(by=['content_item_name'])['item_submitted_timestamp'].rank(method = 'first', ascending = False)==1][['content_item_id', 'content_item_name', 'raw_response', 'item_submitted_timestamp']]
        cor_ans.rename(columns = {'raw_response':'corr_ans_respdf'}, inplace = True)
    
    if(ci_old_version):
        item_ver = response_df[((response_df['score']==1) | (response_df['score']>1)) & (response_df['raw_response']!=0) & (response_df['raw_response'].notnull())][['content_item_id', 'content_item_name', 'raw_response', 'item_submitted_timestamp']].drop_duplicates().sort_values(by=['content_item_name'], ignore_index = True)
        ci_old_keys = libs.pd.merge(item_ver.rename(columns = {'raw_response':'corr_ans_respdf'}).drop(columns = ['item_submitted_timestamp']).drop_duplicates(), cor_ans.drop(columns = ['content_item_id', 'item_submitted_timestamp']), on = ['content_item_name', 'corr_ans_respdf'], how = 'outer', indicator = True)
        ci_old_keys = ci_old_keys[ci_old_keys['_merge']=='left_only'].drop(columns = ['_merge']).rename(columns={'corr_ans_respdf':'raw_response'})

    if(get_cor_ans == True and ci_old_version == False):
        return response_df, cor_ans
    elif(get_cor_ans == False and ci_old_version == True):
        return response_df, ci_old_keys
    elif(get_cor_ans == True and ci_old_version == True):
        return response_df, cor_ans, ci_old_keys
    elif(get_cor_ans == False and ci_old_version == False):
        return response_df