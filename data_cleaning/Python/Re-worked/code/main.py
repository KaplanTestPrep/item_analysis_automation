import libs


def making_response_df(response_df, activity_df, content_df, test_map = libs.pd.DataFrame(), return_summary = False, qbank = False):
    #for all other analysis except NGN items, get cor_ans, ci_old_versions from response and by default these flags are made true
    response_df, cor_ans_resp, ci_old_keys = libs.response_df_cleaning(response_df, get_cor_ans = True, ci_old_version = True)
    activity_df = libs.activity_df_cleaning(activity_df)

    #To get cor_ans, ci_old_version, latest_item_details(like only one row for item)-- these used in NGN item cleaning, from content_df
    #Use below
    #to get all the three but content_df with even multiple rows
    #content_df, cor_ans, ci_old_keys = content_df_cleaning(content_df, get_latest_item_details = False, get_cor_ans = True, ci_old_version = True)
    
    #to get each row for each item with latest details
    #content_df = content_df_cleaning(content_df, get_latest_item_details = True, get_cor_ans = False, ci_old_version = False)
    
    #for rest of the data cleaning, we don't need cor_ans, old_version_keys
    content_df, cor_ans_cidf = libs.content_df_cleaning(content_df, get_latest_item_details = False, get_cor_ans = True, ci_old_version = False)

    response_df = libs.pd.merge(response_df, activity_df, on = ['source_system', 'history_db_id', 'student_id', 'activity_id'], how = 'inner')

    if(response_df.empty):
        print('Response df became empty after merging with activity_df, Check!!', 'alert-danger')
        
    else:
        print(f'Size of response_df after merging with activity_df: {response_df.shape}\
            \n#students in response_df after merging with activity_df : {response_df.student_id.nunique()}\
            \n#activities in response_df after merging with activity_df : {response_df.activity_id.nunique()}', 'message')
    
    #removing corr_ans_cidf from content_df as we also merge corr_ans df from content_df
    response_df = libs.pd.merge(response_df, content_df.drop(columns = ['corr_ans_cidf']), on = ['content_item_id', 'content_item_name'], how = 'inner')

    if(response_df.empty):
        print('Response df became empty after merging with content_df, Check!!', 'alert-danger')
    else:
        print(f'Size of response_df after merging with content_df: {response_df.shape}\
            \n#students in response_df after merging with content_df : {response_df.student_id.nunique()}\
            \n#activities in response_df after merging with content_df : {response_df.activity_id.nunique()}\
            \n#content items in response_df after merging with content_df : {response_df.content_item_name.nunique()}', 'message')
        

    #below is to enrich response column if it is not populated well
    #also making a common column for correct answer
    #merging correct answer column of cor_ans_resp with response df
    response_df = libs.pd.merge(response_df, cor_ans_resp[['content_item_name', 'corr_ans_respdf']], on = ['content_item_name'], how = 'left')
    response_df['correct_answer'] = response_df['corr_ans_respdf']

    #if by chance not found correct answer from response df then we rely on correct_answer found from content_item_df
    #merging cor_ans found from content_df
    response_df = libs.pd.merge(response_df, cor_ans_cidf[['content_item_name', 'corr_ans_cidf']], on = ['content_item_name'], how = 'left')

    #after below correct_answer column will be totally full
    response_df.loc[response_df['correct_answer'].isnull(), 'correct_answer'] = response_df[response_df['correct_answer'].isnull()]['corr_ans_cidf']

    #This should be run after making correct_answer column
    #filling response_df based on response status
    response_df.loc[(response_df['response_status']=='responded') & (response_df['score']==0) & (response_df['raw_response'].isnull()), 'raw_response'] = 'unknown'
    response_df.loc[(response_df['response_status']=='responded') & (response_df['score']==1) & (response_df['raw_response'].isnull()), 'raw_response'] = response_df[(response_df['response_status']=='responded') & (response_df['score']==1) & (response_df['raw_response'].isnull())]['correct_answer']
    #manipulating response_status if its null based on respnose & score
    response_df.loc[(response_df['response_status'].isnull()) & (response_df['raw_response'].isnull()) & (response_df['score'].isnull()), ['response_status', 'score']] = ['not-reached', 0]

    #merging test_map with response_df based on analysis type
    if(test_map.empty==False and (qbank == False or qbank == True)):
        test_map['activity_name'] = test_map['activity_name'].str.lower()

        print(f'Size of response_df before merge with test_map: {str(response_df.shape)}\
            \n#students in response_df before merging with test_map : {response_df.student_id.nunique()}\
            \n#activities in response_df before merging with test_map : {response_df.activity_id.nunique()}', 'message')
        
        response_df = libs.pd.merge(response_df, test_map, on = ['activity_name'], how = 'inner')

        if(response_df.empty):
            print('Response df became empty after merging with test_map, Check!!', 'alert-danger')
        
        else:
            print(f'Size of response_df after merging with test_map: {response_df.shape}\
                \n#students in response_df after merging with test_map : {response_df.student_id.nunique()}\
                \n#activities in response_df after merging with test_map : {response_df.activity_id.nunique()}', 'message')


    #cleaning columns of response_df finally
    response_df = libs.response_df_col_cleaning(response_df)

    #summary to from response df to make test map
    response_summary = response_df[response_df['activity_status'].str.lower() == 'completed'].groupby(['activity_name', 'student_id'], as_index = False, dropna = False).agg(num_responses = ('content_item_name', 'nunique'))\
        .groupby(['activity_name'], dropna = False, as_index = False).agg(min_resp = ('num_responses', 'min'),
                                            median_resp = ('num_responses', 'median'),
                                            max_resp = ('num_responses', 'max'),
                                            num_users = ('num_responses', 'count'))
        
        
    #next is to call the item_cleaning function on finally made response_df
    return response_df, response_summary, cor_ans_resp, ci_old_keys

#merging with test_map if its not qbank analysis

data_path = 'C:\\Users\\VImmadisetty\\Downloads\\Projects\\DRCR Automation\\Project_1\\data\\'
activity_df = libs.pd.read_csv(data_path + 'activity_info.tsv', sep='\t', parse_dates = ['timestamp_created', 'timestamp_completed'])
response_df = libs.pd.read_csv(data_path + 'response_info.tsv', sep='\t', parse_dates = ['item_submitted_timestamp'])
content_df = libs.pd.read_csv(data_path + 'content_info.tsv', sep='\t', parse_dates = ['last_modified'])

response_df, response_summary, cor_ans_resp, ci_old_keys = making_response_df(response_df, activity_df, content_df, test_map = libs.pd.DataFrame(), qbank = False)

cor_ans_resp.to_csv(data_path + 'cor_ans_resp.csv', index = False)
ci_old_keys.to_csv(data_path + 'ci_old_keys.csv', index = False)

#tempate for test_map
#test_map = pd.DataFrame()
#test_map['template_id'] = np.nan
#test_map['activity_name'] = response_summary['sequenceName']

#test_map['section_name'] = np.nan

#test_map['num_ques'] = [90]
#test_map['response_threshold'] = 0.75
#test_map['minutes_allowed'] = [108]

#template for field_test_items
#field_test_items['content_item_name']

print(response_summary)