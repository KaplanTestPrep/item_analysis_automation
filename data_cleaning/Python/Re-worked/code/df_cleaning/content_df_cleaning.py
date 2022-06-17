import libs

#getting latest details is not always the case, only in NGN_items_Cleaning is used: correct_answer is used from this file
#For the rest, we usually don't need any details from content_df: correct_answer is made using respose_df data
#better to make two versions here one for NGN_items_Cleaning and the other for the rest
#Also keep content_item_id, as this helps in identifying versions of item and remember in no cleaning rule, you should use counts in group by as during joining
#various files, rows will get exploded.
def content_df_cleaning(content_df, get_latest_item_details = False, get_cor_ans = False, ci_old_version = False):
    ####Working on content_item_df
    content_df.rename(columns = {'correct_answer':'corr_ans_cidf',
                            'last_modified' : 'item_last_modified'}, inplace = True)

    content_df[['content_item_name', 'content_item_type',
        'interaction_type_name', 'source_system']] = content_df[['content_item_name', 'content_item_type',
                                                                     'interaction_type_name', 'source_system']].apply(lambda x: libs.np.where(libs.pd.isnull(x), x, x.astype(str).str.lower()))

    content_df = content_df[['content_item_id', 'content_item_name', 'content_item_type', 'interaction_type_name', 'count_choices', 'corr_ans_cidf', 'item_last_modified']].drop_duplicates(ignore_index = True)
    
    if(ci_old_version):
        ci_old_keys = content_df[content_df.groupby(by=['content_item_name'])['item_last_modified'].rank(method = 'first', ascending=False)>1][['content_item_id', 'content_item_name', 'corr_ans_cidf', 'last_modified']]
    
    #getting latest details for each item or making content_df to contain each for only each item
    #From content_item_df, we'll get only latest data for count_choices, correct_answer
    #we'll make correct answer columns again based on response_df

    #From below we get only latest item details and like single row for each item.
    #But this may leads to eliminating rows with older version of item when joining with response_df, as it can has various versions of item
    if(get_latest_item_details):
        content_df = content_df[content_df.groupby(by=['content_item_name'])['item_last_modified'].rank(method = 'first', ascending=False)==1]
    
    if(get_cor_ans):
        cor_ans = content_df[content_df.groupby(by=['content_item_name'])['item_last_modified'].rank(method = 'first', ascending=False)==1][['content_item_id', 'content_item_name', 'corr_ans_cidf']]

    if(get_cor_ans == True and ci_old_version == False):
        return content_df, cor_ans
    elif(get_cor_ans == False and ci_old_version == True):
        return content_df, ci_old_keys
    elif(get_cor_ans == True and ci_old_version == True):
        return content_df, cor_ans, ci_old_keys
    elif(get_cor_ans == False and ci_old_version == False):
        return content_df