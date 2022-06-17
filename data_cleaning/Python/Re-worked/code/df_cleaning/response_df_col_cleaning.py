import libs

def response_df_col_cleaning(response_df):
    #Below will avoid null values to getting converted to strings and rest will be turned to string and lower case
    response_df[['section_name', 'content_item_id', 'content_item_name',
            'interaction_type', 'response_status', 'program',
            'product_code', 'product_name', 'activity_name',
            'activity_title', 'activity_type', 'activity_status',
            'content_item_type', 'interaction_type_name']] = response_df[['section_name', 'content_item_id', 'content_item_name',
                                                                          'interaction_type', 'response_status', 'program',
                                                                          'product_code', 'product_name', 'activity_name',
                                                                          'activity_title', 'activity_type', 'activity_status',
                                                                          'content_item_type', 'interaction_type_name']].apply(lambda x: libs.np.where(libs.pd.isnull(x), x, x.astype(str).str.lower()))
        
    #making response as string before feeding to functions
    response_df['raw_response'] = libs.np.where(libs.pd.isnull(response_df['raw_response']), response_df['raw_response'], response_df['raw_response'].astype('str').str.lower())

    #below for multiple choice, SATA or any that allows to select choices
    #converts to numerics & orders them
    response_df.loc[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in'])), 'response'] = response_df[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in']))]['raw_response'].apply(lambda x: libs.resp_cleaning(x, re_order = True) if not libs.pd.isna(x) else x)

    #For order-interactions,numerical fill-ins
    #Converts to numerics for order-interactio & order-match and strip
    #Converts to lower case for text-entry-ineraction & strip
    #Coverts to string & strip for numerical-fill-in
    response_df.loc[(response_df['interaction_type'].isin(['order-interaction', 'order-match'])), 'response'] = response_df[(response_df['interaction_type'].isin(['order-interaction', 'order-match']))]['raw_response'].apply(lambda x: libs.resp_cleaning(x, re_order = False) if not libs.pd.isna(x) else x)
    response_df.loc[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in'])), 'response'] = response_df[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in']))]['raw_response'].apply(lambda x: x.lower().strip() if not libs.pd.isna(x) else x)

    response_df['corr_ans_cidf'] = libs.np.where(libs.pd.isnull(response_df['corr_ans_cidf']), response_df['corr_ans_cidf'], response_df['corr_ans_cidf'].astype('str').str.lower())


    response_df.loc[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in'])), 'corr_ans_cidf'] = response_df[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in']))]['corr_ans_cidf'].apply(lambda x: libs.resp_cleaning(x, re_order = True) if not libs.pd.isna(x) else x)

    response_df.loc[(response_df['interaction_type'].isin(['order-interaction', 'order-match'])), 'corr_ans_cidf'] = response_df[(response_df['interaction_type'].isin(['order-interaction', 'order-match']))]['corr_ans_cidf'].apply(lambda x: libs.resp_cleaning(x, re_order = False) if not libs.pd.isna(x) else x)
    response_df.loc[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in'])), 'corr_ans_cidf'] = response_df[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in']))]['corr_ans_cidf'].apply(lambda x: x.lower().strip() if not libs.pd.isna(x) else x)

    response_df[['m_sec_used', 'scored', 'score']] = response_df[['m_sec_used', 'scored', 'score']].astype('float64')

        
    #making sure date_created, date_completed are in datetime format
    response_df['date_created'] = libs.pd.to_datetime(response_df['date_created'], errors = 'coerce')
    response_df['date_completed'] = libs.pd.to_datetime(response_df['date_completed'], errors = 'coerce')

    return response_df