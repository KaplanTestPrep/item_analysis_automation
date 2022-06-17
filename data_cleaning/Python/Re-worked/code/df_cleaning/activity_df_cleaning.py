import libs

def activity_df_cleaning(activity_df):
######Working on activity_df
    activity_df['timestamp_created'] = libs.pd.to_datetime(activity_df['timestamp_created'], errors = 'coerce')
    activity_df['timestamp_completed'] = libs.pd.to_datetime(activity_df['timestamp_completed'], errors = 'coerce')
        
    activity_df = activity_df[['source_system', 'history_db_id', 'student_id',
            'enrollment_id', 'program', 'product_code', 'product_name',
            'activity_id', 'template_id', 'template_name', 'activity_title',
            'activity_type', 'timestamp_created', 'timestamp_completed',
            'status', 'tutor_mode']].drop_duplicates(ignore_index= True)

    activity_df.rename(columns={'template_name' : 'activity_name',
                            'timestamp_created' : 'date_created',
                            'timestamp_completed' : 'date_completed',
                           'status':'activity_status'}, inplace = True)

    #below doesn't touch nans & doesn't work if source_system is other than string
    activity_df['source_system'] = libs.np.where(libs.pd.isnull(activity_df['source_system']), activity_df['source_system'], activity_df['source_system'].astype('str').str.lower())

    #this will handle error for Nan values while converting to int64
    activity_df['history_db_id'] = libs.np.where(libs.pd.isnull(activity_df['history_db_id']), activity_df['history_db_id'], activity_df['history_db_id'].astype('str').str.lower())

    #making student id truly string to avoid issues while merging with reponse_df
    activity_df['student_id'] = libs.np.where(libs.pd.isnull(activity_df['student_id']), activity_df['student_id'], activity_df['student_id'].astype('str').str.lower())

    #making student id truly string to avoid issues while merging with reponse_df
    activity_df['activity_id'] = libs.np.where(libs.pd.isnull(activity_df['activity_id']), activity_df['activity_id'], activity_df['activity_id'].astype('str').str.lower())

    #making below to lower case, to avoid issues while merging with test_df
    activity_df['activity_name'] = libs.np.where(libs.pd.isnull(activity_df['activity_name']), activity_df['activity_name'], activity_df['activity_name'].astype('str').str.lower())
    activity_df['enrollment_id'] = libs.np.where(libs.pd.isnull(activity_df['enrollment_id']), activity_df['enrollment_id'], activity_df['enrollment_id'].astype('str').str.lower())

    return activity_df
      