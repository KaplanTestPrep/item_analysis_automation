import libs

def rem_users_w_deleted_activity(respExcl = libs.pd.DataFrame()):
    respExcl['deleted_name'] = respExcl['sequenceName'].apply(lambda x: True if (libs.re.search(r'\d', x) and libs.re.search(r'_d$', x)) else False)

    users_w_deleted_act = respExcl[(respExcl['deleted_name']==True) | (respExcl['activity_name']=='reset')][['student_id',
                                                                                             'enrollment_id',
                                                                                             'activity_status',
                                                                                             'deleted_name',
                                                                                             'activity_name',
                                                                                             'template_id']].drop_duplicates(ignore_index = True).copy()
    
    #below code removes entire users from response_data
    respExcl = respExcl[~(respExcl['student_id'].isin(users_w_deleted_act['student_id'].unique()))]
    #num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
    #                                                                                                                       num_responses_current,
    #                                                                                                                         things_to_say = 'Sequences with deleted names are removed: ')
    
    return respExcl