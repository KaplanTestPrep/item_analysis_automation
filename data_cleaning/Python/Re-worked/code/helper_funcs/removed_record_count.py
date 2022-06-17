def removed_record_count(df, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current, num_responses_current, things_to_say = 'Sequence removed', include_item_count = False):
    
    num_seq_new = df['sequenceId'].nunique()
    num_users_new = df['jasperUserId'].nunique()
    num_items_new = df['contentItemName'].nunique()
    num_responses_new = df.shape[0]
    
    if ((num_seq_current-num_seq_new)!=0 and (num_responses_current-num_responses_new)!=0):
        print(things_to_say, num_seq_current-num_seq_new)
        print('Users removed ', num_users_current-num_users_new)
        print('Unique items removed ', num_items_current-num_items_new)
        print('Responses removed ', num_responses_current-num_responses_new)
        print('')
        
        cleaning_info.loc[len(cleaning_info)] = [sec, sub_sec, things_to_say, num_seq_current-num_seq_new,
                                                 num_users_current-num_users_new, num_items_current-num_items_new,
                                                    num_responses_current-num_responses_new]
        
    else:
        print(things_to_say + 'NONE\n')
        cleaning_info.loc[len(cleaning_info)] = [sec, sub_sec, things_to_say, 'None', 'None', 'None', 'None']
    
    num_seq_current = num_seq_new
    num_users_current = num_users_new
    num_items_current = num_items_new
    num_responses_current = num_responses_new
    
    return num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info