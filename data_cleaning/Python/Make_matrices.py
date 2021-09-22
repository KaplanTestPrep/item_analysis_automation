def make_user_level_matrices(df,
                  vars_for_matrices,
                  destination_file_path,
                  destination_file_name_prefix,
                  analysis_name,
                  omit_code = '.',
                  not_seen_code = '-99',
                  use_display_order = False,
                  zero_sec_as_not_reached = False,
                 qbank = True):
    
    if(zero_sec_as_not_reached == True):
        df = df[df['mSecUsed']>0].copy()
    
    for col, mat_nam in vars_for_matrices.items():
        df = df[df['repeatOmitted']==False].copy()
        #marking omit values to omit_code here
        new_col = col+'_omits'
        df[new_col] = df[col]
        df.loc[df['attempted']==False, new_col] = omit_code
        df.loc[df['responseStatus']=='not-reached', new_col] = not_seen_code
        big_matrix = pd.pivot(data = df, index = 'studentId', columns='contentItemName', values = new_col)
            
        #marking not seen items with not_seen_code
        big_matrix.fillna(value = not_seen_code, inplace = True)
    
        if(use_display_order == True):
            if(qbank == False):
                CI_ordered = df[['activityName', 'contentItemName', 'displaySeq']].drop_duplicates().sort_values(by=['activityName', 'displaySeq'])['contentItemName']
                big_matrix = big_matrix[CI_ordered]
            else:
                CI_ordered = df[['contentItemName']].drop_duplicates().sort_values(by=['contentItemName'], ignore_index=True)['contentItemName']
                big_matrix = big_matrix[CI_ordered]
        
        big_matrix.to_csv(destination_file_path+analysis_name+destination_file_name_prefix+mat_nam+'.csv')
        print('Finished creating matrix for ' + mat_nam)
    return big_matrix


#display sequence is removed due to for questbank creating multiple rows
#below also holds for items that are not having constant display sequence across various templates
def make_item_level_info(df, content_df, results_path, analysis_name, corr_ans, qbank = False):
    
    df = df[df['repeatOmitted']==False].copy()
    #making a seen field for an item to make count_seen
    df['itemSeen'] = df['responseStatus']!='not-reached'
    
    if(qbank == True):
        cidf_summary = df.groupby(by=['contentItemId', 'contentItemName', 'activityName', 'templateId'], as_index=False)[['attempted', 'itemSeen', 'score','dateCreated']].agg({'attempted':'sum',
                                                                                                        'itemSeen':'sum',
                                                                                                        'score':'sum',
                                                                                                        'dateCreated':['min', 'max']}).sort_values(by=['activityName'], ignore_index = True)
    
        cidf_summary.columns = ['contentItemId', 'contentItemName', 'activityName', 'templateId', 'count_att', 'count_seen', 'num_correct', 'first_date', 'last_date']
        
        cidf_summary = pd.merge(cidf_summary, content_df)
    
        cidf_summary = cidf_summary[['contentItemId', 'contentItemName',
                             'activityName',
                             'templateId',
                             'count_att',
                             'count_seen',
                             'num_correct',
                             'first_date',
                             'last_date',
                             'interactiontypename',
                             'countchoices',
                             'correctAnswer']].drop_duplicates(ignore_index = True)
    
    else:
        cidf_summary = df.groupby(by=['contentItemId', 'contentItemName', 'activityName', 'templateId', 'displaySeq'], as_index=False)[['attempted', 'itemSeen', 'score','dateCreated']].agg({'attempted':'sum',
                                                                                                        'itemSeen':'sum',
                                                                                                        'score':'sum',
                                                                                                        'dateCreated':['min', 'max']}).sort_values(by=['activityName'], ignore_index = True)
    
        cidf_summary.columns = ['contentItemId', 'contentItemName', 'activityName', 'templateId', 'displaySeq', 'count_att', 'count_seen', 'num_correct', 'first_date', 'last_date']
        
        cidf_summary = pd.merge(cidf_summary, content_df, how = 'left')
    
        cidf_summary = cidf_summary[['contentItemId', 'contentItemName',
                             'activityName',
                             'templateId',
                            'displaySeq',
                             'count_att',
                             'count_seen',
                             'num_correct',
                             'first_date',
                             'last_date',
                             'interactiontypename',
                             'countchoices',
                             'correctAnswer']].drop_duplicates().sort_values(by=['displaySeq'], ignore_index = True)
        
    
    
    cidf_summary = pd.merge(cidf_summary.drop(columns = ['correctAnswer', 'displaySeq', 'contentItemName']), corr_ans, on = ['contentItemId'])
    
    cidf_summary.to_csv(results_path+analysis_name+'_Content_Item_Info.csv', index = False)
    
    return cidf_summary

def make_activity_level_info(df, results_path, analysis_name):
    
    df = df[df['repeatOmitted']==False].copy()
    activity_level_info = df[['studentId', 'activityId', 'dateCreated', 'dateCompleted', 'activityName',
                                'template_num_attempted', 'template_raw_correct', 'template_pTotal', 'template_pPlus']].drop_duplicates()

    activity_level_info.to_csv(results_path+analysis_name+'_activity_Level_Info.csv', index = False)
    
    return activity_level_info
    
    
def make_user_level_info(df, results_path, analysis_name, test_map):
    df = df[df['repeatOmitted']==False].copy()
    panel_calc = df.groupby(by=['studentId', 'activityName'], as_index = False)[['attempted', 'score']].agg({'attempted':['size', 'sum'],
                                                                                   'score':['sum']})
    panel_calc.columns = ['studentId', 'activityName', 'num_seen', 'num_att', 'num_correct']
    panel_calc['activityName'] = 'incl_'+panel_calc['activityName'].astype('str')

    panel_calc[['total_seen', 'total_att', 'total_correct']] = panel_calc.groupby(by=['studentId'])[['num_seen', 'num_att', 'num_correct']].transform('sum')
    panel_calc['num_panel_tests_taken'] = panel_calc.groupby(by=['studentId'])['activityName'].transform('nunique')
    panel_calc['test_incl'] = 1
    panel_calc['total_panel'] = sum(test_map['numQues'])
    panel_calc['panel_ptotal'] = panel_calc['total_correct']/panel_calc['total_seen']
    panel_calc['panel_pplus'] = panel_calc['total_correct']/panel_calc['total_att']
    
    
    test_resp_squished = df.groupby(by=['studentId'], as_index = False)[['activityName', 'attempted', 'score', 'dateCreated']].agg({'activityName':['nunique', 'size'],
                                                                                                                        'attempted':['sum'],
                                                                                                                        'score':['sum'],
                                                                                                                        'dateCreated':['min', 'max']})

    test_resp_squished.columns=['studentId', 'num_seq_taken', 'test_seen', 'test_att', 'test_correct', 'first_test_date', 'last_test_date']
    test_resp_squished['test_total'] = sum(test_map['numQues'])
    test_resp_squished['test_ptotal'] = test_resp_squished['test_correct']/test_resp_squished['test_seen']
    test_resp_squished['test_pplus'] = test_resp_squished['test_correct']/test_resp_squished['test_att']
    
    users = pd.merge(test_resp_squished, panel_calc)

    user_info = pd.pivot(data=users,
        index=['studentId', 'num_panel_tests_taken', 'total_panel', 'total_seen', 'total_att', 'total_correct', 'panel_ptotal', 'panel_pplus'],
        columns = 'activityName',
        values = 'num_seq_taken').reset_index()

    user_info.to_csv(results_path+analysis_name+'_User_Level_Info.csv', index = False)
    
    return user_info
