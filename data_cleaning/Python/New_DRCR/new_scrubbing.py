import pandas as pd
import numpy as np
import re
import warnings

import getpass as gt
import psycopg2
from sqlalchemy import create_engine, sql
from urllib.parse import quote

import datetime


def db_con():
    #make a connection to the database with your credentials
    user = gt.getpass('Enter db username : ')
    pswd = gt.getpass('Enter db password : ')
    db = input('Enter database : ')
    host = 'redshift-apps-clusterredshift-19qcp828fizxm.ctebqc6bt0fq.us-east-1.redshift.amazonaws.com'
    port = '5439'

    engine = create_engine('postgresql://' + user + ':' + quote(pswd) + '@' + host + ':' + port + '/' + db)
    return engine


def ans_keys_validation(respExcl):

    #considering only scored responses per student
    student_key_resp = respExcl[(respExcl['score']==1) & (respExcl['response'].notnull()) & (respExcl['response']!=0)][['studentId', 'activityId', 'contentItemId', 'contentItemName', 'response', 'dateCreated']].drop_duplicates()
    
    #For nCount per each version of response
    item_resp_summary = student_key_resp.groupby(by=['contentItemId', 'contentItemName', 'response'], as_index = False).agg(
        nCount = ('studentId', 'nunique'),
        maxDateCreated = ('dateCreated', 'max'))

    item_resp_summary['key_rnk'] = item_resp_summary.groupby(by=['contentItemName'])['maxDateCreated'].rank(method = 'first', ascending = False)
    CI_old_keys = item_resp_summary[item_resp_summary['key_rnk']!=1][['contentItemId']]

    if CI_old_keys.empty:
        CI_old_keys = pd.DataFrame(columns = ['contentItemId'])

    answer_key_df = item_resp_summary[item_resp_summary['key_rnk']==1][['contentItemId', 'contentItemName', 'response']].rename(columns={'response':'answerKey'})

    return CI_old_keys, answer_key_df


def resp_recoding(df, recode_cond, item_status, resp):

    #recoding the response
    df.loc[recode_cond, 'response'] = resp

    #recoding the responsestatus
    df.loc[recode_cond, 'responseStatus'] = item_status

    #recoding the responsestatus
    df.loc[recode_cond, 'score'] = 0

    #recoding the responsestatus
    df.loc[recode_cond, 'attempted'] = False

    return df

def removed_record_count(df, num_seq_current, num_users_current, num_items_current, num_responses_current, things_to_say = 'Sequence removed', include_item_count = False):
    
    num_seq_new = df['activityId'].nunique()
    num_users_new = df['studentId'].nunique()
    num_items_new = df['contentItemName'].nunique()
    num_responses_new = df.shape[0]
    
    if ((num_seq_current-num_seq_new)!=0 and (num_responses_current-num_responses_new)!=0):
        print(things_to_say, num_seq_current-num_seq_new)
        print('Users removed ', num_users_current-num_users_new)
        print('Unique items removed ', num_items_current-num_items_new)
        print('Responses removed ', num_responses_current-num_responses_new)
        print('')
        
    else:
        print(f"{things_to_say}NONE\n")
    
    num_seq_current = num_seq_new
    num_users_current = num_users_new
    num_items_current = num_items_new
    num_responses_current = num_responses_new
    
    return num_seq_current, num_users_current, num_items_current, num_responses_current


def clean_item_data(data_path = 'C:\\Users\\VImmadisetty\\Downloads\\',
                    results_path = 'C:\\Users\\VImmadisetty\\Downloads\\',
                    analysis_name = '_',
                    resp = pd.DataFrame(),
                    data_pool = dict(),
                    remove_dup_CIs = True,
                    remove_deleted_sequences = True,
                    remove_impo_response_scored = True,
                    remove_impo_timing_seq = True,
                    remove_seq_w_tmq = True,
                    remove_frt_users = True,
                    remove_olc_users = True,
                    remove_hsg_enrolls = True,
                    remove_repeat_test_administrations = True):

    test_map = data_pool.get('test_map', pd.DataFrame())
    frt_enrols = data_pool.get('frt_enrols', pd.DataFrame())
    olc_enrols = data_pool.get('olc_enrols', pd.DataFrame())
    hsg_repeaters = data_pool.get('repeaters', pd.DataFrame())

    num_seq_current = resp['activityId'].nunique()
    num_users_current = resp['studentId'].nunique()
    num_items_current = resp['contentItemName'].nunique()
    num_responses_current = resp.shape[0]

    print(f'Starting clean item data function at : {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f"Total sequences at start: {num_seq_current}")
    print(f"Total users at start: {num_users_current}")
    print(f"Unique items at start: {num_items_current}")
    print(f"Total responses at start: {num_responses_current}")
    print('')

    #making respexcl
    respExcl = resp.copy()

    #make ci_old_keys, correct_answer
    CI_old_keys, correct_answer = ans_keys_validation(respExcl)
    #merging the correct_answer back to the response_df
    respExcl = pd.merge(respExcl, correct_answer[['contentItemName', 'answerKey']], on = ['contentItemName'], how = 'left')


    #Users with sequences containing duplicate content items
    #Excluding sequences that have a single content item more than once (after filtering out tutorials/breaks/staged)
    if(remove_dup_CIs == True):
        seq_to_exclude_calc4 = respExcl[respExcl['contentItemId']!=-1].copy()
        seq_to_exclude_calc4['count'] = seq_to_exclude_calc4.groupby(by=['activityId', 'contentItemName'])['contentItemName'].transform('count')
        seq_to_exclude_calc4 = seq_to_exclude_calc4[seq_to_exclude_calc4['count']>1]['activityId'].unique()
    
        if(len(seq_to_exclude_calc4) > 0):
            #adding data to rejects df
            respExcl = respExcl[~respExcl['activityId'].isin(seq_to_exclude_calc4)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                , things_to_say = f"Sequences with dupe content items within the same exam, removed: ")
    
    if(remove_frt_users == True):
        respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(frt_enrols['kbsenrollmentid'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = f"Sequences with enrollments in \"Free Trial\" product are removed: ")
    
    if(remove_olc_users == True):
        respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(olc_enrols['kbsenrollmentid'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = f"Sequences with enrollments in \"Online Companion\" product are removed: ")
    
    
    #Higher score guarantee or other repeat enrolls
    #We remove only erolls here not users
    if(remove_hsg_enrolls == True):
        respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(hsg_repeaters['kbsenrollmentid'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = f"Sequences with repeated enrollments are removed: ")
    
    
    #Users with sequences containing duplicate content items
    #Excluding sequences that have a single content item more than once (after filtering out tutorials/breaks/staged)
    if(remove_dup_CIs == True):
        seq_to_exclude_calc4 = respExcl[respExcl['contentItemId']!=-1].copy()
        seq_to_exclude_calc4['count'] = seq_to_exclude_calc4.groupby(by=['activityId', 'contentItemName'])['contentItemName'].transform('count')
        seq_to_exclude_calc4 = seq_to_exclude_calc4[seq_to_exclude_calc4['count']>1]['activityId'].unique()
    
        if(len(seq_to_exclude_calc4) > 0):
            respExcl = respExcl[~respExcl['activityId'].isin(seq_to_exclude_calc4)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                , things_to_say = f"Sequences with dupe content items within the same exam, removed: ")
    
    

    print('Sequence Removal')
    print('-'*20)
    sub_sec = 'Sequence Removal'
    #Sequence(s) sharing a template with a deleted sequence for that user
    if(remove_deleted_sequences == True):

        respExcl['deleted_name'] = respExcl['activityName'].apply(lambda x: True if (re.search(r'\d', x) and re.search(r'_d$', x)) else False)

        seq_deleted = respExcl[(respExcl['deleted_name']==True) | (respExcl['sequenceStatus']=='reset')]['activityId'].unique()
        
        if(len(seq_deleted)>0):
            respExcl = respExcl[~respExcl['activityId'].isin(seq_deleted)]
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = f"Sequences with deleted names are removed: ")
    

    #Sequences with impossibly scored responses: response=0 and score=1
    if remove_impo_response_scored:
        #remove the sequence for impossibly scored
        rem_cond = ((respExcl['response'].isnull()) & (respExcl['score']==1) & (respExcl['responseStatus']!='responded'))
        seq_to_remove = respExcl[rem_cond]['activityId'].unique()
        respExcl = respExcl[~respExcl['activityId'].isin(seq_to_remove)]

        #Fill in the raw_response with correct answer and keep the sequence
        rem_cond = ((respExcl['response'].isnull()) & (respExcl['score']==1) & (respExcl['responseStatus']=='responded'))
        respExcl.loc[rem_cond, 'response'] = respExcl[rem_cond]['answerKey']

        #score==0 & responseStatus = 'responded' but response is null
        rem_cond = (respExcl['response'].isnull()) & ((respExcl['score']!=1) | (respExcl['score'].isnull())) & (respExcl['responseStatus']=='responded')
        respExcl.loc[rem_cond, 'response'] = 'void response'

        
    
    #Sequences with impossible timing: mSecUsed < 0
    if(remove_impo_timing_seq == True):
        seq_to_exclude_calc1_5 = respExcl[respExcl['mSecUsed']<0]['activityId'].unique()
        if(len(seq_to_exclude_calc1_5)>0):
            respExcl = respExcl[~respExcl['activityId'].isin(seq_to_exclude_calc1_5)]
                            
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                          num_responses_current
                                                                                , things_to_say = f"Sequences with bad timing (mSecUsed < 0), removed: ")
    
    
    #Sequences with too many questions in a section (not applicable for QBank)
    #preparing the response data for applying above condition
    respExcl['actualNumQues'] = respExcl.groupby(by = ['activityId', 'activityName'])['contentItemName'].transform('count')
    if not test_map.empty:
        ##prep to find sequences with bad records in them or too much time in a section, or multiple items seen in one test,
        # or too many items in a section(repeated positions) for total exclusion
        respExcl = pd.merge(respExcl, pd.DataFrame({'activityName' : test_map['jasperSequenceName'],
                                                    'test_minutes_allowed' : test_map['minutesAllowed'],
                                                    'test_num_ques' : test_map['numQues'],
                                                    'test_response_threshold' : test_map['responseThreshold']}))
        if(respExcl.empty):
            warnings.warn(f"response df is empty after merging with test_map df on \"activityName\"")
    
    if remove_seq_w_tmq:
        seq_to_exclude_calc3 = respExcl[respExcl['actualNumQues'] > respExcl['test_num_ques']]['activityId'].unique()
            
        if(len(seq_to_exclude_calc3) > 0):
            respExcl = respExcl[~respExcl['activityId'].isin(seq_to_exclude_calc3)]
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = f"Sequences with too many questions in a section, removed: ")
    
    #Remove sequences for the users who practiced more than once
    if(remove_repeat_test_administrations == True):
        seq_to_exclude_calc5 = respExcl.copy()
        seq_to_exclude_calc5['num_ques'] = seq_to_exclude_calc5.groupby(by=['studentId', 'activityName', 'activityId', 'dateCreated'])['contentItemName'].transform('count')
        seq_to_exclude_calc5 = seq_to_exclude_calc5[['studentId', 'activityName', 'activityId', 'dateCreated']].drop_duplicates()
        seq_to_exclude_calc5['sequence_order'] = seq_to_exclude_calc5.groupby(by=['studentId', 'activityName'])['dateCreated'].rank(method = 'first')
        seq_to_exclude_calc5 = seq_to_exclude_calc5[seq_to_exclude_calc5['sequence_order']>1]['activityId'].unique()
        
        if(len(seq_to_exclude_calc5) > 0):
            respExcl = respExcl[~respExcl['activityId'].isin(seq_to_exclude_calc5)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = f"Sequences that were not the first administration for the user, removed: ")
    
    #making attempted proxy column used for calculating template metrics
    respExcl['attempted'] = np.where(respExcl['response'].isnull(), False, respExcl['response']!=0)
    
    #Recodings
    #Time Recodings
    #if mSecUsed<0 entire activity is removed
    #mSecUsed=0
    respExcl = resp_recoding(respExcl, ((respExcl['mSecUsed'] >= 0) & (respExcl['mSecUsed'] < 5) & (respExcl['response'].notnull())), 'guessed', -6)

    respExcl = resp_recoding(respExcl, ((respExcl['mSecUsed'] == 0) & (respExcl['response'].isnull())), 'not seen', -99)

    #mSecUsed<0 & mSecUsed<5
    respExcl = resp_recoding(respExcl, ((respExcl['mSecUsed'] > 0) & (respExcl['mSecUsed'] < 5) & (respExcl['response'].isnull())), 'skipped', -8)

    #mSecUsed>=5
    respExcl = resp_recoding(respExcl, ((respExcl['mSecUsed'] >=5) & (respExcl['response'].isnull())), 'omitted', -7)


    #answer key change recoding
    respExcl = resp_recoding(respExcl, ((respExcl['contentItemId'].isin(CI_old_keys['contentItemId']))), 'key changed', -2)


    #repeated item across tests
    rep_item_recod = respExcl[['studentId', 'activityId', 'contentItemName', 'dateCreated']].drop_duplicates().sort_values(by=['studentId', 'contentItemName'], ignore_index=True)
    rep_item_recod['itemrank'] = rep_item_recod.groupby(by=['studentId', 'contentItemName'])['dateCreated'].rank(method = 'first')
    respExcl = resp_recoding(respExcl, ((respExcl['studentId'].isin(rep_item_recod['studentId'])) & (respExcl['activityId'].isin(rep_item_recod['activityId'])) & (respExcl['contentItemName'].isin(rep_item_recod['contentItemName'])) & (rep_item_recod['itemrank']>1)), 'repeated item across test', -3)
    


    #removing sequences with <75% valid responses
    respExcl['template_num_omitted'] = respExcl.groupby(by=['activityId'])['attempted'].transform(lambda x: sum(x!=True))
    respExcl['template_num_attempted'] = respExcl.groupby(by=['activityId'])['attempted'].transform('sum')
    respExcl['template_perc_attempted'] = respExcl['template_num_attempted']/respExcl['test_num_ques']
    respExcl['template_raw_correct'] = respExcl.groupby(by=['activityId'])['score'].transform('sum')
    respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['test_num_ques']
    respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']
            
    seq_below_resp_threshold = respExcl[respExcl['template_perc_attempted'] < respExcl['test_response_threshold']]['activityId'].unique()

    respExcl = respExcl[~respExcl['activityId'].isin(seq_below_resp_threshold)]
            
    num_seq_current, num_users_current, num_items_current, num_responses_current = removed_record_count(respExcl, num_seq_current, num_users_current, num_items_current,
                                                                                         num_responses_current,
                                                                                 things_to_say = f"Sequences with items fewer than {respExcl['test_response_threshold'].unique()*100}% of items attempted in a sequence, removed :")
    

    print(f"Remaining number of responses in final output: {respExcl.shape[0]}")
    print(f"Remaining number of sequences in final output: {num_seq_current}")
    print(f"Remaining number of users in final output: {num_users_current}")
    print(f"Remaining number of unique items in final output: {num_items_current}")
    print(f'Cleaning function time completed: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    return CI_old_keys, correct_answer, respExcl

def make_user_level_matrices(df,
                  vars_for_matrices,
                  destination_file_path,
                  destination_file_name_prefix,
                  analysis_name,
                  omit_code = '.',
                  not_seen_code = '-99',
                  use_display_order = False,
                 qbank = True,
                 item_order_list = []):
    
    
    for col, mat_nam in vars_for_matrices.items():
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
        
        if(len(item_order_list)>0):
            big_matrix = big_matrix[item_order_list]
        
        big_matrix.to_csv(destination_file_path+analysis_name+destination_file_name_prefix+mat_nam+'.csv')
        print('Finished creating matrix for ' + mat_nam)
    return big_matrix


#display sequence is removed due to for questbank creating multiple rows
#below also holds for items that are not having constant display sequence across various templates
def make_item_level_info(df, content_df, results_path, analysis_name, corr_ans, qbank = False):
    #making a seen field for an item to make count_seen
    df['itemSeen'] = df['responseStatus']!='not-reached'
    
    if(qbank == True):
        cidf_summary = df.sort_values(by=['displaySeq']).groupby(by=['contentItemName', 'activityName', 'templateId'], as_index=False, dropna = False).agg(displaySeq = ('displaySeq', lambda x: ', '.join(x.drop_duplicates().astype(str))),
                                                                                                                                  count_att = ('attempted', 'sum'),
                                                                                                                                                   count_seen = ('itemSeen', 'sum'),
                                                                                                                                                   num_correct = ('score', 'sum'),
                                                                                                                                                   first_date = ('dateCreated', 'min'),
                                                                                                                                                   last_date = ('dateCreated', 'max')).sort_values(by=['activityName'], ignore_index = True)
        

    
        cidf_summary = pd.merge(cidf_summary, content_df)
    
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
                             'correctAnswer']].drop_duplicates(ignore_index = True)
    
    else:
        cidf_summary = df.sort_values(by=['displaySeq']).groupby(by=['contentItemName', 'activityName', 'templateId'], as_index=False, dropna = False).agg(displaySeq = ('displaySeq', lambda x: ', '.join(x.drop_duplicates().astype(str))),
                                                                                                                                  count_att = ('attempted', 'sum'),
                                                                                                                                                   count_seen = ('itemSeen', 'sum'),
                                                                                                                                                   num_correct = ('score', 'sum'),
                                                                                                                                                   first_date = ('dateCreated', 'min'),
                                                                                                                                                   last_date = ('dateCreated', 'max')).sort_values(by=['activityName'], ignore_index = True)
        

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
        
    cidf_summary = pd.merge(cidf_summary.drop(columns = ['correctAnswer', 'displaySeq', 'contentItemId']), corr_ans, on = ['contentItemName'])
    cidf_summary.to_csv(results_path+analysis_name+'_Content_Item_Info.csv', index = False)
    
    return cidf_summary

def make_activity_level_info(df, results_path, analysis_name):
    activity_level_info = df[['studentId', 'activityId', 'dateCreated', 'dateCompleted', 'activityName',
                                'template_num_attempted', 'template_raw_correct', 'template_pTotal', 'template_pPlus']].drop_duplicates()

    activity_level_info.to_csv(results_path+analysis_name+'_activity_Level_Info.csv', index = False)
    
    return activity_level_info
    
    
def make_user_level_info(df, results_path, analysis_name, test_map, qbank = False):
    panel_calc = df.groupby(by=['studentId', 'activityName'], as_index = False, dropna = False).agg(num_seen = ('responseStatus', lambda x: sum(x!='not-reached')),
                                                                                                    num_att = ('attempted', 'sum'),
                                                                                                    num_correct = ('score', 'sum'))
    panel_calc['activityName'] = 'incl_'+panel_calc['activityName'].astype('str')

    panel_calc[['total_seen', 'total_att', 'total_correct']] = panel_calc.groupby(by=['studentId'])[['num_seen', 'num_att', 'num_correct']].transform('sum')
    panel_calc['num_panel_tests_taken'] = panel_calc.groupby(by=['studentId'])['activityName'].transform('nunique')
    panel_calc['test_incl'] = 1
    panel_calc['total_panel'] = sum(test_map['numQues']) if not qbank else df[['studentId', 'activityName', 'actualNumQues']].drop_duplicates(ignore_index = True)['actualNumQues']
    panel_calc['panel_ptotal'] = panel_calc['total_correct']/panel_calc['total_seen']
    panel_calc['panel_pplus'] = panel_calc['total_correct']/panel_calc['total_att']
    
    
    test_resp_squished = df.groupby(by=['studentId'], as_index = False, dropna = False).agg(num_seq_taken = ('activityName', 'nunique'),
                                                                                        test_seen = ('activityName', 'size'),
                                                                                        test_att = ('attempted', 'sum'),
                                                                                        test_correct = ('score', 'sum'),
                                                                                        first_test_date = ('dateCreated', 'min'),
                                                                                        last_test_date = ('dateCreated', 'max'))
    
    test_resp_squished['test_total'] = sum(test_map['numQues']) if not qbank else df[['studentId', 'actualNumQues']].drop_duplicates(ignore_index = True).groupby(by=['studentId'], as_index = False)[['actualNumQues']].agg('sum')['actualNumQues']
    test_resp_squished['test_ptotal'] = test_resp_squished['test_correct']/test_resp_squished['test_seen']
    test_resp_squished['test_pplus'] = test_resp_squished['test_correct']/test_resp_squished['test_att']
    
    users = pd.merge(test_resp_squished, panel_calc)

    user_info = pd.pivot(data=users,
        index=['studentId', 'num_panel_tests_taken', 'total_panel', 'total_seen', 'total_att', 'total_correct', 'panel_ptotal', 'panel_pplus'],
        columns = 'activityName',
        values = 'num_seq_taken').reset_index()

    user_info.to_csv(results_path+analysis_name+'_User_Level_Info.csv', index = False)
    
    return user_info


def Generate_file(
    data_path = 'C:\\Users\\CRachuri\\Downloads\\',
    results_path = 'C:\\Users\\CRachuri\\Downloads\\',
    analysis_name = '_',
    Qc_files_data=dict(),
    result_checks=True, # keep always true
    Matched_items=True,
    umatched_items=True,
    duplicate_items=True,
    meta_data=True,
    Ncount_file=True,
    cleaning_log=True,
    score_frequency_count=True,
    response_frequency_count=True
    ):
    response_df = Qc_files_data.get('response_df', pd.DataFrame())
    content_df=Qc_files_data.get('content_df',pd.DataFrame())
    result=Qc_files_data.get('cleaned_response_data',pd.DataFrame())
    Qid_list=Qc_files_data.get('Qid_list',pd.DataFrame())

    if len(Qc_files_data)!=0:
        file_name=results_path+analysis_name
        writer = pd.ExcelWriter(file_name+'_Audit_info_sheets.xlsx', engine='xlsxwriter')
        # checking whether students existed for under no response 
        reslt=result.copy() 
        a=reslt[['studentId','attempted']].drop_duplicates().groupby(['studentId']).agg(attempt_count=('attempted','count')).reset_index()
        data=pd.merge(a[a['attempt_count']==1],reslt[['studentId','attempted']].drop_duplicates(),on='studentId',how='left')
        data=data[(data['attempt_count']==1) & (data['attempted']==False) ]['studentId']
        data.to_excel(writer, sheet_name='Removed_under_no_response',index=False)
        if (data.empty == True):
            result=Qc_files_data['cleaned_response_data']
        else :
            result=reslt[~reslt['studentId'].isin([data])]

        # checking the conditions for omitt, responded and not-reached in result file
        if result_checks== True:

            df=result.copy()
            df_resp=df[(df['responseStatus']=='responded') & ((df['response'].isnull())|(df['attempted']==False)|(df['score'].isnull())) ]
            # condition for  not-reached
            df_not_reached=df[(df['responseStatus']=='not-reached') & ((pd.notna(df['response']))|(df['attempted']==True) | (pd.notna(df['score'])))]
            # condition for omitted
            df_omit=df[(df['responseStatus']=='omitted') & ((~df['response'].isin([0,'0',np.nan]) | (df['attempted']==True) | (df['score']==1)))]
            df_resp=pd.concat([df_resp,df_not_reached,df_omit])
            

            if (df_resp.empty):
                print('All the conditions for result file is in sync')
            else :
                print('conditions for the result files are not in sync , Keep eye on following students !')
                print(df_resp)

        if cleaning_log==True:
            #pushing the cleaning info file
            file = pd.read_csv(results_path + analysis_name+'_Cleaning_info.txt', sep=';')
            file.astype('str').to_excel(writer, sheet_name='cleaning_log',index=False)

            # pushing cleaning summary info
            file=pd.read_csv(results_path + analysis_name+'_cleaningInfo.csv')
            file.rename(columns={'value':'Sequence Removed'},inplace=True)
            file.to_excel(writer, sheet_name='cleaning_log_summary',index=False)

        # file generation for matched or unmatched Qids list
        if Matched_items==True:
            Qids=Qid_list.copy()
            not_matched_items = pd.merge(Qids, content_df[['contentItemName']].drop_duplicates(), on = 'contentItemName', how = 'left', indicator=True)
            matched_items = not_matched_items[not_matched_items['_merge']=='both'][['contentItemName']]
            matched_items.to_excel(writer, sheet_name='Matched_QIDs_list',index=False)

        if  umatched_items==True:
            Qids=Qid_list.copy()
            not_matched_items = pd.merge(Qids, content_df[['contentItemName']].drop_duplicates(), on = 'contentItemName', how = 'left', indicator=True)
            matched_items = not_matched_items[not_matched_items['_merge']=='both'][['contentItemName']]
            not_matched_items = not_matched_items[not_matched_items['_merge']=='left_only'][['contentItemName']]
            not_matched_items.to_excel(writer, sheet_name='Unmatched_QIDs_list',index=False)


        # for Duplicates versions 
        if duplicate_items==True:
            duplicate_item=content_df[content_df['contentItemName'].isin(content_df[['contentItemName','contentItemId' ,'correctAnswer']].drop_duplicates().groupby(by=['contentItemName']).filter(lambda x: len(x)>1)['contentItemName'])][['contentItemId', 'contentItemName', 'correctAnswer', 'last_modified']]
            if duplicate_item.empty:
                duplicate_item['Considered_for_grading']=""
                duplicate_item['User_count_as_per_versions']=""
                duplicate_item.to_excel(writer, sheet_name='duplicate_items_versions_list',index=False)

            else :
                # making the ranking for answer_count             
                duplicate_item['answer_count']=duplicate_item.groupby(['contentItemName'])['correctAnswer'].transform('nunique')
                duplicate_item.loc[duplicate_item['answer_count']==1,'considered_for_grading']='1'
                #duplicate_item['considered_for_grading']=np.where(duplicate_item['answer_count']==1,1)

                # for considering grading columns 
                duplicate_item.loc[duplicate_item['answer_count']>1, 'rank_modifies'] = duplicate_item[duplicate_item['answer_count']>1].groupby(['contentItemName'])['last_modified'].rank(ascending=False , method='first')
                duplicate_item.loc[duplicate_item['rank_modifies']==1, 'considered_for_grading']=1

                duplicate_item['considered_for_grading'].fillna(value = 0, inplace = True)
                duplicate_item.drop(columns={'answer_count','rank_modifies'},inplace=True)

                # counting user count as per versions 
                user_count=response_df[['contentItemId','contentItemName', 'jasperUserId']].drop_duplicates().groupby(by = ['contentItemId','contentItemName'], as_index = False, dropna=False).agg(User_count_as_per_version = ('jasperUserId', 'nunique'))
                duplicate_item=pd.merge(duplicate_item,user_count,on=['contentItemId','contentItemName'],how='inner')
                
                duplicate_item.to_excel(writer, sheet_name='duplicate_items_versions_list',index=False)
            

        if meta_data==True:

            #Making cor_ans df
            cor_ans = content_df[content_df.groupby(by=['contentItemName'])['last_modified'].rank(method = 'first', ascending=False)==1][['contentItemId', 'contentItemName', 'correctAnswer','countchoices','interactionTypeName']]

            # snap for Answer key changed 
            answer_key_flag=content_df[['contentItemName','correctAnswer']].drop_duplicates().groupby(['contentItemName']).agg(answer_key_count=('contentItemName','count')).reset_index()
            answer_key_flag['Answer_key_changed']=np.where(answer_key_flag['answer_key_count']>1,1,0)
            answer_key_flag.drop(['answer_key_count'],axis=1,inplace=True)

            #multi Item Versions

            multi_item_version=content_df.groupby(['contentItemName']).agg(items_versions_count=('contentItemId','count')).reset_index()
            multi_item_version['multi_item_version']=np.where(multi_item_version['items_versions_count']>1, 1 ,0)
            multi_item_version=pd.merge(answer_key_flag,multi_item_version,on='contentItemName',how='inner')

            # option count changes
            options_changed=content_df[['contentItemName','countchoices']].drop_duplicates().groupby(['contentItemName','countchoices']).agg(Options_count=('countchoices','size')).reset_index()
            options_changed['options_countchoices_changed']=np.where(options_changed['Options_count']>1,1,0)
            options_changed=options_changed[['contentItemName','countchoices','options_countchoices_changed']]
            options_changed=pd.merge(options_changed,multi_item_version,on=['contentItemName'],how='inner')
            cor_ans=pd.merge(cor_ans,options_changed,on=['contentItemName','countchoices'],how='inner')
            cor_ans.to_excel(writer, sheet_name='Item_Meta_data',index=False)

        if Ncount_file==True:
            # Ncount before after cleaning
            raw_ft_items_counts = response_df[['contentItemName', 'jasperUserId']].drop_duplicates().groupby(by = ['contentItemName'], as_index = False, dropna=False).agg(initial_count = ('jasperUserId', 'nunique'))
            final_items_counts=result.groupby(by=['contentItemName', 'activityName', 'templateId'],as_index=False,dropna=False).agg(after_cleaning=('attempted', 'sum'))[['contentItemName','after_cleaning']]
            count_result=pd.merge(raw_ft_items_counts,final_items_counts,on=['contentItemName'],how='inner')
            count_result.to_excel(writer, sheet_name='Ncount_file',index=False)

        if score_frequency_count==True :
            # count as per the score
            data=result[['studentId','contentItemName','score','responseStatus']].drop_duplicates().copy()
            data.loc[data['responseStatus']=='not-reached','score']='not-reached'
            data.loc[data['responseStatus']=='omitted','score']='omitted'
            data.loc[data['responseStatus']==np.nan ,'score']='omitted'
            data.loc[data['responseStatus'].isnull(),'score']='omitted'
            data.pivot_table(index='contentItemName',columns='score',values='studentId',aggfunc='count').reset_index().to_excel(writer,sheet_name='Score_frequency_count',index=False)

        if response_frequency_count == True :
            # count as per the response 
            data=result[['studentId','contentItemName','response','responseStatus']].drop_duplicates().copy()
            data.loc[data['responseStatus']=='not-reached','response']='not-reached'
            data.loc[data['responseStatus']=='omitted','response']='omitted'
            data.loc[data['responseStatus']==np.nan ,'response']='omitted'
            data.loc[data['responseStatus'].isnull(),'response']='omitted'
            data.pivot_table(index='contentItemName',columns='response',values='responseStatus',aggfunc='count').reset_index().to_excel(writer,sheet_name='Response_frequency_count',index=False)


        writer.save()