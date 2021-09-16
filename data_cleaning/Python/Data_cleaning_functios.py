import pandas as pd
import numpy as np

import getpass as gt
import psycopg2
from sqlalchemy import create_engine, sql

import os
from openpyxl import load_workbook
#from miscfns import misc_fns

import re
import datetime
import warnings
import sys

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

def db_con():
    #make a connection to the database with your credentials
    user = gt.getpass('Enter db username : ')
    pswd = gt.getpass('Enter db password : ')
    db = input('Enter database : ')
    host = 'redshift-apps-clusterredshift-19qcp828fizxm.ctebqc6bt0fq.us-east-1.redshift.amazonaws.com'
    port = '5439'

    engine = create_engine('postgresql://' + user + ':' + pswd + '@' + host + ':' + port + '/' + db)
    return engine


def describe_dupe_cor_ans(df, use_contentItemName = False):
    if (use_contentItemName == True):
        cadf = df[df['correctAnswer'].notnull()][['contentItemName', 'correctAnswer']].drop_duplicates()
        
        if (cadf['contentItemName'].nunique() < df['contentItemName'].nunique()):
            cadf = df[df['score']==1][['contentItemName', 'response']].drop_duplicates()
            cadf.rename(columns={'response':'correctAnswer'}, inplace = True)
        
        #we get the second instance of duplicate item names with duplicated() fn
        dupe_itemIds = cadf[cadf['contentItemName'].duplicated()]['contentItemName'].unique()
        if (len(dupe_itemIds)==0):
            sys.exit(misc_fns.color.BOLD + 'No duplicate correct answer'  + misc_fns.color.END)
            
        df_to_summarize = df[(df['contentItemName'].isin(dupe_itemIds)) & (df['score']==1)]
        outputdf = df_to_summarize.fillna('None').groupby(by=['contentItemName', 'response'])[['score']].agg(['count'])
        
        return outputdf
    else:
        cadf = df[df['correctAnswer'].notnull()][['contentItemId', 'correctAnswer']].drop_duplicates()
        if (cadf['contentItemId'].nunique() < df['contentItemId'].nunique()):
            cadf = df[df['score']==1][['contentItemId', 'response']].drop_duplicates()
            cadf.rename(columns = {'response':'correctAnswer'})
            
        dupe_itemIds = cadf[cadf['contentItemId'].duplicated()]['contentItemId'].unique()
        
        if (len(dupe_itemIds)==0):
            sys.exit(misc_fns.color.BOLD + 'No duplicate correct answers'  + misc_fns.color.END)
        
        df_to_summarize = df[(df['contentItemId'].isin(dupe_itemIds)) & (df['score']==1)]
        
        if(len(df_to_summarize['dateCreated'])!=0):
            outputdf = df_to_summarize.fillna('None').groupby(by=['contentItemId', 'response']).agg({'contentItemId':'count', 'dateCreated':['min','max']})
            outputdf.columns = ['total', 'min_date', 'max_date']
        else:
            outputdf = df_to_summarize.fillna('None').groupby(by=['contentItemId', 'response']).agg({'contentItemId' : ['count']})
            outputdf.columns = 'total'
        return outputdf

def get_item_cor_ans(df, no_correctAnswer = False, use_contentItemName = False):
    if(use_contentItemName == True):
        if (no_correctAnswer):
            cadf = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemName', 'response']].drop_duplicates().sort_values(by=['contentItemName']).reset_index(drop=True)
            cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if(cadf['contentItemName'].nunique() < cadf.shape[0]):
                print(describe_dupe_cor_ans(df, use_contentItemName = use_contentItemName))
                warnings.warn(misc_fns.color.BOLD + 'Duplicate correct answers for some items'  + misc_fns.color.END)
            else:
                print(misc_fns.color.BOLD + 'No duplicate correct answer'  + misc_fns.color.END)
                dup_cadf = cadf.groupby(by=['contentItemName']).filter(lambda x: len(x)>1).reset_index(drop=True)
                return cadf, dup_cadf
        else:
            cadf = df[df['correctAnswer'].notnull()][['contentItemName', 'correctAnswer']].drop_duplicates().sort_values(by=['contentItemName']).reset_index(drop=True)
            
            if (cadf['contentItemName'].nunique() < df[df['correctAnswer'].notnull()][['contentItemName']].nunique()):
                cadf = df[df['score']==1][['contentItemName', 'response']].drop_duplicates().reset_index(drop=True)
                cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if (cadf['contentItemName'].nunique() < cadf.shape[0]):
                print(describe_dupe_cor_ans(df, use_contentItemName = use_contentItemName))
                warnings.warn(misc_fns.color.BOLD + 'Duplicate correct answers for some items'  + misc_fns.color.END)
            
            else:
                print(misc_fns.color.BOLD + 'No duplicate correct answer'  + misc_fns.color.END)
                dup_cadf = cadf.groupby(by=['contentItemName']).filter(lambda x: len(x)>1).reset_index(drop=True)
                return cadf, dup_cadf
    else:
        #use_contentItemName = False
        if (no_correctAnswer):
            cadf = df[(df['score']==1) & (df['response']!=0) & (df['response'].notnull())][['contentItemId', 'response']].drop_duplicates().sort_values(by=['contentItemId']).reset_index(drop=True)
            cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            if(cadf['contentItemId'].nunique() < cadf.shape[0]):
                print(describe_dupe_cor_ans(df, use_contentItemName = use_contentItemName))
                warnings.warn(misc_fns.color.BOLD + 'Duplicate correct answers for some items'  + misc_fns.color.END)
            
            else:
                print(misc_fns.color.BOLD + 'No duplicate correct answer'  + misc_fns.color.END)
                dup_cadf = cadf.groupby(by=['contentItemId']).filter(lambda x: len(x)>1).reset_index(drop=True)
                return cadf, dup_cadf
        else:
            cadf = df[df['correctAnswer'].notnull()][['contentItemId', 'correctAnswer']].drop_duplicates().sort_values(by=['contentItemId']).reset_index(drop=True)
            
            if (cadf['contentItemId'].nunique() < df[df['correctAnswer'].notnull()][['contentItemId']].nunique()):
                cadf = df[df['score']==1][['contentItemId', 'response']].drop_duplicates().reset_index(drop=True)
                cadf.rename(columns={'response' : 'correctAnswer'}, inplace = True)
            
            if (cadf['contentItemId'].nunique() < cadf.shape[0]):
                print(describe_dupe_cor_ans(df, use_contentItemName = use_contentItemName))
                warnings.warn(misc_fns.color.BOLD + 'Duplicate correct answers for some items'  + misc_fns.color.END)
            else:
                print(misc_fns.color.BOLD + 'No duplicate correct answer'  + misc_fns.color.END)
                dup_cadf = cadf.groupby(by=['contentItemId']).filter(lambda x: len(x)>1).reset_index(drop=True)
                return cadf, dup_cadf


def recode_as_omitted(df, omit_condition = pd.Series(dtype = 'float')):
    if (omit_condition.empty):
        warnings.warn('No omit_condition provided.')
    if(len(set(df.columns).intersection(set(['orig_response', 'orig_score'])))==0):
        #There are no columns called orig_response, orig_score, create them before changing any data
        df['orig_response'] = df['response']
        df['orig_score'] = df['score']
    if(sum(omit_condition.isnull())>0):
        omit_condition[omit_condition.isnull()] = False
        warnings.warn('Null omit condition defaulted to False')
    if(len(omit_condition)>0):
        #change responses to 0
        df.loc[omit_condition, 'response'] = 0
        #change score to 0
        df.loc[omit_condition, 'score'] = 0
        #change attempted to False
        df.loc[omit_condition, 'attempted'] = False
        #change responseStatus to 'omitted' if it is null
        df.loc[omit_condition,  'responseStatus'] = 'omitted'
        # & (df['attempted']==False) & (df['responseStatus'].isnull()
    else:
        warnings.warn('Nothing recoded due to entirely False omit_condition')
    return df
    
def timing_exclusion(df, mSec_min_threshold = None, mSec_max_threshold = None, sec_min_threshold = None, sec_max_threshold = None):
    df_time = df[df['response']!=0]
    
    if((mSec_min_threshold == None) and (sec_min_threshold == None) and (mSec_max_threshold == None) and (sec_max_threshold == None)):
        ## this whole first part is only if arguments are not specified.
        ## When called by another function, one of the thresholds should always be specified.
        if(sum(['mSecUsed' in df_time.columns])>0):
            print("Here's what the distribution of timing looks like in milliseconds for questions answered:")
            print(df_time['mSecUsed'].describe())
            print('There are ',df_time[df_time['mSecUsed']<=0].shape[0],' actual responses where time spent was 0 milliseconds or less.')
            
            minTime = int(input('What is the minimum time allowed in milliseconds? '))
            print('This will change ', df[df['mSecUsed'] <= minTime].shape[0], " responses to 'omit' status.")
            
            cont = input('Continue? Y/N :').lower()
            
            if(cont=='y'):
                df = recode_as_omitted(df, omit_condition = df['mSecUsed'] <= minTime)
            else:
                print('No reponse changed.')
        
        elif(sum(['secUsed' in df_time])>0):
            print("Here's what the distribution of timing looks like in seconds for questions answered:")
            print(df_time['secUsed'].describe())
            print('There are ',df_time[df_time['secUsed']<=0].shape[0],' actual responses where time spent was 0 seconds or less.')
            
            minTime = int(input('What is the minimum time allowed in seconds? '))
            print('This will change ', df[df['secUsed'] <= minTime].shape[0], " responses to 'omit' status.")
            
            cont = input('Continue? Y/N :').lower()
            
            if(cont=='y'):
                df = recode_as_omitted(df, omit_condition = df['secUsed'] <= minTime)
            else:
                print('No reponse changed.')
        
        else:
            print('no time field found')
    
    elif((mSec_min_threshold != None) and (sum(['mSecUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['mSecUsed'] <= mSec_min_threshold)
    elif((sec_min_threshold != None) and (sum(['secUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['secUsed'] <= sec_min_threshold)
    elif(mSec_min_threshold != None):
        warnings.warn('Data frame does not contain mSecUsed column. No questions recoded for minimum timing.')
    elif(sec_min_threshold != None):
        warnings.warn('Data frame does not contain secUsed column. No questions recoded for minimum timing.')
    
    if((mSec_max_threshold != None) and (sum(['mSecUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['mSecUsed'] > mSec_max_threshold)
    elif((sec_max_threshold != None) and (sum(['secUsed' in df_time.columns])>0)):
        df = recode_as_omitted(df, omit_condition = df['secUsed'] > sec_max_threshold)
    elif(mSec_max_threshold != None):
        warnings.warn('Data frame does not contain mSecUsed column. No questions recoded for maximum timing.')
    elif(sec_max_threshold != None):
        warnings.warn('Data frame does not contain secUsed column. No questions recoded for maximum timing.')
    
    return df


def remove_repeat_questions(df, rejects_df, remove = False, add_col = False):
    ## order by the times the person saw the question - dates on sequences.
    df['quesRank'] = df.groupby(by=['contentItemId', 'jasperUserId'])['dateCreated'].rank(method = 'first')
    
    if(remove==False):
        df = recode_as_omitted(df, omit_condition = (df['quesRank']>1))
        if(add_col == True):
            if(sum(df.columns=='repeatOmitted')==0):
                df['repeatOmitted'] = df['quesRank']>1
                
        outputdf = df.drop(columns = ['quesRank'])
    
    elif(remove == True):
        #adding data to rejects df
        temp_rej = df[df['quesRank'] > 1][['jasperUserId', 'contentItemId']].drop_duplicates()
        temp_rej.rename(columns={'contentItemId':'kbsEnrollmentId'}, inplace = True)
        temp_rej['Reason'] = '2nd or later instances of items for users removed'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        removed = df[df['quesRank'] > 1]
        df = df[df['quesRank']==1]
        outputdf = df.drop(columns = ['quesRank'])
    
    return outputdf, rejects_df

def combine_CIinfo(path, respdf, cidf = pd.DataFrame(), ci_cols_to_include = [], interaction_type_list = 1):
    
    #read in content info file
    if(cidf.empty == True):
        cidf = pd.read_csv(path + 'contentItemInfo.tsv', sep = '\t')
    
    #turn all initial letters in column headers to lowercse
    cidf.columns = [col[0].lower()+col[1:] for col in cidf.columns]
    
    cidf = cidf.drop_duplicates()
    
    cidf = cidf[cidf['interactionTypeId'].isin(interaction_type_list)]
    
    columnsToAdd = ['contentItemName', 'contentItemId']
    columnsToAdd = columnsToAdd + ci_cols_to_include
    
    columnsdf = cidf[columnsToAdd]
    
    outputdf = pd.merge(respdf, columnsdf)
    
    return outputdf

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


def clean_item_data(data_path = 'C:\\Users\\VImmadisetty\\Downloads\\',
                    results_path = 'C:\\Users\\VImmadisetty\\Downloads\\',
                    analysis_name = '_',
                    resp = pd.DataFrame(),
                    remove_users_deleted_sequences = True,
                    remove_dup_CIs = True,
                    remove_no_kbsEID = True,
                    remove_deleted_sequences = True,
                    remove_impo_response_scored = True,
                    remove_impo_timing_seq = True,
                    remove_seq_w_tmq = False,
                    remove_staged_responses = False,
                    remove_FT_items = False,
                    data_pool = dict(),
                    CI_remove_before_after = 'before',
                    repeat_treatment = 'omit',
                    mSec_min_threshold = None,
                    mSec_max_threshold = None,
                    sec_min_threshold = None,
                    sec_max_threshold = None,
                    remove_frt_users = True,
                    remove_olc_users = True,
                    remove_repeat_enrolls = True,
                    remove_tutor = True,
                    remove_ada_seq = True,
                    remove_untimed_seq = True,
                    remove_incomplete_seq = True,
                    seq_item_minutes_threshold = None,
                    seq_section_minutes_threshold = None,
                    seq_total_minutes_threshold = None,
                    qbank = False,
                    min_itmes_per_seq = None,
                    section_calc = True,
                    #seq_item_resp_threshold = None,
                    remove_unscored = False,
                    precombined_files = False,
                    remove_repeat_test_administrations = False,
                    remove_seq_wo_dispseq = True):
    
    CI_old_version_dates = data_pool.get('CI_old_version_dates', pd.DataFrame())
    CI_old_version_list = data_pool.get('CI_old_version_list', pd.DataFrame())
    CI_old_keys = data_pool.get('CI_old_keys', pd.DataFrame())
    frt_enrols = data_pool.get('frt_enrols', pd.DataFrame())
    olc_enrols = data_pool.get('olc_enrols', pd.DataFrame())
    repeaters = data_pool.get('repeaters', pd.DataFrame())
    section_map = data_pool.get('section_map', pd.DataFrame())
    test_map = data_pool.get('test_map', pd.DataFrame())
    seqHist_to_exclude = data_pool.get('seqHist_to_exclude', pd.DataFrame())
    cidf = data_pool.get('cidf', pd.DataFrame())
    field_test_items = data_pool.get('field_test_items', pd.DataFrame())
    ci_cols_to_include = data_pool.get('ci_cols_to_include', pd.DataFrame())
    
    if (test_map.empty and section_map.empty):
        sys.exit('No section_map or test_map')
    
    pre_ins = sys.stdout
    
    sys.stdout = open(results_path + analysis_name+'_Cleaning_info.txt', 'w')
    
    print('Starting clean item data function at ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    if(resp.empty):
        sys.exit('No response df')
    
    cleaning_info = pd.DataFrame(columns = ['Section', 'Sub_section', 'Condition',
                           'value', 'Users removed', 'Unique items removed', 'Responses removed'])
    
    rejects_df = pd.DataFrame(columns=['jasperUserId', 'kbsEnrollmentId', 'templateId', 'Reason'])
    
    num_seq_current = resp['sequenceId'].nunique()
    num_users_current = resp['jasperUserId'].nunique()
    num_items_current = resp['contentItemName'].nunique()
    num_responses_current = resp.shape[0]
    
    
    print('Total sequences at start: ', num_seq_current)
    print('Total users at start: ', num_users_current)
    print('Unique items at start: ', num_items_current)
    print('Total responses at start: ', num_responses_current)
    print('')
    
    #this needed if we want to analyse only sections from given df
    if (section_map.empty == False):  
        #adding data to rejects df
        temp_rej = resp[~resp['sectionName'].isin(section_map['jasperSectionName'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'section filtered from input'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        #filtering only sections that are in the section map
        respExcl = resp[resp['sectionName'].isin(section_map['jasperSectionName'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                    num_responses_current
                                                                                    ,things_to_say = 'Sequences without responses specified in the section map, removed: ')
        
    else:
        respExcl = resp
    
    print('Working on Disqualifiers :')
    print('='*30)
    print('User Removal')
    print('-'*20)
    sec = 'Disqualifiers'
    sub_sec = 'User Removal'
    respExcl['deleted_name'] = respExcl['sequenceName'].apply(lambda x: True if (re.search(r'\d', x) and re.search(r'_d$', x)) else False)

    users_w_deletes = respExcl[(respExcl['deleted_name']==True) | (respExcl['sequenceStatus']=='reset')][['jasperUserId',
                                                                                             'kbsEnrollmentId',
                                                                                             'sequenceStatus',
                                                                                             'deleted_name',
                                                                                             'sequenceName',
                                                                                             'templateId']].drop_duplicates().copy()
    
    #Users with any deleted requested sequence(s)
    if(remove_users_deleted_sequences == True):
        #adding data to rejects df
        temp_rej = respExcl[(respExcl['jasperUserId'].isin(users_w_deletes['jasperUserId'].unique()))][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Users with deleted requested sequence(s)'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        #below code removes entire users from response_data
        respExcl = respExcl[~(respExcl['jasperUserId'].isin(users_w_deletes['jasperUserId'].unique()))]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                                                             things_to_say = 'Sequences with deleted names are removed: ')
        
    
    
    #Users with sequences containing duplicate content items
    #Excluding sequences that have a single content item more than once (after filtering out tutorials/breaks/staged)
    if(remove_dup_CIs == True):
        seq_to_exclude_calc4 = respExcl[respExcl['contentItemId']!=-1].copy()
        seq_to_exclude_calc4['count'] = seq_to_exclude_calc4.groupby(by=['sequenceId', 'contentItemName'])['contentItemName'].transform('count')
        seq_to_exclude_calc4 = seq_to_exclude_calc4[seq_to_exclude_calc4['count']>1]['sequenceId'].unique()
    
        if(len(seq_to_exclude_calc4) > 0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc4)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences containing duplicate content items'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc4)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                , things_to_say = 'Sequences with dupe content items within the same exam, removed: ')
    
    #Users with no KBS EID
    #only enrollments are removed
    if (remove_no_kbsEID == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['kbsEnrollmentId']=='0'][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Enrollments with null Eid'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        respExcl = respExcl[respExcl['kbsEnrollmentId']!='0']
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with no KBS EID removed: ')
    
    #Users with non null KBS enrollment IDs and multiple Jasper/Other system user IDs
    #no code for this
    
    print('Sequence Removal')
    print('-'*20)
    sub_sec = 'Sequence Removal'
    #Sequence(s) sharing a template with a deleted sequence for that user
    if(remove_deleted_sequences == True):
        seq_deleted = respExcl[(respExcl['deleted_name']==True) | (respExcl['sequenceStatus']=='reset')]['sequenceId'].unique()
        
        if(len(seq_deleted)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_deleted)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with a deleted seq name'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_deleted)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with deleted names are removed: ')
    
    #Sequences with impossibly scored responses: response=0 and score=1
    if (remove_impo_response_scored == True):
        
        #making response = 0 for empty responses ang got score
        respExcl.loc[(respExcl['response'].isnull()) & (respExcl['score']==1), 'response'] = 0
        # Excluding sequences that have weird response records - scored as correct without a response
        seq_to_exclude_calc1 = respExcl[(respExcl['score']==1) & (respExcl['response'] == 0)]['sequenceId'].unique()
        
        if(len(seq_to_exclude_calc1)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc1)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with impossibly scored responses: response=0 and score=1'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc1)]
                            
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                , things_to_say = 'Sequences with bad records (response = 0 with score = 1), removed: ')
    
    #Sequences with impossible timing: mSecUsed < 0
    if(remove_impo_timing_seq == True):
        seq_to_exclude_calc1_5 = respExcl[respExcl['mSecUsed']<0]['sequenceId'].unique()
        if(len(seq_to_exclude_calc1_5)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc1_5)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with impossible timing: mSecUsed < 0'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc1_5)]
                            
        num_seq_current, num_users_current, num_items_current, num_responses_current, leaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                          num_responses_current
                                                                                , things_to_say = 'Sequences with bad timing (mSecUsed < 0), removed: ')
    
    
    initial_columns = respExcl.columns
    #Sequences with too many questions in a section (not applicable for QBank)
    #preparing the response data for applying above condition
    if (section_map.empty == False):
        if (qbank == True):
            warnings.warn('Why do you have a section map for qbank ?')
            section_map_df = pd.DataFrame({'sectionName' : section_map['jasperSectionName'],
                                          'test_minutes_allowed' : section_map['minutesAllowed'],
                                          'test_response_threshold' : section_map['responseThreshold']})
        else:
            section_map_df = section_map.rename(columns = {'jasperSectionName' : 'sectionName',
                                                          'minutesAllowed' : 'test_minutes_allowed',
                                                          'responseThreshold' : 'test_response_threshold'})
        
        #making cols test_minutes allowed, test_response_threshold on sections from section_map
        respExcl = pd.merge(respExcl, section_map_df, how = 'inner')
        if(respExcl.empty):
            warnings.warn("response df is empty after merging with section_map df")
            
        #respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sectionName'])['contentItemName'].transform('count')
    
    if (test_map.empty == False and qbank == False):
        ##prep to find sequences with bad records in them or too much time in a section, or multiple items seen in one test,
        # or too many items in a section(repeated positions) for total exclusion
        respExcl = pd.merge(respExcl, pd.DataFrame({'sequenceName' : test_map['jasperSequenceName'],
                                                    'test_minutes_allowed' : test_map['minutesAllowed'],
                                                    'test_num_ques' : test_map['numQues'],
                                                    'test_response_threshold' : test_map['responseThreshold']}))
        if(respExcl.empty):
            warnings.warn("response df is empty after merging with test_map df on 'sequenceName'")
                            
        #respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sequenceName'])['contentItemName'].transform('count')
                            
    elif (test_map.empty == False and qbank == True):
        temp_record_check = respExcl.shape[0]
        respExcl = pd.merge(respExcl, pd.DataFrame({'test_minutes_allowed' : test_map['minutesAllowed'],
                                            'test_num_ques' : test_map['numQues'],
                                            'test_response_threshold' : test_map['responseThreshold']}))
        if(respExcl.empty):
            warnings.warn("response df is empty after merging with test_map df")
            
        #respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sequenceName'])['contentItemName'].transform('count')
        
        if (temp_record_check != respExcl.shape[0]):
            sys.exit('Too many things in test_map, probably')
                            
    print('Here are the new columns after joining all the test and section maps')
    print(set(respExcl)^set(initial_columns))
    print('')
    
    respExcl['actualNumQues'] = respExcl.groupby(by = ['sequenceId', 'sequenceName'])['contentItemName'].transform('count')
        
    #Sequences with too many questions in a section (not applicable for QBank)
    if(remove_seq_w_tmq == True):
        if(qbank==False):
            seq_to_exclude_calc3 = respExcl[respExcl['actualNumQues'] > respExcl['test_num_ques']]['sequenceId'].unique()
            
            if(len(seq_to_exclude_calc3) > 0):
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc3)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with too many questions in a section'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc3)]
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = 'Sequences with too many questions in a section, removed: ')
        else:
            print('You are asking if there are too many questions for a qbank which is bad :(')
    
    print('Item Removal')
    print('-'*20)
    sub_sec = 'Item Removal'
    #num_item_responses = respExcl.shape[0]
    print('Total item responses: ', num_responses_current,'\n')
    
    #Staged response records (for CATs only)
    respExcl['attempted'] = np.where(respExcl['response'].isnull(), False, respExcl['response']!=0)
    
    if(remove_staged_responses == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['contentItemId']==-1][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Staged response records'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
        respExcl = respExcl[respExcl['contentItemId']!=-1] # these are staged records and do not represent a question that was viewed
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = 'Staged response records removed : ')
        
    
    sub_sec = 'Item Removal'
    #Field test / experimental items:
    if(remove_FT_items == True):
        if (field_test_items.empty == False):
            respExcl['FT'] = respExcl['contentItemName'].isin(field_test_items['contentItemName'])
            
            #adding data to rejects df
            temp_rej = respExcl[respExcl['FT']!=False][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Field test / experimental items'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
            respExcl = respExcl[respExcl['FT']==False]
            #removed items that represents field test items from responses
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                               num_responses_current
                                                                                    , things_to_say = 'Field Test items/Responses removed : ')
        
        else:
            print('No Field test items provided\n')
    
    print('Working on Cleaning Rules :')
    print('='*30)
    print('User Removal')
    print('-'*20)
    sub_sec = 'User Removal'
    #Users enrolled in specific products (Online companion, Free trials)
    if(remove_frt_users == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['kbsEnrollmentId'].isin(frt_enrols['kbsenrollmentid'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Users enrolled in Free Trials removed'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(frt_enrols['kbsenrollmentid'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with enrollments in "Free Trial" product are removed: ')
    
    if(remove_olc_users == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['kbsEnrollmentId'].isin(olc_enrols['kbsenrollmentid'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Users enrolled in Online companion removed'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(olc_enrols['kbsenrollmentid'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with enrollments in "Online Companion" product are removed: ')
    
    
    #Higher score guarantee or other repeat enrolls
    #We remove only erolls here not users
    if(remove_repeat_enrolls == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['kbsEnrollmentId'].isin(repeaters['kbsenrollmentid'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Sequences with repeated enrollments are removed'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[~respExcl['kbsEnrollmentId'].isin(repeaters['kbsenrollmentid'])]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with repeated enrollments are removed: ')
    
    #Total items taken fewer than set threshold removed (mostly for Qbank):
    #Fewer than ___ items attempted
    #Fewer than ___ %of items attempted across sequence
    #Fewer than ___ % of items attempted across sections(s)
    #have to make code for this
    
    
    print('Sequence Removal')
    print('-'*20)
    sub_sec = 'Sequence Removal'
    #Tutor mode sequences
    if(remove_tutor == True):
        #adding data to rejects df
        temp_rej = respExcl[(respExcl['tutorMode'] == True) | (respExcl['tutorMode'].notnull())][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Tutor mode sequences'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[(respExcl['tutorMode'] != True) | (respExcl['tutorMode'].isnull())]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Tutor mode sequences removed: ')
    
    
    #Sequences administered under ADA accommodations
    #No-need to worry on this as we don't get much in the data
    #but need to make code for this
    if(remove_ada_seq == True):
        print('make code for this to remove ADA enrollemnts\n')
    
    #Untimed sequences
    #have to make code for this
    if(remove_untimed_seq == True):
        respExcl['untimedSequence'] = respExcl['sequenceName'].str.lower().str.endswith('_untimed')
        
        #adding data to rejects df
        temp_rej = respExcl[respExcl['untimedSequence']==True][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Untimed sequences'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[respExcl['untimedSequence']==False]
        respExcl.drop(columns=['untimedSequence'], inplace = True)
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Untimed sequences removed: ')
    
    
    #Timed sequences
    #condition is not clear for this
    
    #Incomplete sequences
    if(remove_incomplete_seq == True):
        #adding data to rejects df
        temp_rej = respExcl[(respExcl['sequenceStatus'] != 4) | (respExcl['sequenceStatus'] != 'completed')][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Incomplete sequences'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[(respExcl['sequenceStatus'] == 4) | (respExcl['sequenceStatus'] == 'completed')]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Non Complete sequences removed: ')
    
    #Sequences with an item taking ___ minutes, or more
    if(seq_item_minutes_threshold):
        seq_item_min_thres = respExcl[respExcl['mSecUsed']>seq_item_minutes_threshold]['sequenceId'].unique()
        
        if(len(seq_item_min_thres)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_item_min_thres)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with Items taking longer than '+ str(seq_item_minutes_threshold) + ' minutes to complete, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_item_min_thres)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences with Items taking longer than '+ str(seq_item_minutes_threshold) + ' minutes to complete, removed: ')
        
    
    #Sequences with a section taking ___ minutes, or more
    #currently assuming that each section in a sequence will be given same criterion for time
    if(seq_section_minutes_threshold):
        seq_sec_min_thres = respExcl.copy()
        seq_sec_min_thres['section_time'] = seq_sec_min_thres.groupby(by=['sequenceId', 'sectionName'])['mSecUsed'].transform('sum')/60000
        seq_sec_min_thres = seq_sec_min_thres[seq_sec_min_thres['test_sum_time']> seq_section_minutes_threshold]['sequenceId'].unique()
        
        if(len(seq_sec_min_thres)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_min_thres)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with sections taking longer than '+ str(seq_section_minutes_threshold) + ' minutes to complete, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_min_thres)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences with sections taking longer than '+ str(seq_section_minutes_threshold) + ' minutes to complete, removed: ')
        
    
    #Sequences taking longer than ___ minutes, defined as
    if(seq_total_minutes_threshold):
        #exclude sequences that took longer than specified time to complete
        seq_min_thres = respExcl.copy()
        seq_min_thres['test_sum_time'] = seq_min_thres.groupby(by=['sequenceId'])['mSecUsed'].transform('sum')/60000
        seq_min_thres = seq_min_thres[seq_min_thres['test_sum_time']> seq_total_minutes_threshold]['sequenceId'].unique()
        
        if(len(seq_min_thres)>0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_min_thres)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences taking longer than '+ str(seq_total_minutes_threshold) + ' minutes to complete, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_min_thres)]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences taking longer than '+ str(seq_total_minutes_threshold) + ' minutes to complete, removed: ')
        
    
    #Sequences with fewer items than set threshold removed:
    #Fewer than ___ items attempted
    #Fewer than ___ % of items attempted in a sequence
    #Fewer than ___ % of items attempted in a section(s)
    
    if(section_map.empty == False):
        
        #get calculations across entire pool of questions
        respExcl[['overall_raw_correct','overall_num_attempted']] = respExcl.groupby(['jasperUserId'])[['score', 'attempted']].transform('sum')
        respExcl['temp_uniq_items'] =  respExcl.groupby(by=['jasperUserId'])['contentItemName'].transform('nunique')
        respExcl['overall_pTotal'] = respExcl['overall_raw_correct']/respExcl['temp_uniq_items'] #dividing by total number of unique questions in this section
        respExcl['overall_pPlus'] = respExcl['overall_raw_correct']/respExcl['overall_num_attempted']
        respExcl.drop(columns = ['temp_uniq_items'], inplace = True)
            
        #get all sequence level calculations
        respExcl[['template_raw_correct','template_num_attempted']] = respExcl.groupby(['sequenceId'])[['score', 'attempted']].transform('sum')
        respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['actualNumQues'] #total questions on a single exam across all sections
        respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']

        #get all the calculations at the section level
        respExcl['section_num_omitted'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['attempted'].transform(lambda x: sum(x!=True))
        respExcl['section_num_attempted'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['attempted'].transform('sum')
        respExcl['section_perc_attempted'] = respExcl['section_num_attempted']/respExcl['sectionNumQues']
        respExcl['section_raw_correct'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['score'].transform('sum')
        respExcl['section_num_scored'] = respExcl.groupby(by=['sequenceId', 'sectionName'])['scored'].transform('sum')
        respExcl['section_pTotal'] = respExcl['section_raw_correct']/respExcl['sectionNumQues']
        respExcl['section_pPlus'] = respExcl['section_raw_correct']/respExcl['section_num_attempted']
            
        if(qbank == True):
            #Fewer than ___ % of items attempted in a sequence
            if(min_items_per_seq == None):
                print('No minimum item threshold provided for this qbank.\n')
            else:
                seq_below_resp_threshold = respExcl[respexcl['template_num_attempted'] < min_items_per_seq]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_belo_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than '+ str(min_items_per_seq*100) + '% of items attempted in a sequence, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_belo_resp_threshold)]
                
                #calculate overall sequence order after all cleaning is complete
                #only needed here coz of qbank
                seq_order_df = respExcl[['jasperUserId', 'sequenceId', 'dateCreated']].drop_duplicates()
                seq_order_df['actual_sequence_order'] =  seq_order_df.groupby(by=['jasperUserId'])['dateCreated'].rank(method = 'first')
                
                respExcl = pd.merge(respExcl, seq_order_df)
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences with items fewer than '+ str(min_items_per_seq*100) + '% of items attempted in a sequence, removed')
            
        else: # if qbank==False
            if(section_map.empty == False):
                total_qs = sum(section_map['sectionNumQues'])
                #create output_df_list variable here
            else:
                if(test_map.empty == False):
                    total_qs = sum(test_map['numQues'])
                else:
                    warnings.warning('No test_map or section_map')
            
            #Fewer than ___ % of items attempted in a section(s)
            ##response threshold filter - first use section map if any, otherwise use response threshold
            if(len(section_map['min_items_per_seq']>0)):
                temp_df = pd.merge(respExcl, pd.DataFrame({'sectionName': section_map['jasperSectionName'],
                                                                           'min_items_per_seq' : section_map['min_items_per_seq']}))
                seq_below_resp_threshold = temp_df[(temp_df['section_num_attempted'] < temp_df['min_items_per_seq']) | (temp_df['template_num_attempted'] < sum(section_map['min_items_per_seq']))]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a section'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences removed because one or more sections were under the threshold of attempted questions: ')
            
            #Fewer than ___ % of items attempted across sections(s)
            elif(section_calc == True):
                seq_below_resp_threshold = respExcl[respExcl['section_perc_attempted'] < respExcl['section_response_threshold']]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted across sections'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                rspExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                num_seq_current, num_users_current, num_items_current = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                             num_responses_current,
                                                                                 things_to_say = 'Sequences removed because one or more sections were under the threshold of attempted questions: ')
                
            elif(section_calc == False):
                seq_below_resp_threshold = respExcl[respExcl['section_perc_attempted']<respExcl['test_response_threshold']]['sequenceId'].unique() #this should come from test_map
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a sequence, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences removed because one or more sections were under the threshold of attempted questions: ')
                
            if(section_map.empty == False):
                #create a output_df_list df with data for each section from section_map
                print('section_map is False & section_separated = True\n')
            else:
                #store total data from respexcl in output_df_list at index 1
                print('all out of section_map\n')
            
    
    elif(test_map.empty == False):
        if(qbank == True):
            #sequences with less than a pre-determined number of valid sequence responses in each of the sections (considered separately) will be excluded
            
            #get all sequence level calculations
            respExcl[['template_raw_correct','template_num_attempted']] = respExcl.groupby(['sequenceId'])[['score', 'attempted']].transform('sum')
            print('Sequence level sums complete')
            
            respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['actualNumQues'] #total questions on a single exam across all sections
            respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']
            
            if(min_items_per_seq==None):
                print('No minimum item threshold provided for this qbank.','\n')
            else:
                #Fewer than ___ % of items attempted in a sequence
                seq_below_resp_threshold = respExcl[respExcl['template_num_attempted'] < min_items_per_seq]['sequenceId'].unique()
                
                #adding data to rejects df
                temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
                temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a sequence, removed'
                rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
                
                respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
                
                seq_order_df = respExcl[['jasperUserId', 'sequenceId', 'dateCreated']].drop_duplicates()
                seq_order_df['actual_sequence_order'] =  seq_order_df.groupby(by=['jasperUserId'])['dateCreated'].rank(method = 'first')
                
                respExcl = pd.merge(respExcl, seq_order_df)
                num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                                   num_responses_current,
                                                                                 things_to_say = 'Sequences removed under the threshold of attempted items: ')
            
            number_of_unique_CIs = respExcl['contentItemName'].nunique()
            respExcl[['overall_raw_correct', 'overall_num_attempted']] = respExcl.groupby(by=['jasperUserId'])[['score', 'attempted']].transform('sum')
            
            respExcl['overall_pTotal'] = respExcl['overall_raw_correct']/number_of_unique_CIs  #divide by total number of unique questions in this section
            respExcl['overall_pPlus'] = respExcl['overall_raw_correct']/respExcl['overall_num_attempted']
            
            seq_order_df = respExcl[['jasperUserId', 'sequenceId', 'dateCreated']].drop_duplicates()
            seq_order_df['actual_sequence_order'] =  seq_order_df.groupby(by=['jasperUserId'])['dateCreated'].rank(method = 'first')
                
            respExcl = pd.merge(respExcl, seq_order_df)
            
        
        elif(qbank == False):
            
            #sequences with less than a pre-determined number of valid responses will be excluded
            respExcl['template_num_omitted'] = respExcl.groupby(by=['sequenceId'])['attempted'].transform(lambda x: sum(x!=True))
            respExcl['template_num_attempted'] = respExcl.groupby(by=['sequenceId'])['attempted'].transform('sum')
            respExcl['template_perc_attempted'] = respExcl['template_num_attempted']/respExcl['test_num_ques']
            respExcl['template_raw_correct'] = respExcl.groupby(by=['sequenceId'])['score'].transform('sum')
            respExcl['template_pTotal'] = respExcl['template_raw_correct']/respExcl['test_num_ques']
            respExcl['template_pPlus'] = respExcl['template_raw_correct']/respExcl['template_num_attempted']
            
            #Fewer than ___ % of items attempted across sections(s)
            seq_below_resp_threshold = respExcl[respExcl['template_perc_attempted'] < respExcl['test_response_threshold']]['sequenceId'].unique()
            
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_below_resp_threshold)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Sequences with items fewer than __% of items attempted in a sequence, removed'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_below_resp_threshold)]
            
            num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                         num_responses_current,
                                                                                 things_to_say = 'Sequences with items fewer than __% of items attempted in a sequence, removed :')
            
        
        else:
            warnings.warn('Parameter qbank was not true or false? somehow?')
            
    else:
        warnings.warn('No section_map or test_map!!!')
    
    
    #Sequences with invalid/missing recorded score elements:
    #Percent correct
    #Scaled Score(s)
    #Theta/Penalty Theta
    
    sec = 'Extras'
    sub_sec = 'Item Removal'
    #Extras
    # remove unscored items if requested, otherwise do nothing. defaults to doing nothing.
    if(remove_unscored == True):
        #adding data to rejects df
        temp_rej = respExcl[respExcl['scored']!=1][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'unscored items'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
        respExcl = respExcl[respExcl['scored']==1]
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Unscored item responses removed : ')
    
    
    sub_sec = 'Sequence Removal'
    #remove sequences if given in list
    if (seqHist_to_exclude.empty == False):
        
        #adding data to rejects df
        temp_rej = respExcl[respExcl['sequenceId'].isin(seqHist_to_exclude['sequenceId'])][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
        temp_rej['Reason'] = 'Sequences removed from a given list'
        rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
        respExcl = respExcl[~respExcl['sequenceId'].isin(seqHist_to_exclude['sequenceId'])]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences input from list, removed : ')
    
    #adding content_item info to response df
    if(precombined_files == False):
        #add content item info
        if not (cidf.empty):
            if not (ci_cols_to_include.empty):
                respExcl = combine_CIinfo(data_path, respExcl, cidf = cidf, ci_cols_to_include = ci_cols_to_include, interaction_type_list = interaction_type_list)
            else:
                respExcl = combine_CIinfo(data_path, respExcl, cidf = cidf, interaction_type_list = interaction_type_list)
        else:
            respExcl = combine_CIinfo(data_path, respExcl, interaction_type_list = interaction_type_list)
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Number of sequences removed during content item join (should be 0): ')
    
    sub_sec = 'Sequence Removal'
    #Remove sequences for the users who practiced more than once
    if(remove_repeat_test_administrations == True):
        seq_to_exclude_calc5 = respExcl.copy()
        seq_to_exclude_calc5['num_ques'] = seq_to_exclude_calc5.groupby(by=['jasperUserId', 'sequenceName', 'sequenceId', 'dateCreated'])['contentItemName'].transform('count')
        seq_to_exclude_calc5 = seq_to_exclude_calc5[['jasperUserId', 'sequenceName', 'sequenceId', 'dateCreated']].drop_duplicates()
        seq_to_exclude_calc5['sequence_order'] = seq_to_exclude_calc5.groupby(by=['jasperUserId', 'sequenceName'])['dateCreated'].rank(method = 'first')
        seq_to_exclude_calc5 = seq_to_exclude_calc5[seq_to_exclude_calc5['sequence_order']>1]['sequenceId'].unique()
        
        if(len(seq_to_exclude_calc5) > 0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc5)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Remove sequences for the users who practiced more than once'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
        
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc5)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences that were not the first administration for the user, removed: ')
    
    sub_sec = 'Sequence Removal'
    if(remove_seq_wo_dispseq == True):
        seq_to_exclude_calc6 = respExcl.copy()
        seq_to_exclude_calc6 = seq_to_exclude_calc6[seq_to_exclude_calc6['displaySeq'].isnull()]['sequenceId'].unique()
        
        if(len(seq_to_exclude_calc6) > 0):
            #adding data to rejects df
            temp_rej = respExcl[respExcl['sequenceId'].isin(seq_to_exclude_calc6)][['jasperUserId', 'kbsEnrollmentId', 'templateId']].drop_duplicates()
            temp_rej['Reason'] = 'Remove sequences for the users who have display sequence null for the items'
            rejects_df = pd.concat([rejects_df, temp_rej], ignore_index = True)
            
            respExcl = respExcl[~respExcl['sequenceId'].isin(seq_to_exclude_calc6)]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current
                                                                                    , things_to_say = 'Sequences with items that have display sequence is null, removed :')
    
    
    
        print('Response / Score Re-coding')
    print('-'*20)
    sec = 'Cleaning Rules'
    sub_sec = 'Response/Score Re-coding'
    
    #2nd and later instances of a repeated item coded as omitted
    remove_value = False
    if (repeat_treatment not in ['omit', 'remove', 'ignore']):
        print("Unknown repeat_treatment value. Allowed values include 'omit','remove', and 'ignore'. Repeat treatment is skipped for now. Repeated questions are recorded as omit by default.")
    elif(repeat_treatment == 'omit'):
        remove_value = False
    elif(repeat_treatment == 'remove'):
        remove_value = True
    elif(repeat_treatment == 'ignore'):
        remove_value = None
    print('Remove repeat item responses, instead of recoding as omitted = ', remove_value)
    
    num_items_omitted = respExcl[respExcl['attempted'] == False].shape[0]
    num_seq_w_omitted = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
    num_users_w_omitted = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
    num_items_omitted_new = 0
    num_seq_w_omitted_new = 0
    num_users_w_omitted_new = 0
    print('Original number of responses omitted', num_items_omitted)
    print('Original number of seq w items omitted', num_seq_w_omitted)
    print('Original number of users w items omitted', num_users_w_omitted, '\n')
    
    
    #Items with inconsistent keying/suggesting multiple versions of answer key coded as omitted
    #treating old version of items based on dates
    if (CI_old_version_dates.empty == False):
        # If items were under an earlier version the response should be recoded as omitted
        if(sum(CI_old_version_dates.columns == 'contentItemName')>0):
            for row, val in enumerate(CI_old_version_dates['contentItemName']):
                
                if(CI_remove_before_after=='before'):
                    date_cond = respExcl['dateCreated'] < CI_old_version_dates['cutoff_date'].loc[row]
                elif(CI_remove_before_after=='after'):
                    date_cond = respExcl['dateCreated'] > CI_old_version_dates['cutoff_date'].loc[row]
                            
                respExcl = recode_as_omitted(respExcl,
                                            omit_condition = ((respExcl['contentItemName']==CI_old_version_dates['contentItemName'].loc[row]) & (date_cond)))
        
        elif(sum(CI_old_version_dates.columns == 'contentItemId')>0):
            for row, val in enumerate(CI_old_version_dates['contentItemId']):
                
                date_cond = respExcl['dateCreated'] < CI_old_version_dates['cutoff_date'].loc[row]
                respExcl = recode_as_omitted(respExcl,
                                            omit_condition = ((respExcl['contentItemId']==CI_old_version_dates['contentItemId'].loc[row]) & (date_cond)))
        
        num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        print('Item responses under previous version marked as omitted: ', num_items_omitted_new - num_items_omitted)
        print('Affected sequences : ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users : ', num_users_w_omitted_new - num_users_w_omitted, '\n')
        
        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
        
    
    #treating old version of items based on item_ids
    if (CI_old_version_list.empty == False):
        # If items were under an earlier version the response should be recoded as omitted
        for row, val in enumerate(CI_old_version_list['contentItemId']):
            respExcl = recode_as_omitted(respExcl,
                                       omit_condition = respExcl['contentItemId']==CI_old_version_list['contentItemId'].loc[row])
        
        num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        print('Item responses under previous version (from id list) marked as omitted: ', num_items_omitted_new - num_items_omitted)
        print('Affected sequences : ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users : ', num_users_w_omitted_new - num_users_w_omitted, '\n')
        
        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
    
    #treating items answered with old version keys
    if (CI_old_keys.empty == False):
        # If items are scored from an earlier answer key, the response should be recoded as omitted
        for row, val in enumerate(CI_old_keys['contentItemId']):
            #elig = respExcl[(respExcl['contentItemId']==CI_old_keys['contentItemId'].loc[row]) & 
            #                                              (respExcl['response'].fillna('None')==CI_old_keys['correctAnswer'].fillna('None').loc[row])]
            #elig_cnt = elig.shape[0]
            #omit_already = elig[elig['attempted']==False].shape[0]
            #print('Total matching', elig_cnt)
            #print('Already in omitted state for these rows', omit_already)
    
            respExcl = recode_as_omitted(respExcl,
                                        omit_condition = ((respExcl['contentItemId']==CI_old_keys['contentItemId'].loc[row]) & 
                                                          (respExcl['response'].fillna('None')==CI_old_keys['correctAnswer'].fillna('None').loc[row])))
        num_items_omitted_new = respExcl[respExcl['attempted']==False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted']==False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted']==False]['jasperUserId'].nunique()
        
        print('Already might have removed in earlier rules or already in omit state')
        print('Item responses with previous version of answer key marked as omitted: ', num_items_omitted_new - num_items_omitted)                                     
        print('Affected sequences: ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users: ', num_users_w_omitted_new - num_users_w_omitted, '\n')
    
        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
    
    num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                       num_responses_current,
                                                                                 things_to_say = 'Number of sequences removed during item exclusions: ')
    
    
    
    
    #2nd and later instances of a repeated item coded as omitted
    if (remove_value!=None):
        respExcl, rejects_df = remove_repeat_questions(respExcl, rejects_df, remove = remove_value, add_col = True)
        
        num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
        num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
        num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        
        print('Repeated items marked as omitted: ', num_items_omitted_new - num_items_omitted)
        print('Current number of responses omitted: ', num_items_omitted_new)
        print('Current number of seq w items omitted: ', num_seq_w_omitted_new)
        print('Current number of users w items omitted: ', num_users_w_omitted_new)
                                             
        print('Affected sequences: ', num_seq_w_omitted_new - num_seq_w_omitted)
        print('Affected users: ', num_users_w_omitted_new - num_users_w_omitted, '\n')

        num_items_omitted = num_items_omitted_new
        num_seq_w_omitted = num_seq_w_omitted_new
        num_users_w_omitted = num_users_w_omitted_new
                                             
        marked_omit_items = respExcl[respExcl['response'] != respExcl['orig_response']]
        
        num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                           num_responses_current,
                                                                                     things_to_say = 'Sequences with 2nd and later instances of a repeated item coded as omitted : ')
    
    
    #Last seen response coded as omitted, subsequent responses as not-reached
    #need to find code for this
    
    #Items with time "under" or "over" given threshold of seconds coded as omitted
    respExcl = timing_exclusion(respExcl, mSec_min_threshold = mSec_min_threshold, sec_min_threshold = sec_min_threshold,
                               mSec_max_threshold = mSec_max_threshold, sec_max_threshold = sec_max_threshold)
    #Responses given in less than the threshold allowed will be recoded as omitted
    num_items_omitted_new = respExcl[respExcl['attempted'] == False].shape[0]
    num_seq_w_omitted_new = respExcl[respExcl['attempted'] == False]['sequenceId'].nunique()
    num_users_w_omitted_new = respExcl[respExcl['attempted'] == False]['jasperUserId'].nunique()
        
    print('Item response time under threshold of ', mSec_min_threshold, ' mSec or over ', mSec_max_threshold, ' mSec, marked as omitted: ', num_items_omitted_new - num_items_omitted)
    print('Current number of responses omitted: ', num_items_omitted_new)
    print('Current number of seq w items omitted: ', num_seq_w_omitted_new)
    print('Current number of users w items omitted: ', num_users_w_omitted_new)
                                             
    print('Affected sequences: ', num_seq_w_omitted_new - num_seq_w_omitted)
    print('Affected users: ', num_users_w_omitted_new - num_users_w_omitted,'\n')
    num_items_omitted = num_items_omitted_new
    num_seq_w_omitted = num_seq_w_omitted_new
    num_users_w_omitted = num_users_w_omitted_new
    
    num_seq_current, num_users_current, num_items_current, num_responses_current, cleaning_info = removed_record_count(respExcl, cleaning_info, sec, sub_sec, num_seq_current, num_users_current, num_items_current,
                                                                                                                       num_responses_current,
                                                                                     things_to_say = 'Sequences with Items with time "under" or "over" given threshold of seconds coded as omitted : ')
                                                                                     
                                                                                     
    print('Remaining number of responses in final output: ', respExcl.shape[0])
    print('Remaining number of sequences in final output: ', num_seq_current)
    print('Remaining number of users in final output: ', num_users_current)
    print('Remaining number of unique items in final output: ', num_items_current)
    print('Cleaning function time completed: ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    cleaning_info.set_index(['Section', 'Sub_section', 'Condition'], inplace = True)
    
    sys.stdout = pre_ins
    
    f = open(results_path + analysis_name+'_Cleaning_info.txt', 'r')
    file_contents = f.read()
    print (file_contents)
    f.close()
    
    #renaming colum names
    respExcl.columns = [col.replace('jasperUser', 'student').replace('sequence', 'activity') for col in respExcl.columns ]
    rejects_df.columns = [col.replace('jasperUser', 'student').replace('sequence', 'activity') for col in rejects_df.columns ]
    
    return respExcl, cleaning_info, rejects_df