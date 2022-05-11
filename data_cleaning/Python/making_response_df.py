from salvador import resp_cleaning, get_item_cor_ans
from flask import Flask, render_template, flash, redirect, url_for

import pandas as pd
import numpy as np
import re


def making_response_df(response_df, activity_df, content_df, test_map = pd.DataFrame(), return_summary = False):
        ####
        ####Working on response_df
        response_df['item_submitted_timestamp'] = pd.to_datetime(response_df['item_submitted_timestamp'], errors = 'coerce')
        response_df.rename(columns = {'section_title':'section_name',
                             'milliseconds_used' : 'm_sec_used',
                             'is_scored' : 'scored',
                             'scored_response' : 'score',
                             'item_status': 'response_status',
                             'item_section_position' : 'display_seq'}, inplace = True)

        response_df.drop(columns = ['item_position', 'field_test',
                           'milliseconds_review_total', 'milliseconds_review_explanation'], inplace = True)
    
        response_df['source_system'] = np.where(pd.isnull(response_df['source_system']), response_df['source_system'], response_df['source_system'].astype('str').str.lower())

        response_df['history_db_id'] = np.where(pd.isnull(response_df['history_db_id']), response_df['history_db_id'], response_df['history_db_id'].astype('str').str.lower())

        #making student_id truly str
        response_df['student_id'] = np.where(pd.isnull(response_df['student_id']), response_df['student_id'], response_df['student_id'].astype('str').str.lower())

        #making activity_id truly string
        response_df['activity_id'] = np.where(pd.isnull(response_df['activity_id']), response_df['activity_id'], response_df['activity_id'].astype('str').str.lower())

        #making content_item_name lower case in response_df to merge with content_item_info
        response_df['content_item_name'] = np.where(pd.isnull(response_df['content_item_name']), response_df['content_item_name'], response_df['content_item_name'].astype('str').str.lower())

        ######
        ######Working on activity_df
        activity_df['timestamp_created'] = pd.to_datetime(activity_df['timestamp_created'], errors = 'coerce')
        activity_df['timestamp_completed'] = pd.to_datetime(activity_df['timestamp_completed'], errors = 'coerce')
        
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
        activity_df['source_system'] = np.where(pd.isnull(activity_df['source_system']), activity_df['source_system'], activity_df['source_system'].astype('str').str.lower())

        #this will handle error for Nan values while converting to int64
        activity_df['history_db_id'] = np.where(pd.isnull(activity_df['history_db_id']), activity_df['history_db_id'], activity_df['history_db_id'].astype('str').str.lower())

        #making student id truly string to avoid issues while merging with reponse_df
        activity_df['student_id'] = np.where(pd.isnull(activity_df['student_id']), activity_df['student_id'], activity_df['student_id'].astype('str').str.lower())

        #making student id truly string to avoid issues while merging with reponse_df
        activity_df['activity_id'] = np.where(pd.isnull(activity_df['activity_id']), activity_df['activity_id'], activity_df['activity_id'].astype('str').str.lower())

        #making below to lower case, to avoid issues while merging with test_df
        activity_df['activity_name'] = np.where(pd.isnull(activity_df['activity_name']), activity_df['activity_name'], activity_df['activity_name'].astype('str').str.lower())
        activity_df['enrollment_id'] = np.where(pd.isnull(activity_df['enrollment_id']), activity_df['enrollment_id'], activity_df['enrollment_id'].astype('str').str.lower())
      
        ####
        ####Working on content_item_df
        content_df.rename(columns = {'correct_answer':'corr_ans_cidf',
                            'last_modified' : 'item_last_modified'}, inplace = True)

        content_df[['content_item_name', 'content_item_type',
        'interaction_type_name', 'source_system']] = content_df[['content_item_name', 'content_item_type',
                                                                     'interaction_type_name', 'source_system']].apply(lambda x: np.where(pd.isnull(x), x, x.astype(str).str.lower()))

        content_df = content_df[['content_item_name', 'content_item_type', 'interaction_type_name', 'count_choices', 'corr_ans_cidf', 'item_last_modified']].drop_duplicates(ignore_index = True)
        
        #getting latest details for each item or making content_df to contain each for only each item
        #From content_item_df, we'll get only latest data for count_choices, correct_answer
        #we'll make correct answer columns again based on response_df
        idx = content_df.groupby(by = ['content_item_name'])['item_last_modified'].transform('max')==content_df['item_last_modified']
        content_df = content_df[idx]



        ####
        #### Merging response_df with activity_df
        flash(f'Size of response_df before merge with activity_df: {str(response_df.shape)}<br/>\
        #students in response_df before merging with activity_df : {response_df.student_id.nunique()}<br/>\
        #activities in response_df before merging with activity_df : {response_df.activity_id.nunique()}<br/>\
        #students in activity_df : {activity_df.student_id.nunique()}<br/>\
        #activities in activity_df : {activity_df.activity_id.nunique()}', 'message')

        response_df = pd.merge(response_df, activity_df, on = ['source_system', 'history_db_id', 'student_id', 'activity_id'], how = 'inner')

        if(response_df.empty):
            flash('Response df became empty after merging with activity_df, Check!!', 'alert-danger')
        
        else:
            flash(f'Size of response_df after merging with activity_df: {response_df.shape}<br/>\
            #students in response_df after merging with activity_df : {response_df.student_id.nunique()}<br/>\
            #activities in response_df after merging with activity_df : {response_df.activity_id.nunique()}', 'message')
    
        ####
        #### Merging with content_df
        flash(f'Size of response_df before merge with content_df: {str(response_df.shape)}<br/>\
        #students in response_df before merging with content_df : {response_df.student_id.nunique()}<br/>\
        #activities in response_df before merging with content_df : {response_df.activity_id.nunique()}<br/>\
        #content items in response_df before merging with content_df : {response_df.content_item_name.nunique()}<br/>\
        #Items in content_df : {content_df.content_item_name.nunique()}', 'message')

        response_df = pd.merge(response_df, content_df, on = ['content_item_name'], how = 'inner')

        if(response_df.empty):
            flash('Response df became empty after merging with content_df, Check!!', 'alert-danger')
        else:
            flash(f'Size of response_df after merging with content_df: {response_df.shape}<br/>\
            #students in response_df after merging with content_df : {response_df.student_id.nunique()}<br/>\
            #activities in response_df after merging with content_df : {response_df.activity_id.nunique()}<br/>\
            #content items in response_df after merging with content_df : {response_df.content_item_name.nunique()}', 'message')
        
        
        if not test_map.empty:
            test_map['activity_name'] = test_map['activity_name'].str.lower()
        
            flash(f'Size of response_df before merge with test_map: {str(response_df.shape)}<br/>\
            #students in response_df before merging with test_map : {response_df.student_id.nunique()}<br/>\
            #activities in response_df before merging with test_map : {response_df.activity_id.nunique()}', 'message')

            response_df = pd.merge(response_df, test_map, on = ['activity_name'], how = 'inner')

            if(response_df.empty):
                flash('Response df became empty after merging with test_map, Check!!', 'alert-danger')
        
            else:
                flash(f'Size of response_df after merging with test_map: {response_df.shape}<br/>\
                #students in response_df after merging with test_map : {response_df.student_id.nunique()}<br/>\
                #activities in response_df after merging with test_map : {response_df.activity_id.nunique()}', 'message')
        
        #Below will avoid null values to getting converted to strings and rest will be turned to string and lower case
        response_df[['section_name', 'content_item_id', 'content_item_name',
            'interaction_type', 'response_status', 'program',
            'product_code', 'product_name', 'activity_name',
            'activity_title', 'activity_type', 'activity_status',
            'content_item_type', 'interaction_type_name']] = response_df[['section_name', 'content_item_id', 'content_item_name',
                                                                          'interaction_type', 'response_status', 'program',
                                                                          'product_code', 'product_name', 'activity_name',
                                                                          'activity_title', 'activity_type', 'activity_status',
                                                                          'content_item_type', 'interaction_type_name']].apply(lambda x: np.where(pd.isnull(x), x, x.astype(str).str.lower()))
        
        #making response as string before feeding to functions
        response_df['raw_response'] = np.where(pd.isnull(response_df['raw_response']), response_df['raw_response'], response_df['raw_response'].astype('str').str.lower())

        #below for multiple choice, SATA or any that allows to select choices
        #converts to numerics & orders them
        response_df.loc[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in'])), 'response'] = response_df[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in']))]['raw_response'].apply(lambda x: resp_cleaning(x, re_order = True) if not pd.isna(x) else x)

        #For order-interactions,numerical fill-ins
        #Converts to numerics for order-interactio & order-match and strip
        #Converts to lower case for text-entry-ineraction & strip
        #Coverts to string & strip for numerical-fill-in
        response_df.loc[(response_df['interaction_type'].isin(['order-interaction', 'order-match'])), 'response'] = response_df[(response_df['interaction_type'].isin(['order-interaction', 'order-match']))]['raw_response'].apply(lambda x: resp_cleaning(x, re_order = False) if not pd.isna(x) else x)
        response_df.loc[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in'])), 'response'] = response_df[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in']))]['raw_response'].apply(lambda x: x.lower().strip() if not pd.isna(x) else x)

        response_df['corr_ans_cidf'] = np.where(pd.isnull(response_df['corr_ans_cidf']), response_df['corr_ans_cidf'], response_df['corr_ans_cidf'].astype('str').str.lower())


        response_df.loc[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in'])), 'corr_ans_cidf'] = response_df[~(response_df['interaction_type'].isin(['order-interaction', 'order-match', 'text-entry-interaction', 'numerical fill- in']))]['corr_ans_cidf'].apply(lambda x: resp_cleaning(x, re_order = True) if not pd.isna(x) else x)

        response_df.loc[(response_df['interaction_type'].isin(['order-interaction', 'order-match'])), 'corr_ans_cidf'] = response_df[(response_df['interaction_type'].isin(['order-interaction', 'order-match']))]['corr_ans_cidf'].apply(lambda x: resp_cleaning(x, re_order = False) if not pd.isna(x) else x)
        response_df.loc[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in'])), 'corr_ans_cidf'] = response_df[(response_df['interaction_type'].isin(['text-entry-interaction', 'numerical fill- in']))]['corr_ans_cidf'].apply(lambda x: x.lower().strip() if not pd.isna(x) else x)

        response_df[['m_sec_used', 'scored', 'score']] = response_df[['m_sec_used', 'scored', 'score']].astype('float64')

        
        #making sure date_created, date_completed are in datetime format
        response_df['date_created'] = pd.to_datetime(response_df['date_created'], errors = 'coerce')
        response_df['date_completed'] = pd.to_datetime(response_df['date_completed'], errors = 'coerce')

        #summary to from response df to make test map
        response_summary = response_df[response_df['activity_status'].str.lower() == 'completed'].groupby(['activity_name', 'student_id'], as_index = False, dropna = False).agg(num_responses = ('content_item_name', 'nunique'))\
        .groupby(['activity_name'], dropna = False, as_index = False).agg(min_resp = ('num_responses', 'min'),
                                            median_resp = ('num_responses', 'median'),
                                            max_resp = ('num_responses', 'max'),
                                            num_users = ('num_responses', 'count'))
        
        #getting correct answer and ci_old_keys
        corr_ans, CI_old_keys = get_item_cor_ans(response_df)
        #merging correct answer with correct_answer df
        response_df = pd.merge(response_df, corr_ans[['content_item_name', 'correct_answer']], on = ['content_item_name'], how = 'left')
        #if by chance not found correct answer from response df then we rely on correct_answer found from content_item_df
        response_df.loc[response_df['correct_answer'].isnull(), 'correct_answer'] = response_df[response_df['correct_answer'].isnull()]['corr_ans_cidf']

        #This should be run after making correct_answer column
        #filling response_df based on response status
        response_df.loc[(response_df['response_status']=='responded') & (response_df['score']==0) & (response_df['response'].isnull()), 'response'] = 'unknown'
        response_df.loc[(response_df['response_status']=='responded') & (response_df['score']==1) & (response_df['response'].isnull()), 'response'] = response_df[(response_df['response_status']=='responded') & (response_df['score']==1) & (response_df['response'].isnull())]['correct_answer']
        #manipulating response_status if its null based on respnose & score
        response_df.loc[(response_df['response_status'].isnull()) & (response_df['response'].isnull()) & (response_df['score'].isnull()), ['response_status', 'score']] = ['not-reached', 0]
        
        if return_summary:
            return response_summary
        else:
            return response_df, CI_old_keys