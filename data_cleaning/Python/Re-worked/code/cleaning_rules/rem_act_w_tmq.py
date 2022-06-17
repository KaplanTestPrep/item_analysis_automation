import libs

def rem_act_w_tmq(respExcl = libs.pd.DataFrame(), qbank = False):

    if(qbank == False):
        act_to_exclude = respExcl[respExcl['actual-num_ques'] > respExcl['test_num_ques']]['activity_id'].unique()

        if(len(act_to_exclude) > 0):
            respExcl = respExcl[~respExcl['sequenceId'].isin(act_to_exclude)]
    
    else:
        print('You are asking too many questions in a question bank which is bad!!')
    
    return respExcl