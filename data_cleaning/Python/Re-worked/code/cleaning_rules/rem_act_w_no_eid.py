import libs

#enroll_id comes from activity_file and it has one row for each activity
#so removing 0 eids will remove entile activity data

def rem_act_w_no_eid(respExcl = libs.pd.DataFrame()):
    respExcl = respExcl[(respExcl['enrollment_id']!='0') & (respExcl['enrollment_id']!='') & (respExcl['enrollment_id'].notnull())]

    return respExcl
