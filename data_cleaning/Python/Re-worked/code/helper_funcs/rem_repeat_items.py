import libs

#below will omit/remove 2nd or later instances of items that are seen by a student based on item id
def rem_repeat_items(respExcl = libs.pd.DataFrame(), remove = False, add_col = False):

    #2nd and later instances of a repeated item coded as omitted
    #remove_value = False
    #if (repeat_treatment not in ['omit', 'remove', 'ignore']):
    #    print("Unknown repeat_treatment value. Allowed values include 'omit','remove', and 'ignore'. Repeat treatment is skipped for now. Repeated questions are recorded as omit by default.")
    #elif(repeat_treatment == 'omit'):
    #    remove_value = False
    #elif(repeat_treatment == 'remove'):
    #    remove_value = True
    #elif(repeat_treatment == 'ignore'):
    #    remove_value = None
    #print('Remove repeat item responses, instead of recoding as omitted = ', remove_value)

    ## order by the times the person saw the question - dates on sequences.
    respExcl['item_rank'] = respExcl.groupby(by=['content_item_id', 'student_id'])['date_created'].rank(method = 'first')
    
    if(remove == False):
        respExcl = libs.recode_as_omitted(respExcl, omit_condition = (respExcl['item_rank']>1))
        if(add_col == True):
            if(sum(respExcl.columns=='repeat_omitted')==0):
                respExcl['repeat_omitted'] = respExcl['item_rank']>1
                
        respExcl.drop(columns = ['item_rank'], inplace = True)
    
    elif(remove == True):
        removed = respExcl[respExcl['item_rank'] > 1]
        respExcl = respExcl[respExcl['item_rank']==1]
        respExcl.drop(columns = ['item_rank'], inplace = True)
    
    return respExcl