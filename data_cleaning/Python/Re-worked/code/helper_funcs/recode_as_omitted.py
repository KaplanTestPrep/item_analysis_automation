import warnings
import libs

def recode_as_omitted(respExcl = libs.pd.DataFrame(), omit_condition = libs.pd.Series(dtype = 'float')):
    if (omit_condition.empty):
        warnings.warn('No omit_condition provided.')

    if(sum(omit_condition.isnull())>0):
        omit_condition[omit_condition.isnull()] = False
        warnings.warn('Null omit condition defaulted to False')

    if(len(omit_condition)>0):
        #change responses to 0
        respExcl.loc[omit_condition, 'raw_response'] = 0
        #change score to 0
        respExcl.loc[omit_condition, 'score'] = 0
        #change attempted to False
        respExcl.loc[omit_condition, 'attempted'] = False
        #change responseStatus to 'omitted' if it is null
        respExcl.loc[omit_condition,  'response_status'] = 'omitted'
        # & (df['attempted']==False) & (df['responseStatus'].isnull()
    else:
        warnings.warn('Nothing recoded due to entirely False omit_condition')
        
    return respExcl