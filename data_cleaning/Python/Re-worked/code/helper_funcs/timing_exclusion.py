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