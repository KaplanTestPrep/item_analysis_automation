
#There a flaw in below code that if response is more than one digit, then sorting ends up messy
def resp_cleaning(resp, re_order = True):
    #output of this function will be string
    resp_list = str(resp).split('|')
    cln_resp_list = []
    for resp_part in resp_list:
        #an error may rise here if the response is a string
        #add np.sort after ','.join function to include sorting feature
        #to remove space around each choice input
        resp_part = ','.join([x.strip() for x in resp_part.split(',')])
        if re_order:
            cln_resp_part = ','.join(np.sort(list(set([str(ord(x.lower())-96) if x.isalpha() else x for x in resp_part.split(',')]))))
        else:
            cln_resp_part = ','.join(list(dict.fromkeys([str(ord(x.lower())-96) if x.isalpha() else x for x in resp_part.split(',')])))
            
        cln_resp_list.append(cln_resp_part)
        
    cln_resp_list = [x.strip() for x in cln_resp_list]
    final_resp = '|'.join(cln_resp_list)
    
    return final_resp.strip()