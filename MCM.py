import pandas as pd
import datetime
import locale




#######################connection to tracker########################

col_value_1=[]
col_value_2=[]
row_value_1=[]



previous_date = (datetime.datetime.today()-datetime.timedelta(days=2)).strftime('%Y-%m-%d')
curr_date = (datetime.datetime.today()).strftime('%Y-%m-%d')
print(previous_date)

previous_date = '2020-08-23'
curr_date = '2020-08-25'
previous_split = previous_date.split("-")
previous_datekey = previous_split[0]+previous_split[1]+previous_split[2]
print(previous_datekey)

curr_split = curr_date.split("-")
curr_datekey = curr_split[0]+curr_split[1]+curr_split[2]
print(curr_datekey)


import warnings
warnings.simplefilter('ignore')



'''
datelag=sys.argv[1]

print(datelag)
date = datetime.datetime.now()-datetime.timedelta(days=int(datelag))

curr = date.strftime("%Y-%m-%d")
curr_split = curr.split("-")
curr_date = int(curr_split[0]+curr_split[1]+curr_split[2])
print(curr_date)
'''



path2 = "/home/anem.phani/analytics/DMC/output/"
    
##########################Non Large Overall#########################
#########################Speed related metrics######################

def cache():
    global col_value_1,col_value_2, row_value_1
    col_value_1 = wks.col_values(1)
    col_value_2 = wks.col_values(2)
    row_value_1 = wks.row_values(1)     

def search_range(cut):
    global row_count
    temp = row_count
    for i in range(1,temp):
        if col_value_1 [i] == cut:
            for j in range(i+1,temp):
                if col_value_1[j] != '':
                    return i,j
                if j + 1 == temp:
                    return i,j + 1

def row_num(cut,cut_1):
    temp = search_range(cut)
    for i in range(temp[0],temp[1] + 1):
        if col_value_2[i] == cut_1:
            return i + 1


def col_num_overall_1():
    temp=wks.col_count
    for i in range(temp):
        if row_value_1[i] == previous_date:
            return i+1
    for i in range(temp):
        if row_value_1[i] != previous_date:
            print ("no_column")


def connection(sheet_name):
    
    global wks,row_count,col_num_overall,sheet   
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/home/anem.phani/analytics/hldm.json', scope)
    client = gspread.authorize(creds)
    #import gspread
    #gc =  gspread.service_account(filename='/home/anem.phani/analytics/hldm.json')
    gsheet = client.open('Marketing Campaign metrics')
    #sheet = client.open("Grocery -  COVID19 2020")
    wks = gsheet.worksheet(sheet_name)
    row_count = wks.row_count
    print(row_count)
    cache()
    





#print (col_num_overall)

def makecolname(num):
     col=''
     while(num>0):
         init=num%26
         if(init==0):
             col=col+'Z'
             num=int(num/26)-1
         else:
             col=col+chr((int(init) - 1) + ord('A'))
             num=int(num/26)
     s1 = ''.join(reversed(col))
     return(s1)

sheets = ['Grocery Shoppers (OO)','Grocery browsers (ON)','Plus+TPC>3']
cohorts = ['A','B','C']

for i in range(len(sheets)):
    print("started")
    print(i)
    
    sheet = sheets[i]
    connection("Metrics")
    coh = cohorts[i]
    '''
    values = []
    for value in row_value_1:
        split_values= value.split("-")      
        print(split_values)
        values.append(int(split_values[0]+split_values[1]+split_values[2]))
            
        print(values)
    '''    
    col_num_overall=col_num_overall_1()
    
    colname = makecolname(col_num_overall)
    range1 = colname+'1'+':'+colname+'1000'
    cell_list1 = wks.range(range1)
    
    
    traffic = pd.read_csv(path2+"traffic"+curr_date+".csv")
    traffic = traffic.drop_duplicates()
    print(traffic.head())
    print(traffic['datekey'].unique())
    
    traffic = traffic[traffic['mp_id']=='HYPERLOCAL']
    transaction = pd.read_csv(path2+"trasaction"+curr_date+".csv")
    transaction = transaction.drop_duplicates()
    
    traffic_data = traffic[traffic['cohort']==coh]
    transaction_data = transaction[transaction['cohort']==coh]
    

    for i in range(0,2):
        if i==0:
            coh_type = "test"
            colname=makecolname(col_num_overall)
            range1=colname+'1'+':'+colname+'87'
            cell_list1=wks.range(range1)
    
        if i==1:
            coh_type = "control"
            colname=makecolname(col_num_overall+1)
            range1=colname+'1'+':'+colname+'87'
            cell_list1=wks.range(range1)   
    
        print(coh_type)
        
        traff = traffic_data[traffic_data['cohort_type']==coh_type]
        visits = traff['session_id'].nunique()
        visitors = traff['visitor_id'].nunique()
        new_visitors = traff[traff['new_visitor_flag']==1]['visitor_id'].nunique()
        v_uv = visits/visitors
        
        trans = transaction_data[transaction_data['cohort_type']==coh_type]
        total_customers = trans['account_id'].nunique()
        new_customers = trans[(trans['new_hl_customer_flag']=='ON')|(trans['new_hl_customer_flag']=='NN')]['account_id'].nunique()
        try:
            nc = round((new_customers/total_customers)*100,2)
        except:
            nc = 0
    
        print(col_value_1)
        print(sheet)
        cell_list1[row_num(sheet,"Visits")-1].value = visits
        cell_list1[row_num(sheet,"Unique Visitors")-1].value = visitors
        cell_list1[row_num(sheet,"New Unique Visitors")-1].value = new_visitors
        cell_list1[row_num(sheet,"Visits/Unique Visitors")-1].value = round(v_uv,2)
        cell_list1[row_num(sheet,"Total customers")-1].value = total_customers
        cell_list1[row_num(sheet,"% New customers")-1].value = nc
        
        
        for i in cell_list1:
            try:
                i.value=round(locale.format("%f", float(i.value), grouping=True),2)
            except:
                i.value=str(i.value)
                
        wks.update_cells(cell_list1)
