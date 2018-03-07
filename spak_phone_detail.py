
# coding: utf-8

# # spark 设置

# In[6]:


import os
os.environ['PYSPARK_DRIVER_PYTHON']='python3'
os.environ['PYSPARK_PYTHON']='python3'
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("locals[*]").setAppName("spark_phone_detail")


# In[147]:


def toCSVLine(data):
  return ','.join(str(d) for d in data)


# # 探索文件结构

# ## 文件残缺检验

# In[24]:


import json
from  hashlib import md5
from functools import reduce
import pandas as pd
def funcs(X):
    X_json = json.loads(X)
        #key = ''.join(X_json['task_data'].keys())
        #hashobj = md5(''.join(X_json['task_data'].keys()).encode('utf8')).hexdigest()
    try:
        return X_json['channel_attr'],X_json['channel_code'],
    except Exception as e:
        return e

mergeFile = open('phone_detail.txt','r')

mergeFile.close()


# ## Call_info

# In[ ]:


mergeFile = open('phone_detail.txt','r')
raw_json = mergeFile.readline()
A_json = json.loads(raw_json)
call_list = [x['call_record'] for x in A_json['task_data']['call_info']]
ret_all = reduce(lambda x,y:x+y,call_list)
pd_df = pd.DataFrame(ret_all)
mergeFile.close()
pd_df.head()


# ## account_info

# In[ ]:


mergeFile_account = open('phone_detail.txt','r')
account_list = []
while  True:
    raw_json = mergeFile_account.readline()
    if raw_json:
        A_json = json.loads(raw_json)
        account_list.append(A_json['task_data']['account_info'])
    else:
        break
mergeFile_account.close()
pd_dfaccount = pd.DataFrame(account_list)
pd_dfaccount.head()


# ## sms_info 

# In[ ]:


mergeFile_sms = open('phone_detail.txt','r')

raw_json = mergeFile_sms.readline()
A_json = json.loads(raw_json)
call_list = [x['sms_record'] for x in A_json['task_data']['sms_info']]
ret_all = reduce(lambda x,y:x+y,call_list)
pd_dfsam = pd.DataFrame(ret_all)
pdsms11 = pd_dfsam[pd_dfsam.msg_other_num.str.len()==11]
print(pdsms11.shape,pdsms11.msg_other_num.nunique())
mergeFile_sms.close()
pdsms11.head(5)


# ## 通信号码种类
# * 我们仅仅需要这些用户的自然人通话记录
call_address:
    1，未知
call_other_number
    1,662 短号，家庭号
    2，未知
    3，95533
    4，4007777777
    5，051280837576
    6，9521290206
    7，664070
    8，10085
    9，158********
    10，125909888218
    11，12583113737263903
## 地址聚合 作为顶点属性：
   非手机号码，聚合分类作为个数

自己给自己打电话，这个这么处理？

# # spark 处理

# ## 加载，转化通话记录

# In[3]:


path_prefix = 'hdfs://192.180.3.43:9000/user/app/'


# In[4]:


#one_log = sc.textFile('hdfs://192.170.3.163:9000/backup/2017_11_02_04_35_11-19292.debug.log')
phone_detail = sc.textFile(path_prefix+'merge_201606.txt')


# In[5]:


import json
from functools import reduce
def first_func(item):
    user_mobile = None
    try:
        A_json = json.loads(item)
        user_mobile = A_json.get('user_mobile')
        call_list = [x['call_record'] for x in A_json['task_data']['call_info']]
        all_call = reduce(lambda x,y:x+y,call_list)
        all_call = [((item.get('call_other_number'),item.get('call_start_time')),
                     (item.get('call_address'),int(item.get('call_time')),item.get('call_type_name'),user_mobile))
                    for item in all_call]
        return user_mobile,all_call
    except Exception as e:
        with open('/home/app/zx_error.log','a') as f:
            f.writelines(user_mobile+','+str(e)+'\n')
        return user_mobile,[]


# In[6]:


def remove_null(item):
    return bool(item.strip())


# In[7]:


rdd1 = phone_detail.filter(remove_null)


# In[8]:


rdd11 = rdd1.map(first_func).filter(lambda x:bool(x[1]))


# In[9]:


#根据用户手机号合并统一用户不同日期文件
rdd2 = rdd11.reduceByKey(lambda x,y:list(dict(x+y).items()))
rdd2.persist()


# ### 统计个人通话次数，利用等个人属性

# In[10]:


def person_attr_func(item):
    #print(item[0],end=';')
    add_list = [address for k,(address,*other) in item[1]]
    address_dict={k:add_list.count(k) for k in set(add_list)}
    call_count = len(item[1]) if len(item[1])>0 else 0.00001
    calltime_sum = sum([int(times) for k,(a,times,*b) in item[1]])
    calltime_mean = calltime_sum/call_count
    
    return item[0],call_count,calltime_sum,calltime_mean,address_dict
    

rdd21 = rdd2.map(person_attr_func)


# In[19]:


rdd21.repartition(1).saveAsTextFile(path_prefix+'person_attr4.txt')


# In[12]:


#from pyspark.sql import SparkSession
from pyspark.sql.types import *
#spark = SparkSession(sc)


# In[13]:


schemaString= "user_mobile call_count calltime_sum calltime_mean address_dict"
TypeString = [StringType(),IntegerType(),IntegerType(),FloatType(),StringType()]
fields = [StructField(field_name, Type, True) for field_name,Type in zip(schemaString.split(),TypeString)]
schema = StructType(fields)
# Apply the schema to the RDD.
people_attr_df = spark.createDataFrame(rdd21, schema)


# ## 去除无用通话记录

# ### 去除通话列表无关属性

# In[18]:


rdd24 = rdd2.flatMap(lambda x:x[1])
rdd25 = rdd24.map(lambda x:(x[0][0],int(x[1][1]),x[1][2],x[1][3]))
rdd25.persist()


# ### 统计通话记录号码性质

# In[19]:


schemaString= "call_other_number call_time call_type_name use_mobile"
TypeString = [StringType(),IntegerType(),StringType(),StringType()]
fields = [StructField(field_name, Type, True) for field_name,Type in zip(schemaString.split(),TypeString)]
schema = StructType(fields)
# Apply the schema to the RDD.
phonelist_df = spark.createDataFrame(rdd25, schema)
# Creates a temporary view using the DataFrame
phonelist_df.createOrReplaceTempView("phonelist")
# SQL can be run over DataFrames that have been registered as a table.
numstat_df = spark.sql("SELECT substring(call_other_number,0,3) otherphone_3,length(call_other_number) len FROM phonelist")
numstat_df.createOrReplaceTempView("numstat")
ret1 = spark.sql('select otherphone_3,len,count(*) cnt from numstat where len = 11  group by otherphone_3,len order by otherphone_3,cnt ')
ret2 = spark.sql('select * from numstat where substring(otherphone_3,0,1)="1" and len = 11')


# ### 自己给自己打电话记录数量占比

# In[20]:


rdd251 = rdd25.filter(lambda x:x[0]==x[3])
#rdd251.collect()


# ### 开头不为1的电话记录数量占比

# In[21]:


rdd252 = rdd25.filter(lambda x:x[0][0]!='1')
#rdd252.count()


# ### 长度不是11位的

# In[22]:


rdd253 = rdd25.filter(lambda x:len(x[0])!=11)
#rdd253.count()


# ### Type 不是主叫被叫的

# In[23]:


rdd254 = rdd25.filter(lambda x:x[2] not in ('被叫','主叫'))
#rdd254.take(40)


# In[1732]:


rdd254.map(lambda x:x[2]).distinct().count()


# ### 去除非人类记录，保留合法号码

# In[24]:


def remove_nonhumanlist_func(item):
     not(((item[0][0])=='1') or (len(item[0])==11) or (item[0]==item[3]))


# In[25]:


rdd26 = rdd25.filter(lambda item:(((item[0][0]=='1') and (len(item[0])==11)
                                   and (item[0]!=item[3]) and (item[1]>0))))


# ### 检查自己给自己打电话记录详情

# In[26]:


def flat_func2(item):
    num_list = [(k[0],item[0],k,v) for k,v in item[1]]
    return set(num_list)

rdd2a = rdd2.flatMap(flat_func2)

rdd2b = rdd2a.filter(lambda x:x[0]==x[1])

#rdd2a.take(10)


# ### 检查  相互打电话重复记录

# In[27]:


def ori_des_rep_check(x):
    return ((x[0]=='15876143316')&(x[1]=='13621455614'))|((x[1]=='15876143316')&(x[0]=='13621455614'))
rdd2C1 = rdd2a.filter(ori_des_rep_check)

#rdd2C1.count()

rdd2C2 = rdd2C1.map(lambda x:(x[1],x[2],x[3]))

rdd2C2.persist()

def addjust_order(item):
    phone1,(phone2,dt),(area,times,Type) = item
    file_phone = phone1
    if Type == '被叫':
        phone1,phone2=phone2,phone1
    return file_phone,phone1,phone2,dt,area,times,Type
    
rdd2C3 = rdd2C2.map(addjust_order)


# In[75]:



#result = rdd2C3.collect()

df_result = pd.DataFrame(result,columns=['file_phone','phone1','phone2','dt','area','times','Type'])

#pd.set_option('display.max_rows',100)

df_result.sort_values('dt')


# ## 生成有向边属性

# ### 调整RDD内容

# In[28]:


def edge_adjust_func(item):
    phone1,phone2 = (item[3],item[0]) if item[2]=='主叫' else (item[0],item[3])
    return (phone1,phone2,item[3]),(item[1],1)
    
rdd3 = rdd26.map(edge_adjust_func)
rdd3.persist()


# ### 边聚合去重统计

# In[29]:


# 分文件聚合两个人的通话记录
rdd31 = rdd3.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
#rdd31.take(3)


# In[30]:


# 按通话记录(区分主被叫)聚合通话记录
rdd32 = rdd31.map(lambda x:((x[0][0],x[0][1]),x[1]))
#rdd32.take(6)


# In[31]:


## 去除不同文件重复记录
rdd33 = rdd32.reduceByKey(lambda x,y:x if x[1]>y[1] else y)
rdd34 = rdd33.map(lambda x:(x[0][0],x[0][1],x[1][0],x[1][1]))


# ### 找出有重复记录数据

# In[32]:


rdd321 = rdd32.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>1)


# In[1788]:


### 生成点边 DataFrame


# In[33]:


schemaString= "src dst calltime_sum calltime_cnt"
TypeString = [StringType(),StringType(),IntegerType(),IntegerType()]
fields = [StructField(field_name, Type, True) for field_name,Type in zip(schemaString.split(),TypeString)]
schema = StructType(fields)
# Apply the schema to the RDD.
edge_df = spark.createDataFrame(rdd34, schema)


# In[34]:


edge_df.persist()


# # 整理个人属性

# In[15]:


sparkdf  = spark.read.json(path_prefix+'merge_201606.txt')
people_rawdf = sparkdf[['user_mobile','real_name','identity_code','channel_src','channel_attr','created_time','channel_type','channel_code',]]
#peopledf.createOrReplaceTempView("people")
peopledf = people_rawdf.join(people_attr_df,on='user_mobile')
peopledf.persist()


# In[17]:


peopledf.repartition(1).write.save(path_prefix+'peopeledf.csv',format='csv')


# In[35]:


edge_df.repartition(1).write.save(path_prefix+'edge_df.csv',format='csv')


# In[36]:


edge_df.count()


# In[37]:


peopledf.count()


# In[14]:


from pyspark.sql.types import *


# In[88]:


def get_schema(schemaString,typeString):
    #schemaString= "src dst calltime_sum calltime_cnt"
    dict_getType = {'int':IntegerType(),'string':StringType(),'float':FloatType()}
#TypeString = [StringType(),StringType(),IntegerType(),IntegerType()]
    fields = [StructField(field_name, dict_getType.get(Type), True) 
              for field_name,Type in zip(schemaString.split(' '),typeString.split(' '))]
    schema = StructType(fields)
    return schema


# In[89]:


get_schema("src dst calltime_sum calltime_cnt","int string int int")


# In[39]:


col_names ='id,real_name,identity_code,channel_src,channel_attr,created_time,channel_type,channel_code,call_count,calltime_sum,calltime_mean,address_dict'
col_types = 'string,string,string,string,string,string,string,string,int,int,float,string'

people_schema = get_schema(col_names,col_types)

peopeledf = spark.read.csv(path_prefix+'peopeledf.csv',schema=people_schema)


# In[27]:


edge_col_name = 'src,dst,calltime_sum,calltime_cnt'
edge_col_types = 'string,string,int,int'
edge_schema = get_schema(edge_col_name,edge_col_types)


# In[28]:


edgedf = spark.read.csv(path_prefix+'edge_df.csv',schema=edge_schema)


# In[40]:


subpeopledf = peopeledf.select(['id'])

