
# coding: utf-8

# In[2]:


from pyspark.sql.types import *
import json
from functools import reduce
path_prefix = 'hdfs://192.180.3.43:9000/phone_detail/'
phone_detail = sc.textFile(path_prefix+'*.txt')


# In[5]:


import time


# In[84]:


pdetail_201705=sc.textFile(path_prefix+'merge201705.txt')


# In[4]:


def get_schema(schemaString,typeString):
    dict_getType = {'int':IntegerType(),'string':StringType(),'float':FloatType(),'bool':BooleanType()}
    fields = [StructField(field_name, dict_getType.get(Type), True) 
              for field_name,Type in zip(schemaString.split(' '),typeString.split(' '))]
    schema = StructType(fields)
    return schema


# In[85]:


def remove_null(item):
    return bool(item.strip())
rdd1 = pdetail_201705.filter(remove_null)


# In[86]:


attrs = 'user_mobile real_name identity_code channel_src channel_attr created_time channel_type channel_code,file_people'
attrs_type = 'string string string string string string string string bool'
people_schema = get_schema(attrs,attrs_type)

def get_people_basic_attr(item):
    try:
        itemj = json.loads(item)
        items_list = [itemj.get(i) for i in attrs.split(' ')]
        items_list[-1] = True
        return itemj.get('user_mobile'),tuple(items_list)
    except Exception as e:
        raise e
        return None,None
def reduce_repeat_item(x,y):
    if x is None:
        return y
    if y is None:
        return x
    return x
rdd40 = rdd1.map(get_people_basic_attr)
rdd41 = rdd40.filter(lambda x:x[0] is not None)
rdd42 = rdd41.reduceByKey(reduce_repeat_item).map(lambda x:x[1])
people_raw_df = spark.createDataFrame(rdd42, people_schema)

#people_raw_df.persist()


# In[78]:


import pyspark


# In[87]:


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
        #raise e
#         with open('/home/app/zx_error.log','a') as f:
#             f.writelines(+str(e)+'\n')
        with open('/home/app/'+str(time.time())[:10],'w') as f2:
            f2.write(item)
        return user_mobile,[]

#根据用户手机号合并统一用户不同日期文件
rdd11 = rdd1.map(first_func).filter(lambda x:bool(x[1]))
rdd2 = rdd11.reduceByKey(lambda x,y:list(dict(x+y).items()))
# ## 去除无用通话记录
# ### 去除通话列表无关属性
rdd24 = rdd2.flatMap(lambda x:x[1])
rdd25 = rdd24.map(lambda x:(x[0][0],int(x[1][1]),x[1][2],x[1][3]))
#rdd25.persist()
# ### 统计通话记录号码性质
# # ### 自己给自己打电话记录数量占比
# # ### 开头不为1的电话记录数量占比
# # ### 长度不是11位的
# # ### Type 不是主叫被叫的
# rdd254 = rdd25.filter(lambda x:x[2] not in ('被叫','主叫'))
# ### 去除非人类记录，保留合法号码


# In[89]:


rdd11.persist()


# In[98]:


def remove_nonhumanlist_func(item):
     not(((item[0][0])=='1') or (len(item[0])==11) or (item[0]==item[3]))
rdd26 = rdd25.filter(lambda item:(((item[0][0]=='1') and (len(item[0])==11)
                                   and (item[0]!=item[3]) and (item[1]>0))))
# ### 检查自己给自己打电话记录详情
# # ### 检查  相互打电话重复记录
#     if Type == '被叫':
# ## 生成有向边属性
# ### 调整RDD内容
def edge_adjust_func(item):
    phone1,phone2 = (item[3],item[0]) if item[2]=='主叫' else (item[0],item[3])
    return (phone1,phone2,item[3]),(item[1],1)
rdd3 = rdd26.map(edge_adjust_func)
#### 边聚合去重统计


# In[106]:


# 分文件聚合两个人的通话记录
rdd31 = rdd3.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
# 按通话记录(区分主被叫)聚合通话记录


# In[107]:


rdd32 = rdd31.map(lambda x:((x[0][0],x[0][1]),x[1]))


# In[100]:


## 去除不同文件重复记录#去除主被叫重复记录的问题

rdd33 = rdd32.reduceByKey(lambda x,y:x if x[1]>y[1] else y)
rdd34 = rdd33.map(lambda x:(x[0][0],x[0][1],x[1][0],x[1][1]))
rdd34.persist()

# ### 找出有重复记录数据
### 生成点边 DataFrame
edgeString= "src dst calltime_sum calltime_cnt"
edgeTString = "string string int int"
edgeschema = get_schema(edgeString,edgeTString)
edge_df = spark.createDataFrame(rdd34, edgeschema)
#edge_df.persist()
# # 整理个人属性


# In[20]:


edge_df.count()


# In[11]:


# 主叫时间统计
rdd341 = rdd34.map(lambda x:(x[0],(x[2],x[3])))
rdd342 = rdd341.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd343 = rdd342.map(lambda x:(x[0],x[1][0],x[1][1]))
callingschema= "user_mobile callingtime_sum calling_cnt"
callingType = "string int int"
schema = get_schema(callingschema,callingType)
calling_df = spark.createDataFrame(rdd343, schema)

# 被叫时间统计
rdd344 = rdd34.map(lambda x:(x[1],(x[2],x[3])))
rdd345 = rdd344.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd346 = rdd345.map(lambda x:(x[0],x[1][0],x[1][1]))
calledschema= "user_mobile calledtime_sum called_cnt"
calledType = "string int int"
schema = get_schema(calledschema,calledType)
called_df = spark.createDataFrame(rdd346, schema)

# 主叫被叫合并统计
rdd346 = rdd342.union(rdd345)
rdd347 = rdd346.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd348 = rdd347.map(lambda x:(x[0],x[1][0],x[1][1]))
callschema= "user_mobile calltime_sum call_cnt"
callType = "string int int"
schema = get_schema(callschema,callType)
call_df = spark.createDataFrame(rdd348, schema)

call_df1 = call_df.join(calling_df,on='user_mobile',how='left')
call_df2 = call_df1.join(called_df,on='user_mobile',how='left')


# In[15]:


# ### 统计个人通话次数，利用等个人属性
def person_attr_func(item):
    add_list = [address for k,(address,*other) in item[1]]
    address_dict={k:add_list.count(k) for k in set(add_list)}
    call_count = len(item[1]) if len(item[1])>0 else 0.00001
    calltime_sum = sum([int(times) for k,(a,times,*b) in item[1]])
    calltime_mean = calltime_sum/call_count    
    return item[0],call_count,calltime_sum,calltime_mean,address_dict   
rdd21 = rdd2.map(person_attr_func)
rdd21.persist()
schemaString= "user_mobile call_count calltime_sum calltime_mean address_dict"
TypeString = "string int int float string"
schema = get_schema(schemaString,TypeString)
people_attr_df = spark.createDataFrame(rdd21, schema)


# In[16]:


people_df = call_df2.join(people_raw_df,on='user_mobile',how='left')


# In[ ]:


#peopledf.persist()
people_df.repartition(1).write.save(path_prefix+'people_dfnew.csv',format='csv')
edge_df.repartition(1).write.save(path_prefix+'edge_dfnew.csv',format='csv')

