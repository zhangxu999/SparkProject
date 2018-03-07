
# coding: utf-8

# In[1]:


from pysparkarkarkarkark import StorageLevel
from pyspark.sql.types import *
import json
from functools import reduce
hdfs_workdir = 'hdfs://192.180.3.43:9000/'
path_prefix = hdfs_workdir+ 'phone_detail/'
import time


# In[2]:


fileall = path_prefix+'*.txt'


# In[4]:


people_attrs = 'user_mobile real_name identity_code channel_src channel_attr created_time channel_type channel_code file_people'
people_attrs_type = 'string string string string string string string string bool'

rdd_pdfile = sc.textFile(fileall).filter(lambda x:bool(x.strip()))


# In[5]:


def get_schema(schemaString,typeString):
    dict_getType = {'int':IntegerType(),'string':StringType(),'float':FloatType(),'bool':BooleanType()}
    fields = [StructField(field_name, dict_getType.get(Type), True) 
              for field_name,Type in zip(schemaString.split(' '),typeString.split(' '))]
    schema = StructType(fields)
    return schema


# In[6]:


error_cnt = sc.accumulator(0)

load_error = sc.accumulator(0)

def load2json(item):
    global load_error
    global error_cnt
    try:
        itemjson = json.loads(item)
        #return itemjson.get('user_mobile'), itemjson
    except Exception as e:
        load_error += 1
        return None,(None,[])
    ple_attrs = [itemjson.get(i) for i in people_attrs.split(' ')]
    ple_attrs[-1] = True
    all_call = []
    try:
        call_list = [x['call_record'] for x in itemjson['task_data']['call_info']]
        all_call = reduce(lambda x,y:x+y,call_list)
        all_call = [((x.get('call_other_number'),x.get('call_start_time')),
                     (x.get('call_address'),int(x.get('call_time')),x.get('call_type_name')))#,user_mobile)
                    for x in all_call]
    except Exception as e:
        error_cnt +=1
        with open('/home/app/errorlines.txt','a') as f2:
            f2.write(item)
        with open('/home/app/'+'error_records.log','a') as f3:
            f3.write(str(itemjson.get('user_mobile'))+':::::::::'+str(e)+'\n')
        all_call = []
    return itemjson.get('user_mobile'), (ple_attrs, all_call)
rdd_jsonfile = rdd_pdfile.map(load2json).filter(lambda x:bool(x[0]))


# In[7]:


rdd_jsonfile.persist(storageLevel=StorageLevel(True,True,True,False,1))


# In[8]:


def people_attr_func(item):
    items_list = [item.get(i) for i in people_attrs.split(' ')]
    items_list[-1] = True
    return items_list
rddx1 = rdd_jsonfile.mapValues(lambda x:x[0])
def remove_repeate_func(x,y):
    func = lambda a:a is not None
    x_len = len(list(filter(func,x))) 
    y_len = len(list(filter(func,y)))
    ret_item = x if x_len >=y_len else y
    if (x[5] is None) or (y[5] is None):
        ret_item[5] = x[5] or y[5]
        return ret_item
    if x[5] > y[5]:
        ret_item[5] = y[5]
        return ret_item
    return ret_item

rddx2 = rddx1.reduceByKey(remove_repeate_func)
people_schema = get_schema(people_attrs,people_attrs_type)
people_raw_df = spark.createDataFrame(rddx2.values(), people_schema)
people_raw_df.persist()


# In[9]:


rddx3 = rdd_jsonfile.mapValues(lambda x:x[1])#.filter(lambda x:bool(x[1]))

#根据用户手机号合并统一用户不同日期文件
rddx4 = rddx3.reduceByKey(lambda x,y:list(dict(x+y).items()))
# ## 去除无用通话记录
# ### 去除通话列表无关属性
rddx5 = rddx4.flatMapValues(lambda x:x)

def remove_nonhumanlist_func(item):
    # 对方电话号码首位是1#长度为11#主叫被叫不是同一个电话号码#通话时长大于0
    return (item[1][0][0][0]=='1')  and (len(item[1][0][0])==11)  and (item[0]!=item[1][0][0]) and (item[1][1][1]>0) 
rddx6 = rddx5.filter(remove_nonhumanlist_func)


# In[10]:


def edge_adjust_func(item):
    '''
    调整主被叫位置
    return 主叫，被叫，源文件）（时间，次数）
    '''
    phone1,phone2 = (item[0],item[1][0][0]) if item[1][1][2]=='主叫' else (item[1][0][0],item[0])

    return (phone1,phone2,item[0]),(item[1][1][1],1)
rddnormal_call_reconds = rddx6.map(edge_adjust_func)


# In[95]:


rddlen1 = rddx3.mapValues(lambda x: (len(x),1))
rddlen2 = rddlen1.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))


# In[11]:


# 聚合得到 每文件/存在的（两人）通话统计
rdd31 = rddnormal_call_reconds.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd32 = rdd31.map(lambda x:((x[0][0],x[0][1]),x[1]))
# 去除两个人之间 来自 不同（两个人文件的）重复记录#去除主被叫重复记录的问题
rdd33 = rdd32.reduceByKey(lambda x,y:x if x[1]>y[1] else y)
rdd_edge = rdd33.map(lambda x:(x[0][0],x[0][1],x[1][0],x[1][1]))
### 生成点边 DataFrame
edgeString= "src dst calltime_sum calltime_cnt"
edgeTString = "string string int int"
edgeschema = get_schema(edgeString,edgeTString)
edge_df = spark.createDataFrame(rdd_edge, edgeschema)
edge_df.persist()


# #### def get_edge_stat(self):

# In[12]:


# 主叫时间统计
rdd341 = rdd_edge.map(lambda x:(x[0],(x[2],x[3])))
rdd342 = rdd341.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd343 = rdd342.map(lambda x:(x[0],x[1][0],x[1][1]))
callingschema= "user_mobile callingtime_sum calling_cnt"
callingType = "string int int"
schema = get_schema(callingschema,callingType)
calling_df = spark.createDataFrame(rdd343, schema)
rdd342.persist()
# 被叫时间统计
rdd344 = rdd_edge.map(lambda x:(x[1],(x[2],x[3])))
rdd345 = rdd344.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd346 = rdd345.map(lambda x:(x[0],x[1][0],x[1][1]))
calledschema= "user_mobile calledtime_sum called_cnt"
calledType = "string int int"
schema = get_schema(calledschema,calledType)
called_df = spark.createDataFrame(rdd346, schema)
rdd345.persist()
# 主叫被叫合并统计
rdd346 = rdd342.union(rdd345)
rdd347 = rdd346.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd348 = rdd347.map(lambda x:(x[0],x[1][0],x[1][1]))
callschema= "user_mobile calltime_sum call_cnt"
callType = "string int int"
schema = get_schema(callschema,callType)
allcall_df = spark.createDataFrame(rdd348, schema)
# 合并
call_df1 = allcall_df.join(calling_df,on='user_mobile',how='left')
call_df = call_df1.join(called_df,on='user_mobile',how='left')


# #### def get_peopele_df(self):

# In[13]:


people_df = call_df.join(people_raw_df,on='user_mobile',how='left')


# #### def save_df(self,vertex_name='people_df.csv',edge_name='edge_df.csv'):

# In[17]:


vertex_name = 'people_full_has_persist.csv'
edge_name = 'edge_full_has_persist.csv'


# In[ ]:


people_df.repartition(4).write.save(hdfs_workdir+vertex_name,format='csv')
edge_df.repartition(4).write.save(hdfs_workdir+edge_name,format='csv')


# ### read_new_data

# In[42]:


new_people = sc.textFile(hdfs_workdir+vertex_name+'/*.csv')


# In[43]:


new_people.repartition(1).saveAsTextFile(hdfs_workdir+'people_onepart.csv')


# In[44]:


new_edge = sc.textFile(hdfs_workdir+edge_name+'/*.csv')


# In[45]:


new_edge.repartition(1).saveAsTextFile(hdfs_workdir+'edge_onepart.csv')


# In[118]:


new_people.count()


# In[213]:


new_edge.count()


# In[37]:


df_edge = spark.read.csv(hdfs_workdir+edge_name+'/*.csv',schema=edge_df.schema)


# In[38]:


df_edge.count()


# In[141]:


df_edge2 = df_edge.filter((df_edge.dst=='18868112289'))


# In[39]:


df_people = spark.read.csv(hdfs_workdir+vertex_name+'/*.csv',schema=people_df.schema)


# In[40]:


df_people.count()


# In[161]:


df_people.persist()


# In[ ]:


df_people.


# In[193]:


df_people.createOrReplaceTempView("newpeople")


# In[205]:


df_people2 = spark.sql('select * from newpeople where _c14 == True')

