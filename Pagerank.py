
# coding: utf-8

# In[1]:


from pyspark import StorageLevel
from pyspark.sql.types import *
import numpy as np
import json
from functools import reduce
hdfs_workdir = 'hdfs://192.180.3.43:9000/'
path_prefix = hdfs_workdir+ 'phone_detail/'
import time
fileall = hdfs_workdir+'edge_full.csv/*.csv'


# In[2]:


def get_schema(schemaString,typeString):
    dict_getType = {'int':IntegerType(),'string':StringType(),'float':FloatType(),'bool':BooleanType()}
    fields = [StructField(field_name, dict_getType.get(Type), True) 
              for field_name,Type in zip(schemaString.split(' '),typeString.split(' '))]
    schema = StructType(fields)
    return schema


# In[363]:


filepeople = hdfs_workdir+'people_full.csv/*.csv'

df_people = spark.read.csv(filepeople)

df_people.createOrReplaceTempView("people")

schema = get_schema('_c0','string')
rdd3 = sc.parallelize(BCoverdue.value).map(lambda x:(x,))
df_overdue = spark.createDataFrame(rdd3, schema=schema)

df_have = df_people.join(df_overdue,on='_c0')

df_have.createOrReplaceTempView("people_have")

df_have2 = spark.sql('select _c0,_c7,_c14 from people_have where _c14 == True')

df_have2.persist()

df_have2.show()

df_have2.count()


# In[442]:


df_have2.show()


# In[3]:


rdd_raw = sc.textFile(fileall)


# In[4]:


def process_func(x):
    items = x.split(',')
    return (items[0],items[1],int(items[2]),int(items[3]))
rdd1 = rdd_raw.map(process_func)


# In[5]:


rdd2 = rdd1.map(lambda x:((x[0],x[1]),(x[2],x[3])))


# In[10]:


rdd21 = rdd2.filter(lambda x:(x[0][0][:4]=='1370'))


# In[11]:


rdd21.persist()


# In[606]:


rdd21.count()


# In[504]:


rdd21.take(10)


# In[6]:


def adjust_order(x):
    if x[0][0] > x[0][1]:
        return x
    else:
        return (x[0][1],x[0][0]),x[1]
# 调整主被叫顺服
rdd22 = rdd2.map(adjust_order)
rdd23 = rdd22.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
def flat_func(x):
    return x,((x[0][1],x[0][0]),x[1])
rdd24 = rdd23.flatMap(flat_func)


# In[7]:


rdd24.unpersist()


# In[7]:


rdd25 = rdd24.groupBy(lambda x:x[0][0])


# In[8]:


with open('overdue.json') as f:
    overdue_users = set(json.load(f).values())
BCoverdue = sc.broadcast(overdue_users)


# In[653]:


def get_wrightv1(x):
    Shrink = 0.5
    k,d = x
    ct1 = np.log(np.array([b[1] for a,b in d.data])+1)
    total1 = np.sum(ct1)
    w1 = (ct1/total1).tolist()
    
#     ct2 = np.array([b[1] for a,b in d.data])
#     total2 = np.sum(ct2)
#     w2 = (ct2/total2).tolist()
    keys = [a for a,b in d.data]
    other_phone = set([y for x,y in keys])
    overdue_people = BCoverdue.value&other_phone
    if overdue_people:
        overdue_rate = sum([v for k,v in zip(keys,w1) if k[1] in overdue_people])
        Shrink_rate = Shrink + (1 - Shrink)/overdue_rate
        re_weight_func = lambda p,w:w*Shrink_rate if p in overdue_people else w*Shrink
        w2 = [re_weight_func(k[1],v) for k,v in zip(keys,w1)]
        return zip(keys,w1,w2)
    else:
        return zip(keys,w1,w1) 
    
    
    
rdd26 = rdd25.flatMap(get_wrightv1)


# ###  Part2

# In[9]:


rdd30 = rdd24.groupBy(lambda x:x[0][0])


# In[15]:


rdd24.take(10)


# In[10]:


def stat_overdue(x):
    others = set([i[0][1] for i in x.data])
    overdue_people = BCoverdue.value&others
    over_num = len(overdue_people)
    if over_num > 0:
        over_calltime_mean = np.mean([i[1][0] for i in x.data])
        over_callcnt_mean = np.mean([i[1][1] for i in x.data])
        return over_num,over_calltime_mean,over_callcnt_mean
    else:
        return 0,0,0
    
rdd31 = rdd30.mapValues(stat_overdue)


# In[ ]:


rdd32 = rdd31.map(lambda x:)


# In[10]:


rdd31.persist(StorageLevel(False,True,True,False,))


# In[ ]:


rdd31


# In[11]:


hdfs_workdir


# In[11]:


rdd31.saveAsTextFile(hdfs_workdir+'overdupeople.csv')


# In[ ]:


rdd31.take(10)


# ## read

# In[13]:


rdd4 = sc.textFile(hdfs_workdir+'overdupeople.csv')


# In[61]:


rdd41 = rdd4.map(lambda x:eval(x))


# In[66]:


rdd41.unpersist()


# In[67]:


rdd42 = rdd41.sortBy(lambda x:x[1][0],ascending=False)
rdd42.persist()
rdd42.take(200)


# In[73]:


rdd43 = rdd42.map(lambda x:(x[0],x[1][0]))


# In[88]:


top10000 = rdd43.take(100000)


# In[77]:


len(top10000)


# In[89]:


top10000[-10:]


# In[79]:


over10000 = set([x for x,y in top10000])

