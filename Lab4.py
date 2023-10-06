#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas
import configparser
import psycopg2


# In[3]:


config = configparser.ConfigParser()
config.read('config.ini')

db = config['myaws']['db']
host = config['myaws']['host']
user = config['myaws']['user']
pwd = config['myaws']['pwd']


# In[4]:


conn = psycopg2.connect(host = host,
                       user = user,
                       password = pwd,
                       dbname = db
                       )


# 3.1

# In[5]:


sql = "select * from student"


# In[6]:


df = pandas.read_sql_query(sql,conn)
df[:]


# 3.2

# In[7]:


sql = """select professor.p_name,
        course.c_name
        from professor 
        inner join course
        on professor.p_email = course.p_email
        """ 


# In[8]:


df = pandas.read_sql_query(sql,conn)
df[:]


# In[9]:


sql = """
select c_number, count(*) as num_student
from enroll
group by c_number
order by num_student desc
"""


# In[10]:


df = pandas.read_sql_query(sql,conn)
df.plot.bar(x='c_number',y='num_student')


# 3.4

# In[11]:


sql ="""
SELECT professor.p_name, COUNT(course.c_number) AS num_courses_taught
FROM professor
INNER JOIN course ON professor.p_email = course.p_email
GROUP BY professor.p_name
ORDER BY num_courses_taught DESC
"""


# In[12]:


df = pandas.read_sql_query(sql,conn)
df.plot.bar(x='p_name',y='num_courses_taught')


# In[ ]:





# 3.5

# In[13]:


sql = """ insert into professor(p_email,p_name,office)

values('p4@jmu.edu','p4','o4')"""


# In[ ]:





# In[14]:


conn.commit()


# In[15]:


conn.rollback()


# In[16]:


df = pandas.read_sql_query('select * from professor',conn)
df[:]


# In[17]:


sql = """
insert into course (c_number,c_name,room,p_email)
values('c5', 'linkedin', 'r2', 'p4@jmu.edu')
"""


# In[ ]:





# In[18]:


conn.commit()


# In[19]:


df = pandas.read_sql_query('select * from course',conn)

df[:]


# In[ ]:





# In[ ]:




