# Databricks notebook source
# Map
x = sc.parallelize(["b","a","c"])
y = x.map(lambda z: (z,z,1))
print(x.collect())
print(y.collect())

# COMMAND ----------

# Map
x = sc.parallelize(["b","a","c"])
y = x.map(lambda z: (z,'p',1))
print(x.collect())
print(y.collect())

# COMMAND ----------

# Filter
x = sc.parallelize([1,2,3,4])
y = x.filter(lambda z: z%2 == 1) #filter odd values
print(x.collect())
print(y.collect())

# COMMAND ----------

# flatMap
x = sc.parallelize([1,2,3,4])
y = x.flatMap(lambda z: (z,z*100,z+1,45)) #filter odd values
print(x.collect())
print(y.collect())

# COMMAND ----------

# groupBy
x = sc.parallelize(["John","Fred","Anna","Frank"])
# x.collect()
y = x.groupBy(lambda z: z[0])
# print(y.collect())
z = y.collect()
sorted([(k, sorted(v)) for (k, v) in z])
