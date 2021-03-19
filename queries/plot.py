import matplotlib.pyplot as plt
import numpy as np

rddt = [2.610966682434082, 2.666517496109009, 2.5541470050811768, 2.4377427101135254, 281.087694644928]
csv = [32.420072078704834, 86.39740824699402, 80.57300066947937, 29.07029914855957, 154.98738551139832]
parquet = [17.7742121219635, 35.41659474372864, 41.31655311584473, 18.80320143699646, 106.39897274971008]
'''
labels = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']
a1 = [rddt[0], rddt[1], rddt[2], rddt[3], rddt[4]]
a2 = [csv[0], csv[1], csv[2], csv[3], csv[4]]
a3 = [parquet[0],parquet[1],parquet[2],parquet[3],parquet[4],]
x = np.arange(len(labels))  # the label locations
width = 0.2  # the width of the bars
fig, ax = plt.subplots()
rects1 = ax.bar(x - width, a1, width, label="RDD")
rects2 = ax.bar(x,  a2, width, label="SparkSQL_csv")
rects3 = ax.bar(x + width, a3, width, label="SparkSQL_parquet")
ax.set_ylabel('Total Time (s)')
ax.set_title('Execution Time')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()
plt.savefig("Total Time 2048.png", bbox_inches="tight")
'''
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------#

labels1 = ['Join']
a1 = 15.8851
#a1 = [15.8851,6.4863]
a2 = 6.4863
x = np.arange(len(labels1))  # the label locations
width = 0.2  # the width of the bars
fig, ax = plt.subplots()
rects1 = ax.bar(x-width/2, a1, width, label="Optimizer Disabled")
rects1 = ax.bar(x+width/2, a2, width, label="Optimizer Enabled")
ax.set_ylabel('Join Exec Time (s)')
ax.set_title('Join Execution Time')
ax.set_xticks(x)
ax.set_xticklabels(labels1)
ax.legend()
plt.savefig("Join Exec Time.png", bbox_inches="tight")

