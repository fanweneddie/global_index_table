import numpy as np
import matplotlib as mpl
mpl.use('TkAgg') 
import matplotlib.pyplot as plt

# select read cases: 0 for readrandom and 1 for readhot
read = 1
# select time to show: 0 for build time and 1 for search time
time = 1
# read cases
read_cases = ['readrandom', 'readhot']
# time to show
time_types = ['build time', 'search time']
# databases for testing
dbs = ['10G', '50G', '100G']
# global index table usages
git_uses = ['no_git', 'git_wo_bf', 'git_w_file_bf', 'git_with_blk_bf']
# stores the overall result of build time and search time
results = {
    'readrandom' : {
        '10G': {
            'no_git' : (0.0, 805.27),
            'git_wo_bf': (7.94, 642.57),
            'git_w_file_bf': (8.87, 640.27),
            'git_with_blk_bf': (8.87, 639.93)
        },
        '50G': {
            'no_git' : (0.0, 5933.1),
            'git_wo_bf': (31218.5, 4734.61),
            'git_w_file_bf': (31509.13, 1959.89),
            'git_with_blk_bf': (31443.63, 1957.69)
        },
        '100G': {
            'no_git' : (0.0, 8030.8),
            'git_wo_bf': (112077.33, 5998.72),
            'git_w_file_bf': (113007.0, 2210.26),
            'git_with_blk_bf': (112356, 2228.23)
        }
    }, 
    'readhot': {
        '10G': {
            'no_git' : (0.0, 752.72),
            'git_wo_bf': (8.75, 554.49),
            'git_w_file_bf': (8.04, 555.96),
            'git_with_blk_bf': (8.00, 553.26)
        },
        '50G': {
            'no_git' : (0.0, 3617.10),
            'git_wo_bf': (31216.30, 2816.18),
            'git_w_file_bf': (31540.8, 1340.37),
            'git_with_blk_bf': (31534.07, 1340.71)
        },
        '100G': {
            'no_git' : (0.0, 4558.41),
            'git_wo_bf': (111582.0, 3616.56),
            'git_w_file_bf': (112209.33, 1573.45),
            'git_with_blk_bf': (112213.0, 1569.44)
        }
    }
}

# parameters for this plot
read_case = read_cases[read]
time_type = time_types[time]

# basic plot info
title = 'Global Index Table(git) ' + time_type + ' in ' + read_case
x_label = 'Database'
y_labels = ['build time (ms)', 'search time per 100000 ops (ms)']
# labels on x,y axis
labels = dbs
y_label = y_labels[time]
# number of groups of columns
label_num = len(labels)
# number of columns in a group
gp_col_num = len(git_uses)
# data to be plotted
datas = []
for i in range(0, gp_col_num):
    git_use = git_uses[i]
    data = []
    for j in range(0, label_num):
        db = dbs[j]
        data.append(results[read_case][db][git_use][time])
    datas.append(data)

# the location of labels
x = np.arange(label_num)
# the width of the bars
width = 0.2
# the offset of each bar
offsets = [-width*3/2, -width/2, width/2, width*3/2]

# add texts to the plot
fig, ax = plt.subplots()
ax.set_xlabel(x_label, fontsize=18)
ax.set_ylabel(y_label, fontsize=18)
ax.set_yscale('log')
ax.set_title(title, fontsize = 26)
ax.set_xticks(x, labels, size = 14)
ax.tick_params(axis = 'y', labelsize=14)
ax.legend()
# plot
rects = []
for j in range(0, gp_col_num):
    rect = ax.bar(x + offsets[j], datas[j], width, label=git_uses[j], )
    rects.append(rect)

for j in range(0, gp_col_num):
    ax.bar_label(rects[j])

fig.tight_layout()
plt.legend()
plt.show()