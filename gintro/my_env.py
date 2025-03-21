"""
Command:
from gintro.my_env import *
"""

import matplotlib.pyplot as plt
import seaborn as sns
from IPython import display

display.set_matplotlib_formats('svg')

# sns.set()  # old version
sns.set_theme()
plt.figure(figsize=[8, 5])


"""
Possible Error: 
findfont: Generic family 'sans-serif' not found because none of the following families were found: SimHei
"""
# plt.rcParams["font.sans-serif"]=["SimHei"]  # 设置字体
# plt.rcParams["axes.unicode_minus"]=False    # 该语句解决图像中的“-”负号的乱码问题
