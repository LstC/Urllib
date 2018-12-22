import datetime 
import tushare as ts # 导入 tushare 模块
import pymysql # 导入 pymysql 模块
import numpy as np

# 连接 mysql 数据库 database ： stock
db = pymysql.connect(host='127.0.0.1', user='LstCC', passwd='112358', db='stock')

cursor = db.cursor()

# 设置tushare pro的token并获取连接
ts.set_token('ae9283163f806c321f2f4aa00a3c5cce84e69aa44eb2873d4863498e')

pro = ts.pro_api()

# 设定获取日线行情的初始日期和终止日期，其中终止日期设定为昨天。
start_dt = '20100101'

time_temp = datetime.datetime.now() - datetime.timedelta(days=1)

end_dt = time_temp.strftime('%Y%m%d')

stocks = pro.daily('ts_code', start_date = start_dt, trade_date = end_dt)

stock_pool = np.array(stocks['ts_code']) # list

total = len(stock_pool)

for i in range(total):
    try:
        df = pro.daily(ts_code = stock_pool[i], start_date = start_dt, end_date = end_dt)
        # 打印进度
        print('Seq: ' + str(i+1) + ' of ' + str(total) + '   Code: ' + str(stock_pool[i]))
        c_len = df.shape[0]
        
    except Exception as aa:
        print(aa)
        print('No DATA Code: ' + str(i + 1))
        continue
        
    for j in range(c_len):
        resu0 = list(df.ix[c_len - 1 - j])
        resu = []
        
        for k in range(len(resu0)):
            
            if str(resu0[k]) == 'nan':
                resu.append(-1)
            else:
                resu.append(resu0[k])
                
        state_dt = (datetime.datetime.strptime(resu[1], "%Y%m%d")).strftime('%Y-%m-%d')
        
        # 写入数据到表 stock_all中
        try:
        
            sql_insert = "INSERT INTO stock_all(state_dt,stock_code,open,close,high,low,vol,amount,pre_close,amt_change,pct_change) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%i','%.2f','%.2f','%.2f','%.2f')" % (state_dt,str(resu[0]),float(resu[2]),float(resu[5]),float(resu[3]),float(resu[4]),float(resu[9]),float(resu[10]),float(resu[6]),float(resu[7]),float(resu[8]))

            cursor.execute(sql_insert)

            db.commit()
        
        except Exception as err:
                continue
                
# 关闭数据库连接
cursor.close()

db.close()

print('All Finished!')

