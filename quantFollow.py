import analyzeFollow
from stock_pool import backTest
import os
import sys

if __name__ == '__main__':
    tester = analyzeFollow
    tester.follow_interval = 3
    tester.adv_moment_num = 5
    tester.session_length = 75
    tester.day_avg = 1
    tester.weighted_avg = True

    s_dir = 'C:/Users/21995/Desktop/量化投资/CB_Data'
    d_dir = 'C:/Users/21995/Desktop/量化投资/CB_Data_Test'
    files = os.listdir(s_dir)
    
    print(f"Start processing data with following parameters:\nfollow_interval: {tester.follow_interval}\nadv_momentum_num: {tester.adv_moment_num}\nsession_length: {tester.session_length}\nday_avg: {tester.day_avg}\nweighted_avg: {tester.weighted_avg}")
    print(f"source directory: {s_dir}\ndestination directory: {d_dir}")
    for ind, file in enumerate(files):
        tester.analyze(file, window=int(225/tester.session_length)*tester.day_avg, s_dir=s_dir, d_dir=d_dir)
        print("\r", end="")
        print("Processing Data: {}%: ".format(int(ind+1)*100//len(files)), "▋" * (int((int(ind+1)/len(files)) * 100 // 2)), end="")
        sys.stdout.flush()

    
    backTest(name=f"top {0*10} to {0*10+10}%", save=False, group=0, dir='')
    # for a in range(0,10):
    #     print(f"top {a*10} to {a*10+10}%")
    #     backTest(name=f"top {a*10} to {a*10+10}%", save=True, group=a, dir="Result_15min")