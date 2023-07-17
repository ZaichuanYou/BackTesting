import analyze
from stock_pool import backTest

if __name__ == '__main__':
    tester = analyze
    tester.follow_interval = 5
    tester.adv_moment_num = 5
    tester.session_length = 25
    tester.day_avg = 1
    tester.weighted_avg = True

    