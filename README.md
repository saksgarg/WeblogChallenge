# WeblogChallenge

The goal is to do web log analysis using distributed tools.

## Tools Used:

- Spark
- Scala
- SBT
- IntelliJ IDEA Community Edition
- Databricks Community Edition
- Github

## Setup Instructions:

- Set up Spark and SBT on system.
- Set SPARK_HOME environment variable.
- Use "bash run.sh" command to build and run the solution.
- Outputs will be saved in data folder after running the "run" script.

## Assumptions:

- There were total 1158500 number of logs in the dataset. 12 logs had non-standard request strings, and were dropped. Additionally, 5 logs had null user agent strings, and were dropped. Remaining 1158483 logs were used for the analysis.
- A combination of request IP address + User Agent strings was used to determine unique visitors.
- Sessionization was done by time window, with session idle time cut-off set to 15 minutes.
- Most engaged users were identified by filtering sessions having length greater than the 95th percentile of session lengths.
- Sessions with zero session length were ignored.

## Results:

- Average session time: 682.25 seconds
- Average of unique url count by session: 7.67

- Top 5 Counts of Unique URL visits by session and corresponding Session Ids (request_ip+user_agent+session_num):
    
    - 9012: 119.81.61.166_Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10_6
    - 5790: 119.81.61.166_Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10_7
    - 4938: 52.74.219.71_Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)_5
    - 4656: 106.186.23.95_-_8
    - 4594: 52.74.219.71_-_5

- Most Engaged Users (by ip address:user agent) and length of corresponding longest session times: 

    - 220.226.206.7: -: 4097
    - 119.81.61.166: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10: 3039
    - 52.74.219.71: Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html): 3036
    - 54.251.151.39: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8: 3029
    - 52.74.219.71: -: 3029

