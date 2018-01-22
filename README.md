# WeblogChallenge

The goal is to do web log analysis using a distributed tool.

## Tools Used:

- Spark
- Scala
- SBT
- IntelliJ IDEA Community Edition
- Databricks
- Github

## Setup Instructions:

- Set up Spark, Scala and SBT on system.
- Set up attached files, as per folder structure.
- Set SPARK_HOME environment variable.
- Move to project folder on terminal.
- "bash run.sh" on command line.

## Assumptions:

- A combination of request IP address + User Agent strings was used to determine unique visitors.
- Sessionization was done by time window, with session idle time cut-off set to 15 minutes.
- Most engaged users were identified by filtering sessions having length greater than the 95th percentile of session lengths.
- Logs with non-standard values, null user agent values, as well as sessions with zero session length were ignored.

## Results:

- Average session time: 682.25 seconds
- Average of unique url count by session: 7.67

- Top 5 Total Unique URL Counts and corresponding Session Ids (request_ip+user_agent+session_num):
    
    - 9012: 119.81.61.166_Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10_6
    - 5790: 119.81.61.166_Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10_7
    - 4938: 52.74.219.71_Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)_5
    - 4656: 106.186.23.95_-_8
    - 4594: 52.74.219.71_-_5

- Top 5 Visitors (by ip address:user agent) and length of corresponding longest session times: 

    - 220.226.206.7: -: 4097
    - 119.81.61.166: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10: 3039
    - 52.74.219.71: Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html): 3036
    - 54.251.151.39: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.8 (KHTML, like Gecko) Chrome/17.0.940.0 Safari/535.8: 3029
    - 52.74.219.71: -: 3029

