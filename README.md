# TalkingData AdTracking Fraud detection challenge

Source: <url> https://www.kaggle.com/c/talkingdata-adtracking-fraud-detection </url>

### Description from kaggle

Fraud risk is everywhere, but for companies that advertise online, click fraud can happen at an overwhelming volume, resulting in misleading click data and wasted money. Ad channels can drive up costs by simply clicking on the ad at a large scale. With over 1 billion smart mobile devices in active use every month, China is the largest mobile market in the world and therefore suffers from huge volumes of fradulent traffic.

TalkingData, China’s largest independent big data service platform, covers over 70% of active mobile devices nationwide. They handle 3 billion clicks per day, of which 90% are potentially fraudulent. Their current approach to prevent click fraud for app developers is to measure the journey of a user’s click across their portfolio, and flag IP addresses who produce lots of clicks, but never end up installing apps. With this information, they've built an IP blacklist and device blacklist.

While successful, they want to always be one step ahead of fraudsters and have turned to the Kaggle community for help in further developing their solution. In their 2nd competition with Kaggle, you’re challenged to build an algorithm that predicts whether a user will download an app after clicking a mobile app ad. To support your modeling, they have provided a generous dataset covering approximately 200 million clicks over 4 days!

### Evaluation from kaggle

Submissions are evaluated on area under the ROC curve between the predicted probability and the observed target.

Submission File
For each click_id in the test set, you must predict a probability for the target is_attributed variable. The file should contain a header and have the following format:

click_id,is_attributed
1,0.003
2,0.001
3,0.000
etc.
