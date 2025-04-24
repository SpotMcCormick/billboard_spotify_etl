# Billboard 100 and Spotify ETL

## About the Project

Hello everybody! 

Welcome to another ETL project. Music was a big part of my life in my 20's and early 30's so I wanted to do a project on it. This came up when I did a linear regression on Spotify's API and got audio features. Since then it lookes like a few of their features are depricated and cant get that metadata. Oh well I decided to use what data I could get in Spotfy's API which was Album, Release Date, and Popularity. Billboard has a library that gives me the top 100 songs for the week graded on their metrics. Their fields inclued Chart Date, Rank, Song Title, and Artist. From there I decided to make an age of the song field because as the data set gets bigger I am curious of how old #1 songs are from their release date. I also decided to join these two data sets to track the age of a song vs its rank and vs its popularity. I was just curious and also it give me a reason to do another ETL project. I utilezed free public APIs, Pandas, and loaded everything into Big Query. Below you will see a preview of it. 


![Image](https://github.com/user-attachments/assets/b0e89222-2c88-4bad-a8d1-34414fc3e32d)

Stay tuned as you will see this orchestrated in Airflow in the future. 
