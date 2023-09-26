# MacKillop College Canberra | Overview

This repo contains scripts built by Peter Zhao as a part of the new digital transformation project mandated by the Catholic Education Office in 2023. It includes 4 main Folders containing scripts that are important to business as usual.  
  
#### 1. Staging-SMMCD3  
Staging-SMMCD3 is a script that is run every morning at 8am and what is does is recreate the Staging database from multiple sources of truth and collects them into one database. This data can then be used to create Views and undergo ELT processes to get data that we can actually use and provide to staff and students  
#### 2. Historical-SMMCD3  
Historical-SMMCD3 contains 2 scripts. **create-isnert-historical-smmcd3.py**  takes data from multiple sources of truth and creates Temporal Tables (History Tables). These tables serve as a way to keep logs of what happens to the data inside of our database. If data is deleted, we will have automatic logs of this, if data is inserted or deleted, we will know as well. This runs in conjunction with the script **updated-new-info-historical-smmcd3.py** which is a headless python database crawler which compares our Historical database to our sources of truth. The script then inserts any missing rows, deletes rows that no longer exist in the source of truth and updates rows if data has changed.  
#### 3. student-info-flask-app  
This repo is a simple repo, since the source of truth for data is changing at St Mary MacKillop College, the way for Staff members to get student-id's and Catholic Education Id's is temporarily unavailable. This app is rolled out through Group Policy and allowws Staff to search up students by their name or classes and save to excel or print the results out. This gives teachers access to Student ID's while we handle the data migration  
#### 4. mole-export-crawler  
This folder consists of two scripts. A recorder and an actioner. The recorder records the users mouse movements for a specified amount of time and copies it into a txt file. The action folder then replays the recorded actions with customisable time delays between each clicks. This was created to extract data from the Legacy mole system and download pdf's and html files of students behavioural data. Unfortunately this cannot be done in a headless manner because MOLE is an offline copy.
 