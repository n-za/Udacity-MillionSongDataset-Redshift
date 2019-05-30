# Extract Load Transform for the million song dataset

## Project Contents

### song_json_path.json
This file describes the song json document and explains how to map the json elements into relational columns. 
This file has then been uploaded via the console into a S3 bucket in the same region as the redshift database. The accessible URL of the format file is https://s3-us-west-2.amazonaws.com/nicolas-dend-project3/songs_json_path.json

### sql_queries.py

This file contains the <code>drop table if exists</code> and <code>create table if not exists</code> for staging and analytical tables.

It contains the <code>copy</code> instructions reading from the S3 bucket the raw data. And the <code>insert</code> commands of the data into the analytical tables (fact and dimensions).

### dwh.cfg
This configuration file contains:
* the parameters of the connections to the redshift cluster.
* the ARN of the role to use with the job
* the location of the raw data.

Additionnaly it contains the json path file for the songs data.

### create_tables.py
This file contains the script to launch with python3 in order to create the schema.
In the project directory:

<code>
    python3 create_tables.py
</code>

### etl.py
This file contains the code for loading the data into the analytical tables. It first fills the staging tables and then insert into the dimension and fact tables.

In the project directory:

<code>
    python3 etl.py
</code>

## Extract from S3

The log and song data are retrieved from S3 using the copy command of Amazon Redshift.

## Load into Amazon Redshift

The format file for the song dataset has been provided in the workspace like the one given for the log data. 
The format is stored in the <code>son_json_path.json</code> file. 

## Transform 

### Star Schema
The raw data are processed in order to keep them in a star schema. This schema is roughly the same as in the Project 1B.

<code>Songplays</code> are the facts. The fact table is surrounded by the dimensions:
* <code> users</code>
* <code> artists</code>
* <code> time</code>
* <code> song</code>

Dimensions are handled like slow changing dimensions (SCD) of type 0 (immutable), but <code>users</code>. For this dimension we keep track of the current status of the <code>level</code> (free or paid). Hence it is a slow changing dimension of type 1 (no history kept). This improvement was not required by the assignment.
It is noteworthy to mention that Amazon Redshift does not provide with a MERGE/UPSERT statement.

For each dimension table a numeric sugorate key has been added in order to be less dependent on the source system. This is a good practice in data warehousing.

### Parallelism Issues
The fact table is the most crowded table. Its processing should be split on multiple CPUs. As a valid criterion wa can use the song_id as a distkey for the fact table and the song dimension. So the songplays and the songs will be evenly distributed among the instances of the cluster. In order to speed up the join between the fact table and the song dimension, we can it set also as a sortkey.

The time and artist dimensions are the smaller ones and we can replicate these tables on each instance of the cluster.

# Possible Improvements

* The calendar <code>time</code> contains only the timestamps that really occur in the data from songplays. It would be much more efficient to implement a dense calendar where every second of a given time frame (say 3 years) is already in the calendar. Hence the calendar is updated once a year for example and when the <code>songplays</code> is updated the timestamps already exist in the calendar. 

* Our ELT job assumes that songs and artists are already in their analytical tables when a songplay about them arrives. It is a rather reasonable assumption. However it could happen that the song files in S3 are not provided when needed, and then in our implementation the columns <code>songplays.artist_key</code> and <code>songplays.song_key</code> are <code>NULL</code>. This is typically handled by creating a dummy row in the <code>artists</code> or <code>songs</code> keeping track in the dimension tables of the missing information. The keys are provided when needed and the details would be fed into the dimension tables when they arrive. There are 6530 cases where the table <code>songplays</code> contains a reference to a song not yet included in the <code>songs</code> table. There are only 321 songs that are played and present in the <code>songs</code> table. The results are more balanced for the artists (4259 present vs 2592 not found).

# Sample Queries

## Trends

```sql
with det as (select t.year, t.month, count(songplay_key) cnt from songplays s join time t on s.start_time = t.start_time
group by t.year, t.month),
mon as (select year, null, sum(cnt) cnt from det group by year),
year as (select null, null, sum(cnt) cnt from mon)
select * from det union all select * from mon union all select * from year
order by 1 asc nulls last, 2 asc nulls last
```

Surprisingly enough rollup is not (yet?) implemented in Amazon Redshift.

## Who pays?
```sql
with det as (select gender, level, count(user_key) cnt from users
group by gender, level),
gend as (select gender, null, sum(cnt) cnt from det group by gender),
lev as (select null, level, sum(cnt) cnt from det group by level),
tot as (select null, null, sum(cnt) cnt from gend)
select * from det
union all
select * from gend
union all
select * from lev
union all
select * from tot
order by 1 asc nulls last, 2 asc nulls last
```
Cube is neither defined.

The twice more women (15) than men (7) that have chosen the paid offer. There is roughly the same number of men (40) and women (35) in the free offer.
