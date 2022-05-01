
# Chicago crime and weather

## Goals

The primary purpose of this project was to build an ETL pipeline that extracts, transforms and loads Chicago crime and daily weather data using Apache Airflow. The main goal was to prepare a data model that allows data scientist, or criminologist researchers to study and model how weather conditions may impact the type and nature of crimes occuring in Chicago.  

## Data sources

The data used in this project has two sources:
- Crime data was extracted from the Chicago Data Portal, using Socrata API. It contains the type, date, description and circumstances of the crimes occuring in chicago, dating back to 2001.
- Historical daily weather data recorded at a Chicago weather station was extracted from Global Historical Climate Network (GHCN), that was made publicly available on Google BigQuery. This data contains - among others - the average daily temperature,  windspeed, whether there was any fog or thunder during the day etc...


## Tools
The primary tool for establishing the data pipeline was Apache Airflow. The rationale behind choosing Airflow is based on its' ability to easily organize, execute and monitor the data flow and it is well suited for cases when batch of data needs to be extracted from multiple sources at a specific time interval. Furthermore, Airflow offers connectivity to a variety of data sources, thus provides great flexibility and simplicity.
Initial storage of the raw data was on AWS S3. The reason why S3 was selected as an intermediary storage was twofold: firstly, the raw data will be backed up in case it is required for any additional purpose (eg.: a data scientist who often requires raw data for their modeling purposes). Secondly, writing data from memory directly to Redshift is considerably slower than copying it from S3.
The transformed/modeled data was stored on AWS Redshift. As Redshift is a columnar database, it is considered good at queries involving a lot of aggregations, which is particularly useful for the project as the data model heavily relies on aggregations.

## ETL Pipeline

The Apache Airflow DAG consists of 12 different tasks with 6 custom operators. 

 - The first and second operators pull the data from their respective sources. The crime data is extracted from the Chicago Data Portal, using the Socrata API. The weather data was extracted from a public Google BigQuery database. Then, in both cases, the extracted data was loaded into AWS S3.
 - The third operator copies the data from S3 and loads them into two Redshift staging tables. 
 - The fourth and fifth operator loads the data into fact and dimensional tables, respectively.
 - Finally, the sixth operator is responsible for performing a data quality check, which can be passed if there are no NULL values in tables with primary keys, and if there are specifically two distinct values in boolean dimensional tables (crime_arrest, crime_domestic).

 ![DAG with task dependencies](https://github.com/ivsk/CrimeWeatherPipeline/blob/main/airflow_dag.jpg?raw=true)


The pipeline currently runs on a monthly basis, with the first run backfilling until the first month of the crime dataset (2001-12). Too frequent update of the pipeline would potentially lead to the unneccessary run of the pipeline as the crime data source by the City of Chicago is not updated on a daily basis.

## Data model and dictionary 

The data model used for the project was star schema with 1 fact table (fact_daily_crime_weather) and 6 dimensional tables as the primary use for the database is for general research/BI Analysis.
Detailed data dictionary is shown below.

### Fact table
The fact table is an aggregate table that contains the following: 
#### Daily crime weather

| Column           |        Type        |                                         Description |
|------------------|:------------------:|----------------------------------------------------:|
| crime_date       | `DATE primary key` |                                The date in question |
| crime_count      |       `int4`       |             Number of crimes registered on that day |
| arrest_count     |       `int4`       |                       Number of arrests on that day |
| domestic_count   |       `int4`       |                        Number of domestic incidents |
| temp             | `double precision` |                                 Average temperature |
| windspeed        | `double precision` |                                   Average windspeed |
| fog              |     `boolean`      |                   Whether there was fog on that day |
| rain_drizzle     |    `boolean`       |          Whether there was rain_drizzle on that day |
| snow_ice_pellets |     `boolean`      | Whether there were snow ice pellets fog on that day |
| thunder          |     `boolean`      |           Whether there was thunder fog on that day |

### Dimensional tables

#### Crime

|  Column      |           Type            |                                   Description |
|--------------|:-------------------------:|----------------------------------------------:|
| primary_type | `VARCHAR(50) primary key` | The unique type of the crimes in the database |

#### Crime location

| Column         |           Type            |                                                          Description |
|----------------|:-------------------------:|---------------------------------------------------------------------:|
| block          | `VARCHAR(50) primary key` |                           The unique location of the crime blockwise |
| community_area |          `int4`           | The community area associated with the block where the crime oocured |
| district       | `VARCHAR(50) primary key` |              The district where the location of the crime belongs to |
| ward           | `VARCHAR(50) primary key` |                  The ward where the location of the crime belongs to |

##### Arrest
| Column |   Type    |                   Description |
|--------|:---------:|------------------------------:|
| arrest | `boolean` | Whether any arrests were made |
 
##### Domestic
| Column   |   Type    |                         Description |
|----------|:---------:|------------------------------------:|
| domestic | `boolean` |  Whether any the crime was domestic |
 
##### Time
| Column     |          Type           |                                                     Description |
|------------|:-----------------------:|----------------------------------------------------------------:|
| crime_time | `TIMESTAMP primary key` |                         The approximate timestamp of the crimes |
| hour       |         `int4`          |  The hour of the timestamp when the crime approximately occured |
| day        |         `int4`          |                                  The day when the crime occured |
| week       |         `int4`          |                                 The day when the crime  occured |
| year       |         `int4`          |                                 The year when the crime occured |
| weekday    |     `VARCHAR(256)`      |                              The weekday when the crime occured |

##### Daily weather

| Column           |          Type           |                                                    Description |
|------------------|:-----------------------:|---------------------------------------------------------------:|
| id               | `TIMESTAMP primary key` |                        The approximate timestamp of the crimes |
| year             |         `int4`          | The hour of the timestamp when the crime approximately occured |
| month            |         `int4`          |                                 The day when the crime occured |
| day              |         `int4`          |                                The day when the crime  occured |
| temp             |  `double precision`     |                                            Average temperature |
| windspeed        |   `double precision`    |                                              Average windspeed |
| fog              |        `boolean`        |                              Whether there was fog on that day |
| rain_drizzle     |        `boolean`        |                     Whether there was rain_drizzle on that day |
| snow_ice_pellets |        `boolean`        |            Whether there were snow ice pellets fog on that day |
| thunder          |        `boolean`        |                      Whether there was thunder fog on that day |

## Scenarios

How you would approach the problem differently under the following scenarios:
- **If the data was increased by 100x:** The current datapipeline has two sources: daily weather data cannot be increased by 100x due to obvious constrainst. 100 times increase on the crime data on the other hand, would probably require Redshift cluster with larger storage as the currently set up dc2.large could be insufficient.   
- **If the pipelines were run on a daily basis by 7am:** If the pipelines were run on a daily basis by 7am, then we would potentially end up running the pipeline unneccessarily as the crime data source is not always updated on a daily basis.
- **If the database needed to be accessed by 100+ people:** Redshift can handle up to 500 connections and thus no modifications are neccessary. With regards to the raw data on S3, one would need to consider creating specific user groups with specific permissions based on the requirements. 

## Example queries

After backfilling for the first month of crime data, running the query `SELECT COUNT(id) from staging_crimes` returns 6.633.155 rows. The query `SELECT COUNT(id) from staging_weather` returns 7.757 rows.
Running the query `SELECT COUNT(crime_date) from fact_daily_crime_weather` returns 7.757 rows as the fact table is grouped by the day of the crime.
Example row from the fact table - `SELECT TOP 1 * from fact_daily_crime_weather` outputs the following: 

| crime_date | crime_count | arrest_count | domestic_count |  temp   |  windspeed  |   fog   |  rain_drizzle  | snow_ice_pellets | thunder |
|------------|:-----------:|:------------:|:--------------:|:-------:|:-----------:|:-------:|:--------------:|:----------------:|:-------:|
| 2001-01-01 |   1165      |     331      |      198       |  20.69  |    5.89     |  false  |     false      |      false       |  false  |



# Usage

The current setup runs Airflow locally using Docker. Inside the root folder run the following command (assuming Windows OS) to run database migrations:

```docker-compose up airflow-init```

Set up your own AWS resources (S3, Redshift) or use local storage and DB of your preference.

After the initial setup, run `docker build` to install additional dependencies listed in the Dockerfile.

**Important note regarding docker: this setup is only a quickstart, it is insufficient to run production-ready Docker Compose Airflow installation. [ For details, check the official documentation.](https://www.google.com) **

Set up your own BigQuery and Sodapy credentials as well as the Airflow connections to AWS to get access to the S3 bucket and the Redshift cluster

Parameterize the DAG and the tasks with the S3 sources/target tables, start time and schedule interval. 

Run the DAG and monitor its' state.
