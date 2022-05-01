
CREATE TABLE public.staging_crimes (
    id int primary key,
	crime_date TIMESTAMP,
	block varchar(256),
	primary_type varchar(256),
	description varchar(256),
	arrest boolean,
	domestic boolean,
	district int4,
	ward int4,
	community_area int4
);

CREATE TABLE public.staging_weather (
    id int primary key,
	year int4,
	month int4,
	day int4,
	temp double precision,
	windspeed double precision,
	fog boolean default false,
	rain_drizzle boolean default false,
	snow_ice_pellets boolean default false,
	thunder boolean default false
);

CREATE TABLE public.fact_daily_crime_weather(
    crime_date DATE,
    crime_count int4,
    arrest_count int4,
    domestic_count int4,
    temp double precision,
	windspeed double precision,
	fog boolean default false,
	rain_drizzle boolean default false,
	snow_ice_pellets boolean default false,
	thunder boolean default false,
    CONSTRAINT daily_crime_weather_pkey PRIMARY KEY (crime_date)

);

CREATE TABLE public.crime(
    primary_type varchar(50),
    CONSTRAINT crime_pkey PRIMARY KEY (primary_type)
);

CREATE TABLE public.crime_location(
    block varchar(50),
    community_area int4,
	district int4,
	ward int4,
    CONSTRAINT location_pkey PRIMARY KEY (block)
);

CREATE TABLE public.crime_arrest(
    arrest boolean
);

CREATE TABLE public.crime_domestic(
    domestic boolean
);

CREATE TABLE public."time"(
    crime_time timestamp NOT NULL,
    "hour" int4,
    "day" int4,
    week int4,
    "year" int4,
    weekday varchar(256),
    CONSTRAINT time_pkey PRIMARY KEY (crime_time)
);

CREATE TABLE public.daily_weather (
    id int primary key,
	year int4,
	month int4,
	day int4,
	temp double precision,
	windspeed double precision,
	fog boolean default false,
	rain_drizzle boolean default false,
	snow_ice_pellets boolean default false,
	thunder boolean default false
);
