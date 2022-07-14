CREATE TABLE public.fact_immigration (
	immigration_id varchar NOT NULL,
	imm_year int,
	imm_month int,
	transport_mode varchar,
	arrival_city_code varchar,
	arrival_city varchar,
    	arrival_state varchar,
    	arrival_date date,
    	departure_date date,
    	travel_purpose varchar,
    	visa_type varchar,
    	visa_issued_state varchar,
    	airline varchar,
	CONSTRAINT fact_immigration_pkey PRIMARY KEY (immigration_id)
);

CREATE TABLE public.dim_immigrant (
	immigration_id varchar NOT NULL,
	citizenship_country varchar,
	residence_country varchar,
	age int,
    	birthyear int,
    	gender varchar,
	CONSTRAINT dim_immigration_pkey PRIMARY KEY (immigration_id)
);


CREATE TABLE public.dim_temperature (
	temp_id int NOT NULL,
	temp_report_month date,
	city varchar,
    	country varchar,
    	avg_temp double precision,
    	avg_temp_uncertainty double precision,
	CONSTRAINT dim_temperature_pkey PRIMARY KEY (temp_id)
);

CREATE TABLE public.dim_demographics (
	demo_id int NOT NULL,
	city varchar,
	state_code varchar,
    	race varchar,
    	median_age double precision,
    	male_population int,
    	female_population int,
    	total_population int,
    	number_of_veterans int,
    	foreign_born int,
    	avg_household_size double precision,
	CONSTRAINT dim_demographics_pkey PRIMARY KEY (demo_id)
);


