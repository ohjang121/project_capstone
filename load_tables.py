class ProdQueries:

    fact_immigration_prod = '''
    SELECT string(int(i.cicid)) as immigration_id,
    int(i.i94yr) as imm_year,
    int(i.i94mon) as imm_month,
    case when i.i94mode = 1 then 'Air'
         when i.i94mode = 2 then 'Sea'
         when i.i94mode = 3 then 'Land'
         when i.i94mode = 9 then 'Not reported'
         else null end as transport_mode,
    i.i94port as arrival_city_code,
    p.city as arrival_city,
    coalesce(i.i94addr, p.state) as arrival_state,
    date_add(date('1960-01-01'), int(arrdate)) as arrival_date,
    date_add(date('1960-01-01'), int(depdate)) as departure_date,
    case when i.i94visa = 1 then 'Business'
         when i.i94visa = 2 then 'Pleasure'
         when i.i94visa = 3 then 'Student'
         else null end as travel_purpose,
    i.visatype as visa_type,
    i.visapost as visa_issued_state,
    i.airline as airline
    FROM stg_immigration i
    LEFT JOIN port_mapping p on lower(i.i94port) = lower(p.code)
    '''

    dim_immigrant_prod = '''
    SELECT string(int(i.cicid)) as immigration_id,
    c1.country as citizenship_country,
    c2.country as residence_country,
    int(i.i94bir) as age,
    int(i.biryear) as birthyear,
    i.gender
    FROM stg_immigration i
    LEFT JOIN country_mapping c1 on int(i.i94cit) = int(c1.code)
    LEFT JOIN country_mapping c2 on int(i.i94res) = int(c2.code)
    '''

    # need pk - temp_report_month || city || country

    dim_temperature_prod = '''
    SELECT date(dt) as temp_report_month,
    city,
    country,
    avg(float(averagetemperature)) as avg_temp,
    avg(float(averagetemperatureuncertainty)) as avg_temp_uncertainty
    FROM stg_temperature
    GROUP BY 1,2,3
    '''

    # need pk - city || state_code || race

    dim_demographics_prod = '''
    SELECT city,
    state_code,
    race,
    float(median_age) as median_age,
    int(male_population) as male_population,
    int(female_population) as female_population,
    int(total_population) as total_population,
    int(number_of_veterans) as number_of_veterans,
    int(foreign_born) as foreign_born,
    float(avg_household_size) as avg_household_size
    FROM stg_demographics
    '''
