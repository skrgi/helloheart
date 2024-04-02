def create_schema_query():
    sql_create_schema = """
    CREATE SCHEMA IF NOT EXISTS covid_19;
    """
    return sql_create_schema

def max_date_query():
    sql_max_date = """
    SELECT to_char(max(date), 'YYYY-MM-DD') as max_date_str
    FROM covid_19.covid_test_results
    """
    return sql_max_date

def create_table_query():
    sql_create_table = '''
    DROP TABLE IF EXISTS covid_19.covid_test_results;

    CREATE TABLE covid_19.covid_test_results (
        state VARCHAR(2),
        state_name VARCHAR(255),
        state_fips VARCHAR(2),
        fema_region VARCHAR(20),
        overall_outcome VARCHAR(20),
        date DATE,
        new_results_reported INTEGER,
        total_results_reported INTEGER
    );
    '''
    return sql_create_table

def insert_query():
    sql_insert = '''INSERT INTO covid_19.covid_test_results (state, state_name, state_fips, fema_region, overall_outcome, date, new_results_reported, total_results_reported) VALUES %s'''
    return sql_insert

def outcome_results_query():
    sql_outcome_results = '''
    DROP TABLE IF EXISTS covid_19.outcome_results;
    
    CREATE TABLE covid_19.outcome_results AS
    SELECT
        overall_outcome,
        SUM(new_results_reported) as total_results_reported
    FROM covid_19.covid_test_results
    GROUP BY overall_outcome
    ORDER BY SUM(new_results_reported) desc;
    '''
    return sql_outcome_results

def state_results_query():
    sql_state_results = '''
    DROP TABLE IF EXISTS covid_19.state_results;
    
    CREATE TABLE covid_19.state_results AS
    SELECT
        state_name,
        SUM(CASE WHEN overall_outcome = 'Positive' THEN new_results_reported ELSE 0 END) AS positive_tests,
        SUM(CASE WHEN overall_outcome = 'Negative' THEN new_results_reported ELSE 0 END) AS negative_tests,
        SUM(CASE WHEN overall_outcome = 'Inconclusive' THEN new_results_reported ELSE 0 END) AS inconclusive_tests,
        ROUND(SUM(CASE WHEN overall_outcome = 'Positive' THEN new_results_reported ELSE 0 END)::numeric / SUM(new_results_reported)::numeric * 100, 2) AS positivity_rate
    FROM covid_19.covid_test_results
    GROUP BY state_name
    ORDER BY state_name;
    '''
    return sql_state_results

def smoothed_results_query():
    sql_smoothed_results = '''
    DROP TABLE IF EXISTS covid_19.smoothed_results;
    
    CREATE TABLE covid_19.smoothed_results AS
    SELECT
        date,
        ROUND(AVG(SUM(CASE WHEN overall_outcome = 'Positive' THEN new_results_reported ELSE 0 END)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 2) AS smoothed_positive_tests,
        ROUND(AVG(SUM(CASE WHEN overall_outcome = 'Negative' THEN new_results_reported ELSE 0 END)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 2) AS smoothed_negative_tests,
        ROUND(AVG(SUM(CASE WHEN overall_outcome = 'Inconclusive' THEN new_results_reported ELSE 0 END)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 2) AS smoothed_inconclusive_tests
    FROM covid_19.covid_test_results
    GROUP BY date
    ORDER BY date;
    '''
    return sql_smoothed_results

def total_results_query():
    sql_total_results = '''
    DROP TABLE IF EXISTS covid_19.total_results;
    
    CREATE TABLE covid_19.total_results AS
    SELECT
        t1.state,
        total_results_reported
    FROM covid_19.covid_test_results t1
    JOIN (
    		SELECT
    		state,
    		max(date) as latest_date
    		FROM covid_19.covid_test_results
    		GROUP BY state
    	) t2 on t1.state = t2.state and t1.date = t2.latest_date
    ORDER BY t1.state;
    '''
    return sql_total_results
