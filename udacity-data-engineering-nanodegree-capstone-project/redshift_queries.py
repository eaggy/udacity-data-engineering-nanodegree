# -*- coding: utf-8 -*-

"""The file contains sql queries required by the project."""

# get tables
get_tables = """SELECT t.table_name
                FROM information_schema.tables t
                WHERE t.table_schema = 'public'
                AND t.table_type = 'BASE TABLE'
                ORDER BY t.table_name;
             """
# drop table
drop_table = "DROP TABLE IF EXISTS {table_name};"


# create staging tables
create_staging_table_transactions = """CREATE TABLE IF NOT EXISTS
                               staging_transactions
                               (report_date DATE,
                                loan_id VARCHAR,
                                start_date TIMESTAMP,
                                end_date TIMESTAMP,
                                result VARCHAR,
                                discount_rate REAL,
                                debt_days_at_start REAL,
                                debt_days_at_end REAL,
                                principal_at_start REAL,
                                principal_at_end REAL,
                                CONSTRAINT staging_transactions_pkey
                                 PRIMARY KEY (loan_id, start_date));
                            """

create_staging_table_loans = """CREATE TABLE IF NOT EXISTS staging_loans
                               (loan_id VARCHAR,
                                user_name VARCHAR,
                                amount REAL,
                                interest REAL,
                                loan_duration SMALLINT,
                                monthly_payment REAL,
                                use_of_loan SMALLINT,
                                verification_type SMALLINT,
                                loan_date TIMESTAMP,
                                default_date TIMESTAMP,
                                debt_occured_on TIMESTAMP,
                                last_payment_on TIMESTAMP,
                                CONSTRAINT staging_loans_pkey
                                 PRIMARY KEY (loan_id));
                            """

create_staging_table_borrowers = """CREATE TABLE IF NOT EXISTS
                               staging_borrowers
                               (user_name VARCHAR,
                                loan_application_started_date TIMESTAMP,
                                age SMALLINT,
                                gender SMALLINT,
                                country VARCHAR,
                                education SMALLINT,
                                marital_status SMALLINT,
                                nr_of_dependants SMALLINT,
                                employment_status SMALLINT,
                                occupation_area SMALLINT,
                                home_ownership_type SMALLINT,
                                income_total REAL,
                                liabilities_total REAL,
                                debt_to_income REAL,
                                free_cash REAL,
                                CONSTRAINT staging_borrowers_pkey
                                 PRIMARY KEY (user_name,
                                              loan_application_started_date));
                            """

create_staging_table_market = """CREATE TABLE IF NOT EXISTS staging_market
                               (date_id DATE,
                                omx REAL,
                                d_omx REAL,
                                estoxx REAL,
                                d_estoxx REAL,
                                CONSTRAINT staging_market_pkey
                                 PRIMARY KEY (date_id));
                            """

create_staging_table_statistics = """CREATE TABLE IF NOT EXISTS
                                     staging_statistics
                                     (date_id DATE,
                                      house_price_index REAL,
                                      harmonized_unemployment_rate REAL,
                                      consumers REAL,
                                      CONSTRAINT staging_statistics_pkey
                                       PRIMARY KEY (date_id));
                                  """

# create tables
create_table_transactions = """CREATE TABLE IF NOT EXISTS transactions
                               (loan_id VARCHAR(36) NOT NULL,
                                principal_at_end REAL,
                                discount_rate REAL,
                                start_date TIMESTAMP NOT NULL,
                                end_date TIMESTAMP NOT NULL,
                                offer_time REAL,
                                CONSTRAINT transactions_pkey
                                 PRIMARY KEY (loan_id, start_date));
                            """

create_table_loans = """CREATE TABLE IF NOT EXISTS loans
                               (loan_id VARCHAR(36),
                                user_name VARCHAR(50),
                                amount REAL,
                                interest REAL,
                                loan_duration SMALLINT,
                                monthly_payment REAL,
                                use_of_loan SMALLINT,
                                verification_type SMALLINT,
                                loan_date TIMESTAMP,
                                default_date TIMESTAMP,
                                debt_occured_on TIMESTAMP,
                                last_payment_on TIMESTAMP,
                                CONSTRAINT loans_pkey PRIMARY KEY (loan_id));
                            """

create_table_borrowers = """CREATE TABLE IF NOT EXISTS borrowers
                               (user_name VARCHAR(32),
                                loan_application_started_date TIMESTAMP,
                                age SMALLINT,
                                gender SMALLINT,
                                country VARCHAR(2),
                                education SMALLINT,
                                marital_status SMALLINT,
                                nr_of_dependants SMALLINT,
                                employment_status SMALLINT,
                                occupation_area SMALLINT,
                                home_ownership_type SMALLINT,
                                income_total REAL,
                                liabilities_total REAL,
                                debt_to_income REAL,
                                free_cash REAL,
                                CONSTRAINT borrowers_pkey
                                 PRIMARY KEY (user_name,
                                              loan_application_started_date));
                            """

create_table_market = """CREATE TABLE IF NOT EXISTS market
                               (date_id DATE,
                                omx REAL,
                                d_omx REAL,
                                estoxx REAL,
                                d_estoxx REAL,
                                CONSTRAINT market_pkey PRIMARY KEY (date_id));
                            """

create_table_statistics = """CREATE TABLE IF NOT EXISTS statistics
                               (date_id DATE NOT NULL,
                                house_price_index REAL,
                                harmonized_unemployment_rate REAL,
                                consumers REAL,
                                CONSTRAINT statistics_pkey
                                PRIMARY KEY (date_id));
                            """

copy_table = """COPY {table_name}
                FROM '{location}'
                IAM_ROLE '{iam_role}'
                REGION '{region}'
                DELIMITER ','
                {file_format}
                IGNOREHEADER {header}
                ;"""


delete_table_transactions = """DELETE FROM transactions
                             USING staging_transactions
                             WHERE transactions.loan_id =
                              TRIM (LEADING '{' FROM TRIM (TRAILING '}'
                                    FROM staging_transactions.loan_id ) )
                               AND transactions.start_date =
                                    staging_transactions.start_date;"""

insert_table_transactions = """INSERT INTO transactions
                             (loan_id,
                              principal_at_end,
                              discount_rate,
                              start_date,
                              end_date,
                              offer_time)
                             SELECT DISTINCT
                              TRIM (LEADING '{' FROM TRIM (TRAILING '}'
                                    FROM loan_id ) ),
                              principal_at_end,
                              discount_rate,
                              start_date,
                              end_date,
                              EXTRACT(EPOCH FROM (end_date - start_date) )
                               AS offer_time
                             FROM staging_transactions
                             ;"""

delete_table_loans = """DELETE FROM loans
                             USING staging_loans
                             WHERE loans.loan_id = staging_loans.loan_id;"""

insert_table_loans = """INSERT INTO loans
                             SELECT DISTINCT *
                             FROM staging_loans;"""

delete_table_borrowers = """DELETE FROM borrowers
                             USING staging_borrowers
                             WHERE borrowers.user_name =
                                   staging_borrowers.user_name
                               AND borrowers.loan_application_started_date =
                            staging_borrowers.loan_application_started_date;"""

insert_table_borrowers = """INSERT INTO borrowers
                             SELECT DISTINCT *
                             FROM staging_borrowers;"""

delete_table_market = """DELETE FROM market
                             USING staging_market
                             WHERE market.date_id = staging_market.date_id;"""

insert_table_market = """INSERT INTO market
                             SELECT DISTINCT *
                             FROM staging_market;"""

delete_table_statistics = """DELETE FROM statistics
                             USING staging_statistics
                             WHERE statistics.date_id =
                              staging_statistics.date_id;"""

insert_table_statistics = """INSERT INTO statistics
                             SELECT DISTINCT *
                             FROM staging_statistics;"""


# query lists
create_staging_table_queries = [create_staging_table_transactions,
                                create_staging_table_loans,
                                create_staging_table_borrowers,
                                create_staging_table_market,
                                create_staging_table_statistics]

create_table_queries = [create_table_transactions,
                        create_table_loans,
                        create_table_borrowers,
                        create_table_market,
                        create_table_statistics]

upsert_table_queries = [(delete_table_transactions, insert_table_transactions),
                        (delete_table_loans, insert_table_loans),
                        (delete_table_borrowers, insert_table_borrowers),
                        (delete_table_market, insert_table_market),
                        (delete_table_statistics, insert_table_statistics)]
