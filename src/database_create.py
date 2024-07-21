from src.utils import get_id_employees
from src.utils import get_vacancies
from src.settings import hh_company_names, employee_API, vacancies_API
import psycopg2


def create_db(vacancy=None):
    # Переменная со списком ID компаний, а так же списком вакансий в формате JSON.
    id_employees_list = get_id_employees(hh_company_names, employee_API)
    get_vacancy_list = get_vacancies(id_employees_list, vacancies_API)

    #  Вводим свои локальные параметры базы данных Postgres SQL.
    try:
        params = dict(host='localhost',
                      database='CW_5',
                      port=5432,
                      user='postgres',
                      password='12345', )
        with psycopg2.connect(**params) as connection:
            with connection.cursor() as cursor:
                #  Удаляем старые таблицы, если ранее существовали.
                cursor.execute(f"""
                DROP TABLE IF EXISTS vacancies CASCADE;
                DROP TABLE IF EXISTS employers CASCADE;
                """)

                # Создаем новые таблицы.
                cursor.execute(f"""
        
                    CREATE TABLE vacancies (
                    vacancy_id INT PRIMARY KEY, 
                    vacancy_name VARCHAR(100) NOT NULL, 
                    salary_from REAL, 
                    salary_to REAL, 
                    currency VARCHAR(5) NOT NULL,
                    published_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    url VARCHAR(200),
                    alternate_url VARCHAR(200),
                    employer_id INT
                    );
                    
                    CREATE TABLE employers (
                    employer_id INT PRIMARY KEY,
                    company_name VARCHAR(100) NOT NULL, 
                    url VARCHAR(100) NOT NULL,
                    alternate_url VARCHAR(100) NOT NULL,
                    logo_urls TEXT NOT NULL,
                    accredited_it_employer BOOL 
                     );
        
                    """)
                cursor.execute(f""" 
                                CREATE TEMP TABLE temp_employers(
                                employer_id INT NOT NULL,
                                company_name VARCHAR(100) NOT NULL, 
                                url VARCHAR(100) NOT NULL,
                                alternate_url VARCHAR(100) NOT NULL,
                                logo_urls TEXT NOT NULL,
                                accredited_it_employer BOOL );
                                """)
                cursor.close()
                connection.commit()
            with connection.cursor() as cursor:
                for company_vacancies in get_vacancy_list:  # Список компаний
                    for vacancy in company_vacancies:  # Список вакансий по каждой отдельной компании
                        add_vacancy = (
                            vacancy['id'], vacancy['name'], vacancy['salary']['from'] if vacancy.get('salary') else 0,
                            vacancy['salary']['to'] if vacancy.get('salary') else 0,
                            vacancy['salary']['currency'] if vacancy.get('salary') else '', vacancy['published_at'],
                            vacancy['created_at'], vacancy['url'], vacancy['alternate_url'],
                            vacancy['employer']['id'])

                        add_employer = (
                            vacancy['employer'].get('id'),
                            # vacancy['id'],
                            vacancy['employer'].get('name'),
                            vacancy['employer'].get('url'),
                            vacancy['employer'].get('alternate_url'),
                            str(vacancy['employer'].get('logo_urls')),
                            # vacancy['employer'].get('vacancies_url'),
                            vacancy['employer'].get('accredited_it_employer')
                        )
                        #  добавляем данные в таблицы

                        cursor.execute(f"""
                                                INSERT INTO vacancies(vacancy_id, vacancy_name, salary_from, 
                                                salary_to, currency, published_at, created_at, url, alternate_url, 
                                                employer_id) 
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning *;""",
                                       add_vacancy)

                    cursor.execute(f"""
                                            INSERT INTO temp_employers
                                            (employer_id, -- vacancy_id, 
                                            company_name, url, 
                                            alternate_url, logo_urls, -- vacancies_url, 
                                            accredited_it_employer) VALUES 
                                            (%s, %s, %s, %s, %s, %s) returning *;
                                """, add_employer)

                cursor.execute(f"""
                        INSERT INTO employers
                        SELECT DISTINCT * FROM temp_employers """)

                cursor.execute(f"""DROP TABLE  temp_employers""")

                cursor.execute(f"""
                ALTER TABLE vacancies ADD CONSTRAINT fk_vacancies_employers 
                FOREIGN KEY(employer_id) REFERENCES employers(employer_id);
                """)

                connection.commit()
                cursor.close()
        print("База данных создана, данные в БД загружены.")
        connection.close()

    except psycopg2.OperationalError:
        print("Ошибка соединения с базой данных. Проверьте настройки.")
