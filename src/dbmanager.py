import psycopg2
from abc import ABC, abstractmethod


class DataBase(ABC):
    """Абстрактный класс для DBManager"""

    @abstractmethod
    def get_companies_and_vacancies_count(self):
        pass

    @abstractmethod
    def get_all_vacancies(self):
        pass

    @abstractmethod
    def get_avg_salary(self):
        pass

    @abstractmethod
    def get_vacancies_with_higher_salary(self):
        pass

    @abstractmethod
    def get_vacancies_with_keyword(self):
        pass


class DBManager(DataBase):
    """Класс для работы с существующей базой данных на localhost"""

    def __init__(self, host='localhost', database='CW_5', port=5432, username='postgres', password='12345') -> None:
        """Именные параметры уже внутри функции,
        однако их можно изменить на свои при создании экземпляра класса."""
        self.host = host
        self.database = database
        self.port = port
        self.user = username
        self.password = password
        self.params = dict(host=self.host, database=self.database, port=self.port, user=self.user,
                           password=self.password)

    def __enter__(self):
        """Создает соединение с БД"""
        try:
            self.conn = psycopg2.connect(**self.params)
            return self.conn

        except psycopg2.OperationalError:
            print(f"Ошибка соединения с базой данных. Проверьте введенные настройки.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Выполняет коммиты и закрытие Базы данных"""
        self.conn.commit()
        self.conn.close()


    def get_companies_and_vacancies_count(self):
        """Печатает и возвращает список всех компаний и количество вакансий у каждой компании."""

        cur = self.conn.cursor()
        query = ("SELECT e.company_name, COUNT(v.vacancy_id) as quantity_vacancies "
                 "FROM vacancies v INNER JOIN employers e USING(employer_id) GROUP BY e.company_name;")
        cur.execute(query)
        result = cur.fetchall()
        for one in result:
            print(
                f"Компания: {one[0]}, Количество вакансий: {one[1]}")
        cur.close()
        return result

    def get_all_vacancies(self):
        """
        получает список всех вакансий с указанием
        названия компании, названия вакансии и зарплаты и ссылки на вакансию
        :return:
        """

        cur = self.conn.cursor()
        query = ("SELECT e.company_name, v.vacancy_name, v.salary_from, v.salary_to, "
                 "v.alternate_url as url FROM vacancies v INNER JOIN employers e USING(employer_id) ")
        cur.execute(query)
        result = cur.fetchall()
        for one in result:
            print(
                f"{one[0]}, Вакансия: {one[1]}, "
                f"Зарплата: от {one[2] if one[2] else 'н/у'} - {one[3] if one[3] else 'н/у'}), "
                f"Ссылка: {one[4]}")
        cur.close()
        return result

    def get_avg_salary(self):
        """
         получает среднюю зарплату по вакансиям
        :return:
        """
        cur = self.conn.cursor()
        query = (
            "SELECT AVG(average) FROM(SELECT CASE WHEN salary_from <> 0 THEN salary_from ELSE salary_to END AS average "
            "FROM(SELECT salary_from, salary_to FROM vacancies WHERE (salary_from <> 0 OR salary_to <> 0) "
            "AND currency='RUR')) ")
        cur.execute(query)
        result = cur.fetchone()
        print(f"Средняя зарплата по вакансиям - {result[0]:,.2f} RUR")
        cur.close()

        return result[0]

    def get_vacancies_with_higher_salary(self):
        """
         получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        :param self:
        :return:
        """
        avg_salary = self.get_avg_salary()
        cur = self.conn.cursor()
        query = (f"SELECT vacancy_name, company_name, salary_from, salary_to FROM vacancies v INNER JOIN employers e "
                 f"USING(employer_id) WHERE "
                 f"(salary_from <> 0 OR salary_to <> 0) AND "
                 f"(salary_from > {avg_salary} OR salary_to > {avg_salary}) AND currency='RUR' ")
        cur.execute(query)
        result = cur.fetchall()
        for one in result:
            print(f"Вакансия: {one[0]}, Компания-работодатель:  {one[1]}, "
                  f"Уровень зарплаты: {one[2] if one[2] else 'н/у'} - {one[3] if one[3] else 'н/у'}")
        cur.close()

        return result

    def get_vacancies_with_keyword(self):
        """
         получает список всех вакансий, в названии которых
         содержатся переданные в метод слова, например python
        :return:
        """
        print(f"Введите ключевые слова через пробел:")
        key_word = input()
        while '  ' in key_word:
            key_word = key_word.replace('  ', ' ')
        key_word = key_word.split()
        like_str = ''
        for word in key_word:
            like_str += f"LOWER(vacancy_name) LIKE LOWER('%{word}%') OR "

        cur = self.conn.cursor()
        query = f"SELECT vacancy_name FROM vacancies WHERE {like_str[:-4]}"
        # print(query)
        cur.execute(query)
        result = cur.fetchall()
        for one in result:
            print(f"Вакансия: {one[0]}")
        cur.close()

        return result
