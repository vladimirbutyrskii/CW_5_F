
from src.dbmanager import DBManager
from src.database_create import create_db


def main(db_instance: DBManager) -> None:
    """
    Управление работой всех функций проекта
    :param db_instance:
    :return:
    """

    print(f"\nВыбрать один из пунктов меню:")
    user_answer = 0

    while True:
        try:
            user_answer = int(input("""
1. Вывод на экран списка всех компаний и количество вакансий в каждой из них.
2. Вывод на экран списка всех вакансий с указанием названий компании, вакансии, уровня зарплаты (н/у - не указана), ссылки на вакансию.
3. Вывод на экран средней зарплаты по вакансиям (только для российских рублей).
4. Вывод на экран списка вакансий, зарплата которых выше среднего уровня заработной платы по вакансиям.
5. Вывод на экран списка названий вакансий, где присутствуют введенные ключевые слова (одно или несколько).
6. Обновление БД вакансий с сайта HH.RU
7. Выход.\n
            """))

        except ValueError:
            print(f"Введите номер одного из пунктов меню.")

        if user_answer == 1:
            db_conn.get_companies_and_vacancies_count()

        elif user_answer == 2:
            db_conn.get_all_vacancies()

        elif user_answer == 3:
            db_conn.get_avg_salary()

        elif user_answer == 4:
            db_conn.get_vacancies_with_higher_salary()

        elif user_answer == 5:
            db_conn.get_vacancies_with_keyword()

        elif user_answer == 6:
            create_db()

        elif user_answer == 7:
            print(f"Окончание работы...")
            break
        else:
            print(f"Неверный выбор.Еще раз...")
            continue


if __name__ == '__main__':
    db_conn = DBManager()
    with db_conn:
        main(db_conn)
