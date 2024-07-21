import requests


def get_id_employees(company_names: list[str], url: str) -> list[str]:
    """Выполняет API запрос на hh.ru. В параметре text поочереди подаются названия компаний.
    Затем программа выбирает все ID и возвращает их в виде списка."""
    params = dict(text='')
    headers = {'User-Agent': 'HH-User-Agent'}
    res = []
    for company_name in company_names:
        params['text'] = company_name
        response = requests.get(url, params=params, headers=headers).json()['items']
        for resp in response:
            res.append((resp['id']))
    # print(res)
    return res


def get_vacancies(id_list: list[str], url: str) -> list[dict]:
    """Принимает на вход список id компаний на hh.ru. Получает ответ по API по url ссылке,
    в каждый элемент списка возвращает подсписок вакансий компаний по каждой отдельной компании"""
    headers = {'User-Agent': 'HH-User-Agent'}
    result = []
    for id_ in id_list:
        params = dict(employer_id=id_, per_page=100)
        response = requests.get(url, params=params, headers=headers).json()
        result.append(response['items'])
    # print(result)
    return result
