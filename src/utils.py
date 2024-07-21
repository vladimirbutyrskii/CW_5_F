import requests


def get_id_employees(company_names: list[str], url: str) -> list[str]:
    """Выполняет запрос на hh.ru.
    В параметре text поочередно подаются названия компаний.
    По названиям выбираются все ID и возвращаются в виде списка."""
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
    """На вход подается список id компаний, полученных на hh.ru.
    Возвращается список вакансий компаний по каждой отдельно"""
    headers = {'User-Agent': 'HH-User-Agent'}
    result = []
    for id_ in id_list:
        params = dict(employer_id=id_, per_page=100)
        response = requests.get(url, params=params, headers=headers).json()
        result.append(response['items'])
    # print(result)
    return result
