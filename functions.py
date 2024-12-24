import requests

def get_patient_appointments(api_key, v_api_url, query_date='01.10.2024'):
    try:
        date_from = f"{query_date} 00:00"
        date_to = f"{query_date} 23:59"

        url = f"{v_api_url}/getAppointments"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        params = {
            "api_key": api_key,
            "date_from": date_from,
            "date_to": date_to
        }

        response = requests.post(url, headers=headers, data=params)

        if response.status_code == 200:
            data = response.json()
            if data['error'] == 0:
                return [
                    {
                        'patient_id': appointment['patient_id'],
                        'appointment_id': appointment['id'],
                        'sum_value': appointment['sum_value']# Предполагается, что id визита - это 'id'

                    }
                    for appointment in data['data']
                ]  # Возвращаем список словарей с уникальными данными пациентов и визитов
            else:
                raise Exception(f"Ошибка: {data['data'][0]['code']} - {data['data'][0]['desc']}")
        else:
            raise Exception(f"Ошибка HTTP: {response.status_code}")

    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print("Ошибка произошла в функции getAppointmentServices.")
def handle_api_response(response):
    if response.status_code != 200:
        raise Exception(f"Ошибка HTTP: {response.status_code}")

    data = response.json()
    if data['error'] != 0:
        error_code = data.get('data', {}).get('code', 'Неизвестный код ошибки')
        error_desc = data.get('data', {}).get('desc', 'Нет описания ошибки')
        raise Exception(f"Ошибка: {error_code} - {error_desc}")

    return data['data']

def getAppointmentServices(api_key, appointment_ids, v_api_url):
    try:
        params = {
            "appointment_id": ",".join(map(str, appointment_ids)),  # Получаем список идентификаторов визитов
            "show_deleted": "0",
            "api_key": api_key
        }
        url = f"{v_api_url}/v2/getAppointmentServices"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # Отправляем POST-запрос к API
        response = requests.post(url, headers=headers, data=params)
        handle_api_response(response)  # Обработка ответа на ошибки

        # Получаем данные о предоставленных услугах
        AppointmentServices = response.json().get("data", {})

        invoice_ids = set()  # Используем множество для уникальных invoice_id

        # Проходим по каждому визиту и извлекаем услуги
        for appointment_id, services in AppointmentServices.items():
            for service in services:
                invoice_id = service.get("invoice_id")
                if invoice_id:
                    invoice_ids.add(invoice_id)  # Добавляем уникальный invoice_id в множество

        return list(invoice_ids)  # Возвращаем список уникальных invoice_id
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print("Ошибка произошла в функции getAppointmentServices.")

def get_company_types(api_key, company_id, query_date, v_api_url):
    try:
        invoices = []
        params = {
            "company_id": company_id,
            "api_key": api_key,
            "date_from": query_date,
            "date_to": query_date,

            "show_deleted": "0"
        }

        url = f"{v_api_url}/getInvoices"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, headers=headers, data=params)
        handle_api_response(response)

        invoices.extend(response.json()['data'])


        return invoices
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print("Ошибка произошла в функции get_company_types.")

def get_invoices(api_key, invoice_ids, v_api_url):
    try:
        invoices = []
        for invoice_id in invoice_ids:
            params = {
                "invoice_id": invoice_id,
                "api_key": api_key,
                "show_deleted": "0"
            }

            url = f"{v_api_url}/v2/getInvoices"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }

            response = requests.post(url, headers=headers, data=params)
            handle_api_response(response)

            invoices.extend(response.json()['data'])

        return invoices
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print("Ошибка произошла в функции get_invoices.")